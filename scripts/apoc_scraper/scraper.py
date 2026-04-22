"""APOC Playwright Scraper — Ported from NickCrews/apoc-data (MIT License)

Scrapes the Alaska Public Offices Commission ASP.NET portal using headless
Chromium via Playwright. The state site stores all session state server-side,
so you MUST use a real browser session to navigate the forms and trigger the
CSV export. Simple HTTP requests get rejected by the WAF.

Original: https://github.com/NickCrews/apoc-data
License: MIT
"""

from __future__ import annotations

import asyncio
import csv
import logging
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterable, ClassVar, Iterable

from playwright.async_api import BrowserContext, async_playwright, expect

from .filters import ScrapeFilters, YearEnum

_logger = logging.getLogger(__name__)


@asynccontextmanager
async def make_browser(headless: bool = True) -> AsyncIterable[BrowserContext]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        yield await browser.new_context(**p.devices["Desktop Chrome"])


async def _run_scrape_flow(page, url: str, filters: ScrapeFilters):
    await page.goto(url)
    await page.wait_for_timeout(100)
    await page.select_option("select:below(:text('Status:'))", filters.status.value)
    await page.select_option("select:below(:text('Report Year:'))", filters.report_year.value)
    await page.wait_for_timeout(100)

    await page.click("//input[@value='Search']")
    await page.wait_for_timeout(100)
    await expect(page.get_by_text("Press 'Search' to Load Results.")).to_be_hidden(timeout=30_000)

    await page.click("//input[@value='Export']")
    async with page.expect_download(timeout=120_000) as download_info:
        await page.click("a:text('.CSV'):below(:text('Export All Pages:'))")

    await page.click("//input[@value='Close']")
    return await download_info.value


def _check_valid_csv(path: Path) -> None:
    with open(path) as f:
        for i, line in enumerate(f):
            if "<html>" in line:
                raise ValueError(f"Bad CSV content in line {i} of {path}: {line}")


class _ScraperBase:
    _HOME_URL: ClassVar[str]
    _HEADER_ROW: ClassVar[str]
    name: ClassVar[str]

    def __init__(self, *, destination: str | Path, filters: ScrapeFilters | None = None):
        self.destination = Path(destination)
        self.filters = filters or ScrapeFilters()

    async def __call__(self, browser_context: BrowserContext) -> None:
        page = browser_context.pages[0] if browser_context.pages else await browser_context.new_page()
        _logger.info(f"Downloading {self.name} to {self.destination} using {self.filters}")
        download = await _run_scrape_flow(page, self._HOME_URL, self.filters)
        _logger.info("Download started")
        path = await download.path()
        if path.stat().st_size == 0:
            _logger.info(f"No results. Writing header to {self.destination}")
            self.destination.parent.mkdir(parents=True, exist_ok=True)
            content = self._HEADER_ROW
            if not content.endswith("\n"):
                content += "\n"
            self.destination.write_text(content)
        else:
            _check_valid_csv(path)
            await download.save_as(self.destination)
            _logger.info(f"Downloaded {self.destination}")


class _AnyYearMicroBatchScraper(_ScraperBase):
    """Downloads report_year=Any in micro-batches to avoid APOC server crashes."""

    def __init__(self, *, destination: str | Path, filters: ScrapeFilters | None = None, tempdir: Path | None = None):
        super().__init__(filters=filters, destination=destination)
        self.tempdir = tempdir

    async def __call__(self, browser_context: BrowserContext) -> None:
        if self.filters.report_year != YearEnum.any:
            return await super().__call__(browser_context)

        async def f(tmpdir):
            tmpdir = Path(tmpdir)
            sub_scrapers = [
                self.__class__(
                    filters=ScrapeFilters(report_year=year, status=self.filters.status),
                    destination=tmpdir / f"{self.name}_{year.value}.csv",
                )
                for year in YearEnum if year != YearEnum.any
            ]
            for s in sub_scrapers:
                await s(browser_context)
            self._merge_csvs([s.destination for s in sub_scrapers], self.destination)

        if self.tempdir is None:
            with tempfile.TemporaryDirectory() as tmpdir:
                await f(tmpdir)
        else:
            await f(self.tempdir)

    def _merge_csvs(self, srcs: Iterable[Path], destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with open(destination, "w") as f:
            writer = csv.writer(f)
            writer.writerow([col.strip('"') for col in self._HEADER_ROW.split(",")])
            i = 1
            for src in srcs:
                with open(src, "r") as srcf:
                    reader = csv.reader(srcf)
                    next(reader)  # skip header
                    for row in reader:
                        _index, *rest = row
                        writer.writerow([i, *rest])
                        i += 1


# ═══════════════════════════════════════════════════════════
# INDIVIDUAL SCRAPER CLASSES (one per APOC form type)
# ═══════════════════════════════════════════════════════════

class CandidateRegistrationScraper(_ScraperBase):
    _HOME_URL = "https://aws.state.ak.us/ApocReports/Registration/CandidateRegistration/CRForms.aspx"
    _HEADER_ROW = '"Result","Report Year","Display Name","Last Name","First Name","Committee","Purpose","Previously Registered","Address","City","State","Zip","Country","Phone","Fax","Email","Election","Election Type","Municipality","Office","Treasurer Name","Treasurer Email","Treasurer Phone","Chair Name","Chair Email","Chair Phone","Bank Name","Bank Address","Bank City","Bank State","Bank Zip","Bank Country","Submitted","Status","Amending"'
    name = "candidate_registration"


class LetterOfIntentScraper(_ScraperBase):
    _HOME_URL = "https://aws.state.ak.us/ApocReports/Registration/LetterOfIntent/LOIForms.aspx"
    _HEADER_ROW = '"Result","Report Year","Display Name","Last Name","First Name","Previously Registered","Phone","Fax","Email","Election","Election Type","Municipality","Office","Submitted","Status","Amending"'
    name = "letter_of_intent"


class GroupRegistrationScraper(_ScraperBase):
    _HOME_URL = "https://aws.state.ak.us/ApocReports/Registration/GroupRegistration/GRForms.aspx"
    _HEADER_ROW = '"Result","Report Year","Abbreviation","Name","Address","City","State","Zip","Country","Plan","Type","Subtype","Treasurer Name","Treasurer Email","Chair Name","Chair Email","Additional Emails","Submitted","Status","Amending"'
    name = "group_registration"


class EntityRegistrationScraper(_ScraperBase):
    _HOME_URL = "https://aws.state.ak.us/ApocReports/Registration/EntityRegistration/ERForms.aspx"
    _HEADER_ROW = '"Result","Report Year","Abbreviation","Name","Purpose","Supporting State Initiative","Phone","Email","Address","City","State","Zip","Country","Contact Name","Contact Email","Submitted","Status","Amending"'
    name = "entity_registration"


class DebtScraper(_ScraperBase):
    _HOME_URL = "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDDebt.aspx"
    _HEADER_ROW = '"Result","Date","Balance Remaining","Original Amount","Name","Address","City","State","Zip","Country","Description/Purpose","--------","Filer Type","Name","Report Year","Submitted"'
    name = "debt"


class ExpenditureScraper(_ScraperBase):
    _HOME_URL = "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDExpenditures.aspx"
    _HEADER_ROW = '"Result","Date","Transaction Type","Payment Type","Payment Detail","Amount","Last/Business Name","First Name","Address","City","State","Zip","Country","Occupation","Employer","Purpose of Expenditure","--------","Report Type","Election Name","Election Type","Municipality","Office","Filer Type","Name","Report Year","Submitted"'
    name = "expenditures"


class IncomeScraper(_AnyYearMicroBatchScraper):
    _HOME_URL = "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDIncome.aspx"
    _HEADER_ROW = '"Result","Date","Transaction Type","Payment Type","Payment Detail","Amount","Last/Business Name","First Name","Address","City","State","Zip","Country","Occupation","Employer","Purpose of Expenditure","--------","Report Type","Election Name","Election Type","Municipality","Office","Filer Type","Name","Report Year","Submitted"'
    name = "income"


class CampaignFormScraper(_AnyYearMicroBatchScraper):
    _HOME_URL = "https://aws.state.ak.us/ApocReports/CampaignDisclosure/CDForms.aspx"
    _HEADER_ROW = '"Result","Report Year","Report Type","Begin Date","End Date","Filer Type","Name","Beginning Cash On Hand","Total Income","Previous Campaign Income","Campaign Income Total","Total Expenditures","Previous Campaign Expense","Campaign Expense Total","Closing Cash On Hand","Total Debt","Surplus/Deficit","Submitted","Status","Amending"'
    name = "campaign_form"


# ═══════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════

ALL_SCRAPER_CLASSES = [
    CampaignFormScraper,
    IncomeScraper,
    CandidateRegistrationScraper,
    LetterOfIntentScraper,
    GroupRegistrationScraper,
    EntityRegistrationScraper,
    DebtScraper,
    ExpenditureScraper,
]


def scrape_all(directory: str | Path = "scraped/", *, headless: bool = True) -> None:
    """Scrape all APOC CSV datasets using headless Chromium."""
    directory = Path(directory)
    scrapers = [cls(destination=directory / f"{cls.name}.csv") for cls in ALL_SCRAPER_CLASSES]

    async def run():
        async with make_browser(headless=headless) as browser_context:
            for s in scrapers:
                await s(browser_context)

    asyncio.run(run())
