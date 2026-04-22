import os
import json
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | RFP-ENGINE | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "rfp_intel.json")

# ══════════════════════════════════════════════════════════════════════
# VERIFIED PRODUCTION ENDPOINTS — State of Alaska Procurement
# ══════════════════════════════════════════════════════════════════════

# DOT&PF Construction Bidding — live HTML tables with active/awarded bids
DOT_BIDS_URL = "https://dot.alaska.gov/procurement/awp/awp-bids.cfm"
DOT_BID_CALENDAR_URL = "https://dot.alaska.gov/procurement/awp/bids.html"
DOT_CONTRACT_AWARDS_URL = "https://dot.alaska.gov/procurement/awp/cas.html"
DOT_ADVERTISING_SCHEDULE_URL = "https://dot.alaska.gov/procurement/awp/tas.html"
DOT_BID_RESULTS_URL = "https://www.bidx.com/ak/lettings/"

# State Online Public Notices (RFPs, ITBs, public comment)
# Note: aws.state.ak.us blocks simple requests (WAF). Requires headless browser in Phase 2.
STATE_PUBLIC_NOTICES_URL = "https://aws.state.ak.us/OnlinePublicNotices/Default.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def scrape_dot_bid_calendar():
    """
    Scrapes the DOT&PF Bid Calendar for upcoming construction bids.
    This is a plain HTML table — no JS rendering required.
    """
    logger.info(f"Targeting DOT&PF Bid Calendar: {DOT_BID_CALENDAR_URL}")
    bids = []
    try:
        res = requests.get(DOT_BID_CALENDAR_URL, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        # DOT bid calendars are typically rendered as HTML tables
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header row
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:
                    bid = {
                        "columns": [cell.get_text(strip=True) for cell in cells],
                        "links": [a.get('href') for a in row.find_all('a') if a.get('href')]
                    }
                    bids.append(bid)
        
        logger.info(f"Extracted {len(bids)} bid entries from DOT calendar.")
    except Exception as e:
        logger.error(f"DOT Bid Calendar extraction failed: {e}")

    return bids


def scrape_dot_contract_awards():
    """
    Scrapes the DOT&PF Contract Award Status page for recently awarded contracts.
    """
    logger.info(f"Targeting DOT&PF Contract Awards: {DOT_CONTRACT_AWARDS_URL}")
    awards = []
    try:
        res = requests.get(DOT_CONTRACT_AWARDS_URL, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 3:
                    award = {
                        "columns": [cell.get_text(strip=True) for cell in cells],
                        "links": [a.get('href') for a in row.find_all('a') if a.get('href')]
                    }
                    awards.append(award)

        logger.info(f"Extracted {len(awards)} contract award entries.")
    except Exception as e:
        logger.error(f"DOT Contract Awards extraction failed: {e}")

    return awards


def scrape_dot_advertising_schedule():
    """
    Scrapes the Tentative Advertising Schedule for future planned procurements.
    """
    logger.info(f"Targeting DOT&PF Advertising Schedule: {DOT_ADVERTISING_SCHEDULE_URL}")
    schedule = []
    try:
        res = requests.get(DOT_ADVERTISING_SCHEDULE_URL, headers=HEADERS, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    entry = {
                        "columns": [cell.get_text(strip=True) for cell in cells],
                        "links": [a.get('href') for a in row.find_all('a') if a.get('href')]
                    }
                    schedule.append(entry)

        logger.info(f"Extracted {len(schedule)} tentative advertising entries.")
    except Exception as e:
        logger.error(f"DOT Advertising Schedule extraction failed: {e}")

    return schedule


def execute_rfp_scrape():
    ensure_output_dir()
    logger.info("═══ STATE PROCUREMENT EXTRACTION PROTOCOL INITIATED ═══")

    bids = scrape_dot_bid_calendar()
    awards = scrape_dot_contract_awards()
    schedule = scrape_dot_advertising_schedule()

    intel_payload = {
        "metadata": {
            "last_synced": datetime.utcnow().isoformat(),
            "target": "Alaska DOT&PF Procurement & Contracting",
            "source_urls": {
                "bid_calendar": DOT_BID_CALENDAR_URL,
                "contract_awards": DOT_CONTRACT_AWARDS_URL,
                "advertising_schedule": DOT_ADVERTISING_SCHEDULE_URL,
                "bid_results_bidx": DOT_BID_RESULTS_URL,
                "state_public_notices": STATE_PUBLIC_NOTICES_URL + " (WAF-blocked, Phase 2)",
            },
            "status": "Operational",
            "notes": "DOT&PF pages are plain HTML tables. aws.state.ak.us requires Playwright (Phase 2)."
        },
        "upcoming_bids": bids,
        "awarded_contracts": awards,
        "advertising_schedule": schedule,
        "summary": {
            "total_upcoming_bids": len(bids),
            "total_awarded_contracts": len(awards),
            "total_scheduled_projects": len(schedule),
        }
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(intel_payload, f, indent=2)

    logger.info(f"═══ RFP PAYLOAD WRITTEN: {OUTPUT_FILE} ═══")
    logger.info(f"    Bids: {len(bids)} | Awards: {len(awards)} | Schedule: {len(schedule)}")


if __name__ == "__main__":
    execute_rfp_scrape()
