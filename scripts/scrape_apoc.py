"""
APOC Scraper Runner — AlaskaIntel Pipeline

Runs the full Playwright-based APOC scraper on our own GitHub Actions
infrastructure. This eliminates our dependency on NickCrews/apoc-data.

The scraper downloads 8 CSV datasets from the APOC portal:
  - candidate_registration.csv
  - letter_of_intent.csv
  - group_registration.csv
  - entity_registration.csv
  - campaign_form.csv
  - income.csv (contributions / dark money)
  - expenditures.csv
  - debt.csv

After scraping, we convert the CSVs to a single consolidated JSON
for consumption by the AlaskaIntel frontend.
"""

import os
import sys
import json
import csv
import logging
from datetime import datetime
from pathlib import Path

# Add the scripts directory to the path so we can import our ported module
sys.path.insert(0, os.path.dirname(__file__))

from apoc_scraper.scraper import scrape_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s | APOC-ENGINE | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

SCRAPE_DIR = os.path.join(os.path.dirname(__file__), "../scraped")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "apoc_intel.json")


def ensure_dirs():
    os.makedirs(SCRAPE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def csv_to_records(csv_path: str, max_rows: int = 500) -> list[dict]:
    """Read a CSV file and return a list of dicts (capped at max_rows)."""
    records = []
    try:
        with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                records.append(dict(row))
    except Exception as e:
        logger.warning(f"Could not read {csv_path}: {e}")
    return records


def execute_apoc_scrape():
    ensure_dirs()
    logger.info("═══ APOC PLAYWRIGHT EXTRACTION PROTOCOL INITIATED ═══")
    logger.info("Running headless Chromium against aws.state.ak.us...")

    # Run the actual Playwright scraper (headless Chromium)
    scrape_all(directory=SCRAPE_DIR, headless=True)

    logger.info("Playwright scrape complete. Converting CSVs to JSON...")

    # Convert the downloaded CSVs into a unified JSON payload
    datasets = {}
    summary = {}
    scrape_path = Path(SCRAPE_DIR)

    for csv_file in sorted(scrape_path.glob("*.csv")):
        dataset_name = csv_file.stem  # e.g. "income", "expenditures"
        records = csv_to_records(str(csv_file))
        datasets[dataset_name] = records
        summary[f"total_{dataset_name}"] = len(records)
        logger.info(f"  {dataset_name}: {len(records)} records")

    intel_payload = {
        "metadata": {
            "last_synced": datetime.utcnow().isoformat(),
            "target": "Alaska Public Offices Commission (APOC)",
            "method": "Direct Playwright scrape (self-hosted)",
            "source_portal": "https://aws.state.ak.us/ApocReports/Campaign/",
            "status": "Operational",
            "notes": "Scraped directly using headless Chromium on AlaskaIntel GitHub Actions. No third-party dependency."
        },
        **datasets,
        "summary": summary
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(intel_payload, f, indent=2)

    logger.info(f"═══ APOC PAYLOAD WRITTEN: {OUTPUT_FILE} ═══")


if __name__ == "__main__":
    execute_apoc_scrape()
