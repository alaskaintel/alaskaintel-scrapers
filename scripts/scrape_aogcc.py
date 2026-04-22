import os
import json
import logging
import requests
import zipfile
import csv
from io import BytesIO, StringIO
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | AOGCC-ENGINE | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "aogcc_intel.json")

# ══════════════════════════════════════════════════════════════════════
# VERIFIED PRODUCTION ENDPOINTS — State of Alaska AOGCC
# Source: https://www.commerce.alaska.gov/web/aogcc/Data
# All endpoints are public, unauthenticated, and updated daily.
# ══════════════════════════════════════════════════════════════════════

# Direct-download ZIP files from Azure CDN (official state hosting)
WELLS_ZIP_URL = "https://aogcccdn.azureedge.net/dataminerzip/wells.zip"
WELL_HISTORY_ZIP_URL = "https://aogcccdn.azureedge.net/dataminerzip/wellhistory.zip"
FACILITIES_ZIP_URL = "https://aogcccdn.azureedge.net/dataminerzip/facilities.zip"
FRACTURED_WELLS_ZIP = "https://aogcccdn.azureedge.net/dataminerzip/fracturedwells.zip"

# Full data extract (Microsoft Access DB — ~30MB, contains everything)
FULL_EXTRACT_URL = "https://aogcccdnstorage.blob.core.windows.net/webextract/AOGCC_DataExtract.zip"

# Drilling activity page (JS-rendered, requires headless browser for deep data)
DRILLING_PAGE_URL = "http://aogweb.state.ak.us/Drilling"

# Orders search (Conservation Orders, Area Injection Orders, etc.)
ORDERS_SEARCH_URL = "http://aogweb.state.ak.us/WebLinkSearch"

# Production data
PRODUCTION_URL = "http://aogweb.state.ak.us/DataMiner4/Forms/Production.aspx"

# Pool statistics
POOL_STATS_URL = "http://aogweb.state.ak.us/PoolStatistics"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def download_and_extract_csv(zip_url, max_rows=200):
    """
    Downloads a ZIP file from the AOGCC CDN, extracts the CSV inside,
    and returns the first N rows as a list of dicts.
    """
    logger.info(f"Downloading ZIP payload from: {zip_url}")
    try:
        res = requests.get(zip_url, headers=HEADERS, timeout=60)
        res.raise_for_status()

        with zipfile.ZipFile(BytesIO(res.content)) as zf:
            csv_files = [f for f in zf.namelist() if f.lower().endswith('.csv')]
            if not csv_files:
                # Try txt files (some AOGCC exports use .txt with CSV formatting)
                csv_files = [f for f in zf.namelist() if f.lower().endswith('.txt')]
            
            if not csv_files:
                logger.warning(f"No CSV/TXT files found in {zip_url}. Contents: {zf.namelist()}")
                return []

            target_file = csv_files[0]
            logger.info(f"Extracting: {target_file}")

            with zf.open(target_file) as f:
                # Read as text, handle encoding issues gracefully
                raw_text = f.read().decode('utf-8', errors='replace')
                reader = csv.DictReader(StringIO(raw_text))
                rows = []
                for i, row in enumerate(reader):
                    if i >= max_rows:
                        break
                    rows.append(dict(row))
                
                logger.info(f"Extracted {len(rows)} rows from {target_file}")
                return rows

    except Exception as e:
        logger.error(f"Failed to download/extract {zip_url}: {e}")
        return []


def execute_aogcc_scrape():
    ensure_output_dir()
    logger.info("═══ AOGCC EXTRACTION PROTOCOL INITIATED ═══")

    # ── Vector 1: Wells Database ──
    logger.info("─── VECTOR 1: WELLS DATABASE ───")
    wells = download_and_extract_csv(WELLS_ZIP_URL, max_rows=500)

    # ── Vector 2: Well History ──
    logger.info("─── VECTOR 2: WELL HISTORY ───")
    history = download_and_extract_csv(WELL_HISTORY_ZIP_URL, max_rows=300)

    # ── Vector 3: Facilities ──
    logger.info("─── VECTOR 3: FACILITIES ───")
    facilities = download_and_extract_csv(FACILITIES_ZIP_URL, max_rows=200)

    # ── Vector 4: Hydraulically Fractured Wells ──
    logger.info("─── VECTOR 4: FRACTURED WELLS ───")
    fractured = download_and_extract_csv(FRACTURED_WELLS_ZIP, max_rows=100)

    # ── Assemble Intelligence Payload ──
    intel_payload = {
        "metadata": {
            "last_synced": datetime.utcnow().isoformat(),
            "target": "Alaska Oil & Gas Conservation Commission (AOGCC)",
            "source_urls": {
                "wells": WELLS_ZIP_URL,
                "well_history": WELL_HISTORY_ZIP_URL,
                "facilities": FACILITIES_ZIP_URL,
                "fractured_wells": FRACTURED_WELLS_ZIP,
                "full_extract": FULL_EXTRACT_URL,
                "drilling_page": DRILLING_PAGE_URL,
                "orders_search": ORDERS_SEARCH_URL,
                "production": PRODUCTION_URL,
                "pool_stats": POOL_STATS_URL,
            },
            "status": "Operational",
            "notes": "Data extracted from official State of Alaska AOGCC Azure CDN. Updated daily by the state."
        },
        "wells": wells,
        "well_history": history,
        "facilities": facilities,
        "fractured_wells": fractured,
        "summary": {
            "total_wells_extracted": len(wells),
            "total_history_records": len(history),
            "total_facilities": len(facilities),
            "total_fractured_wells": len(fractured),
        }
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(intel_payload, f, indent=2)

    logger.info(f"═══ AOGCC PAYLOAD WRITTEN: {OUTPUT_FILE} ═══")
    logger.info(f"    Wells: {len(wells)} | History: {len(history)} | Facilities: {len(facilities)} | Fractured: {len(fractured)}")


if __name__ == "__main__":
    execute_aogcc_scrape()
