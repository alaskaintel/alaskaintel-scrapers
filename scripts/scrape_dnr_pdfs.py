import os
import json
import logging
import requests
import pdfplumber
from datetime import datetime
from io import BytesIO

# Configure Operational Logging (No sensitive info!)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# The raw intelligence feed we use as the source of truth
DNR_GEOJSON_URL = "https://alaskaintel-api.kbdesignphoto.workers.dev/dnr/land-sales?v=2"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "dnr_extracted_intel.json")

def initialize_directories():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def fetch_active_adjudications():
    """Pulls the active state DNR feed and isolates targeted adjudications."""
    logger.info("Synchronizing with State of Alaska DNR GeoJSON feed...")
    try:
        req = requests.get(DNR_GEOJSON_URL, timeout=15)
        req.raise_for_status()
        data = req.json()
        
        # We only want features that are active.
        # Ensure we safely check properties to avoid TypeErrors
        features = data.get("features", [])
        active_cases = []
        for f in features:
            props = f.get("properties", {})
            status = str(props.get("CSSTTSDSCR", "")).upper()
            
            # Target explicit cases
            if "ACTIVE ADJUDICATION" in status or "PUBLIC NOTICE" in status:
                active_cases.append(props)
                
        logger.info(f"Isolated {len(active_cases)} active adjudications out of {len(features)} total assets.")
        return active_cases
    except Exception as e:
        logger.error(f"Failed to fetch upstream DNR data: {e}")
        return []

def extract_intel_from_pdf(pdf_url):
    """
    Downloads the PDF in memory (no disk footprint) and extracts intelligence.
    WARNING: Does not emit cookies or sensitive tokens.
    """
    logger.info(f"Targeting PDF payload: {pdf_url}")
    intel_matrix = {
        "extracted_text_snippet": None,
        "deadline_found": False,
        "email_contacts": []
    }
    
    try:
        # Some state servers require a user agent
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        res = requests.get(pdf_url, headers=headers, timeout=20)
        
        if res.status_code == 200 and 'application/pdf' in res.headers.get('Content-Type', '').lower():
            pdf_bytes = BytesIO(res.content)
            
            with pdfplumber.open(pdf_bytes) as pdf:
                full_text = ""
                # Scan up to the first 5 pages to find deadlines (rarely deeper)
                pages_to_scan = min(5, len(pdf.pages))
                for i in range(pages_to_scan):
                    page = pdf.pages[i]
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n"
                        
            # Basic analysis via string heuristics
            full_text_lower = full_text.lower()
            if "comment" in full_text_lower and "deadline" in full_text_lower:
                intel_matrix["deadline_found"] = True
                
            # Extract standard state emails using regex/heuristics (simplified)
            import re
            emails = re.findall(r'[a-zA-Z0-9._%+-]+@alaska\.gov', full_text)
            intel_matrix["email_contacts"] = list(set(emails))
            
            intel_matrix["extracted_text_snippet"] = full_text[:200].replace('\n', ' ') + "..."
            
            logger.info("Successfully exfiltrated PDF text payload.")
        else:
            logger.warning(f"Payload was not a valid PDF. Content-Type: {res.headers.get('Content-Type')}")

    except Exception as e:
        logger.error(f"Failed to read PDF stream: {e}")
        
    return intel_matrix

def execute_pipeline():
    initialize_directories()
    
    active_cases = fetch_active_adjudications()
    enriched_data = []
    
    # Cap the execution during tests to avoid spamming the state servers
    # For production cron jobs, we usually scan all, but let's pause between requests
    MAX_SCRAPES = 10
    scraped_count = 0
    
    for case in active_cases:
        info_link = case.get("INFO_LINK")
        case_id = case.get("CASE_ID") or case.get("FILENUMBER") or "UNKNOWN_ID"
        
        enriched_case = {
            "case_id": case_id,
            "name": case.get("CSTMRNM", "Public Notice"),
            "status": case.get("CSSTTSDSCR"),
            "info_link": info_link,
            "intelligence": None,
            "last_scraped_at": datetime.utcnow().isoformat()
        }
        
        if info_link and scraped_count < MAX_SCRAPES:
            # Note: Many state INFO_LINKs are HTML pages that contain a link to a PDF.
            # For this pipeline, we will assume the link itself might be a direct PDF,
            # or in a phase 2, we would use BeautifulSoup to find the .pdf href on the page.
            if info_link.lower().endswith('.pdf'):
                intel = extract_intel_from_pdf(info_link)
                enriched_case["intelligence"] = intel
                scraped_count += 1
            else:
                logger.info(f"Targeting HTML node parsing for {case_id} (Phase 2 feature)")
                # Placeholder for HTML-to-PDF scrape
                pass
                
        enriched_data.append(enriched_case)
        
    # Write to local JSON ("The GitHub Repo Database")
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(enriched_data, f, indent=2)
        
    logger.info(f"Pipeline complete. Intelligence written to {OUTPUT_FILE}.")

if __name__ == "__main__":
    execute_pipeline()
