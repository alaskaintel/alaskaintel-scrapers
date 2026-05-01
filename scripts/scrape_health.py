import os
import json
import logging
import feedparser
import requests
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | HEALTH-ENGINE | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "health_intel.json")

# ══════════════════════════════════════════════════════════════════════
# VERIFIED PRODUCTION ENDPOINTS — Alaska Healthcare Intel
# ══════════════════════════════════════════════════════════════════════

FEEDS = [
    # Tier 1 (REAL-TIME ALERTS)
    {"url": "http://www.cdc.gov/media/rss.xml", "source": "CDC Media Relations News", "tier": 1},
    {"url": "https://alaskabeacon.com/feed", "source": "Alaska Beacon", "tier": 1},
    
    # Tier 2 (SYSTEM SIGNALS)
    {"url": "https://www.southcentralfoundation.com/feed/", "source": "Southcentral Foundation", "tier": 2},
    {"url": "https://tools.cdc.gov/api/v2/resources/media/342778.rss", "source": "CDC MMWR", "tier": 2},
    
    # Tier 3 (CONTEXT + BACKGROUND)
    {"url": "https://www.nomenugget.net/rss.xml", "source": "Nome Nugget", "tier": 3},
    {"url": "https://www.juneauempire.com/feed/", "source": "Juneau Empire", "tier": 3},
    {"url": "https://www.homernews.com/feed/", "source": "Homer News", "tier": 3},
    {"url": "https://thecordovatimes.com/feed/", "source": "The Cordova Times", "tier": 3},
    {"url": "https://alaska-native-news.com/feed/", "source": "Alaska Native News", "tier": 3},
]

def ensure_output_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def analyze_item(title, description):
    """
    Alert Scoring System using heuristic keywords.
    Infers severity, category, impact, and action.
    """
    text = f"{title} {description}".lower()
    
    # Severity Scoring
    severity = "low"
    if any(k in text for k in ["outbreak", "death", "pandemic", "emergency", "crisis", "severe", "critical", "warning", "fatal"]):
        severity = "high"
    elif any(k in text for k in ["policy", "hospital", "strain", "case", "flu", "virus", "alert", "funding", "shortage"]):
        severity = "medium"
        
    # Category Identification
    category = "policy"
    if any(k in text for k in ["outbreak", "virus", "covid", "flu", "disease", "infection", "measles", "syphilis", "tuberculosis"]):
        category = "outbreak"
    elif any(k in text for k in ["hospital", "capacity", "providence", "icu", "bed", "staff", "expansion", "clinic", "treatment"]):
        category = "hospital"
        
    # Impact Region
    impact = "statewide"
    if any(k in text for k in ["rural", "village", "remote", "tribal"]):
        impact = "rural / tribal communities"
    elif any(k in text for k in ["anchorage", "matsu", "mat-su", "valley", "fairbanks", "juneau", "kenai"]):
        impact = "regional hubs"
        
    # Action
    action = "monitor"
    if severity == "high":
        action = "immediate review required"
    elif category == "outbreak":
        action = "review transmission risk"
    elif category == "policy":
        action = "assess legislative impact"
        
    return {
        "severity": severity,
        "category": category,
        "impact": impact,
        "action": action
    }

def execute_health_scrape():
    ensure_output_dir()
    logger.info("═══ HEALTHCARE INTELLIGENCE EXTRACTION PROTOCOL INITIATED ═══")
    
    all_alerts = []
    
    for feed in FEEDS:
        logger.info(f"─── SCRAPING TIER {feed['tier']}: {feed['source']} ───")
        try:
            # Use requests with a browser User-Agent to avoid blocks
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            res = requests.get(feed['url'], headers=headers, timeout=15)
            res.raise_for_status()
            
            parsed = feedparser.parse(res.content)
            logger.info(f"Found {len(parsed.entries)} entries in {feed['source']}")
            
            for entry in parsed.entries[:15]: # Take top 15 from each feed to prevent bloat
                title = entry.get('title', '')
                description = entry.get('summary', entry.get('description', ''))
                link = entry.get('link', '')
                published = entry.get('published', entry.get('updated', datetime.utcnow().isoformat()))
                
                # Filter feeds that are not exclusively health/Alaska related
                text_to_check = f"{title} {description}".lower()
                is_cdc = "CDC" in feed['source']
                
                if is_cdc:
                    # CDC feeds must mention Alaska or related terms
                    if not any(k in text_to_check for k in ["alaska", "arctic", "tribal", "native", "rural", "village", "seafood"]):
                        continue
                else:
                    # General Alaska news feeds must mention health terms to be included
                    if feed['source'] not in ["Southcentral Foundation"]:
                        health_keywords = ["health", "hospital", "clinic", "disease", "outbreak", "virus", "flu", "medicaid", "behavioral", "care", "doctor", "nurse", "patient", "medical"]
                        if not any(k in text_to_check for k in health_keywords):
                            continue

                
                analysis = analyze_item(title, description)
                
                alert = {
                    "id": entry.get('id', link),
                    "type": "health_alert",
                    "source": feed['source'],
                    "tier": feed['tier'],
                    "title": title,
                    "description": description[:500] + "..." if len(description) > 500 else description,
                    "link": link,
                    "published": published,
                    "region": analysis['impact'],
                    "severity": analysis['severity'],
                    "category": analysis['category'],
                    "impact": analysis['impact'],
                    "action": analysis['action']
                }
                all_alerts.append(alert)
                
        except Exception as e:
            logger.error(f"Failed to scrape {feed['source']} ({feed['url']}): {e}")

    # Sort by severity (high -> medium -> low) and tier (1 -> 2 -> 3)
    severity_rank = {"high": 1, "medium": 2, "low": 3}
    all_alerts.sort(key=lambda x: (x['tier'], severity_rank[x['severity']]))

    # ── Assemble Intelligence Payload ──
    intel_payload = {
        "metadata": {
            "last_synced": datetime.utcnow().isoformat(),
            "target": "Alaska Healthcare Intelligence Network",
            "sources": [f['source'] for f in FEEDS],
            "status": "Operational",
            "total_alerts": len(all_alerts)
        },
        "alerts": all_alerts
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(intel_payload, f, indent=2)

    logger.info(f"═══ HEALTHCARE PAYLOAD WRITTEN: {OUTPUT_FILE} ═══")
    logger.info(f"    Total Alerts Extracted: {len(all_alerts)}")

if __name__ == "__main__":
    execute_health_scrape()
