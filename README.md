# AlaskaIntel Scraper Engine

This repository hosts the zero-cost intelligence extraction pipelines for the AlaskaIntel platform. 

## Architectural Concept
By keeping this pipeline in a **Public Repository**, we leverage unlimited free GitHub Actions Linux compute automatically. This repository wakes up daily via `.github/workflows/daily_scrape.yml`, runs deep PDF extractions against heavy State of Alaska servers without consuming our own AWS/Cloudflare compute limits, and commits the structured JSON back into the `data/` directory.

The frontend (`alaskaintel.com`) simply consumes the resulting JSON payload through Cloudflare edge caching, achieving instant response times at virtually zero cost.

## Security Directives
**CRITICAL:** This repository is PUBLIC.
You **MUST NOT** include any API keys, AWS credentials, `.env` files, or internal company tokens in this codebase. 

The scrapers within this repo are designed to purely operate on open-source, unauthenticated state domains (like `dnr.alaska.gov`). If secrets are ever required (e.g., pushing to Cloudflare R2 instead of Git-Scraping), they must be securely injected via GitHub Action native secrets (`${{ secrets.R2_TOKEN }}`) and never hard-coded.
