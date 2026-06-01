import json
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BOT_NAME = "amazon_scraper"
SPIDER_MODULES = ["amazon_scraper.spiders"]
NEWSPIDER_MODULE = "amazon_scraper.spiders"

# --- Anti-bot ---
ROBOTSTXT_OBEY = False
CONCURRENT_REQUESTS = 1
DOWNLOAD_DELAY = 2
RANDOMIZE_DOWNLOAD_DELAY = True

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "amazon_scraper.middlewares.RotateUserAgentMiddleware": 400,
}

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0

# --- Pipeline ---
ITEM_PIPELINES = {
    "amazon_scraper.pipelines.NiFiPipeline": 300,
}

NIFI_ENDPOINT = os.getenv("NIFI_ENDPOINT", "http://localhost:8082/contentListener")

# --- Product list ---
_product_ids_path = Path(__file__).parent.parent / "product_ids.json"
PRODUCT_IDS = json.loads(_product_ids_path.read_text())

# --- Misc ---
HTTPCACHE_ENABLED = False
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
TELEMETRY_ENABLED = False
