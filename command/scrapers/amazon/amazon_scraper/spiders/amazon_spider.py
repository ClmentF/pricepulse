from datetime import datetime, timezone

import scrapy

from ..items import AmazonPriceItem

AMAZON_BASE_URL = "https://www.amazon.fr/dp/{asin}"


class AmazonSpider(scrapy.Spider):
    name = "amazon"

    def start_requests(self):
        for entry in self.settings.get("PRODUCT_IDS", []):
            url = AMAZON_BASE_URL.format(asin=entry["asin"])
            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={"product_id": entry["product_id"]},
                # Prevent Scrapy from following Amazon's canonical redirects
                dont_filter=False,
            )

    def parse(self, response):
        product_id = response.meta["product_id"]

        product_name = self._extract_name(response)
        price_raw = self._extract_price(response)

        if not product_name:
            self.logger.warning("No product name found for %s — possible block", product_id)

        yield AmazonPriceItem(
            product_id=product_id,
            product_name=product_name or "",
            source="amazon",
            price_raw=price_raw or "",
            url=response.url,
            scraped_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _extract_name(self, response) -> str:
        return (response.css("#productTitle::text").get() or "").strip()

    def _extract_price(self, response) -> str:
        # Amazon varies its price markup; try candidates in priority order.
        candidates = [
            # Current price (most reliable — hidden full-price for screen readers)
            ".priceToPay .a-offscreen::text",
            # Standard product page
            ".a-price.a-text-price.a-size-medium.apexPriceToPay .a-offscreen::text",
            # Generic offscreen price (first match)
            ".a-price .a-offscreen::text",
            # Legacy price block IDs
            "#priceblock_ourprice::text",
            "#priceblock_dealprice::text",
            "#price_inside_buybox::text",
        ]
        for selector in candidates:
            value = response.css(selector).get()
            if value and value.strip():
                return value.strip()
        return ""
