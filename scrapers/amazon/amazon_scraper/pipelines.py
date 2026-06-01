import json
import logging
import requests
from itemadapter import ItemAdapter

logger = logging.getLogger(__name__)


class NiFiPipeline:
    def open_spider(self, spider):
        self.nifi_url = spider.settings.get(
            "NIFI_ENDPOINT", "http://localhost:8082/contentListener"
        )
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def process_item(self, item, spider):
        payload = dict(ItemAdapter(item))

        try:
            response = self.session.post(
                self.nifi_url,
                data=json.dumps(payload),
                timeout=10,
            )
            response.raise_for_status()
            logger.info("Posted %s to NiFi [HTTP %s]", payload["product_id"], response.status_code)
        except requests.RequestException as exc:
            logger.error("Failed to post %s to NiFi: %s", payload["product_id"], exc)

        return item

    def close_spider(self, spider):
        self.session.close()
