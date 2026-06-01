import scrapy


class AmazonPriceItem(scrapy.Item):
    product_id = scrapy.Field()
    product_name = scrapy.Field()
    source = scrapy.Field()
    price_raw = scrapy.Field()
    url = scrapy.Field()
    scraped_at = scrapy.Field()
