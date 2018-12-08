# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
class CmindexItem(scrapy.Item):

    # define the fields for your item here like:
    name=scrapy.Field()
    slug=scrapy.Field()
    logo=scrapy.Field()
    symbol=scrapy.Field()
    price=scrapy.Field()
    priceBtc=scrapy.Field()
    priceNames=scrapy.Field()
    priceValues=scrapy.Field()
    stats_detail=scrapy.Field()
    market_cap=scrapy.Field()
    volumne_24h=scrapy.Field()
    circulating_supply=scrapy.Field()
    total_supply=scrapy.Field()
    max_supply=scrapy.Field()
    explorer=scrapy.Field()
    tags=scrapy.Field()
    website=scrapy.Field()
    rank=scrapy.Field()
    message_board=scrapy.Field()
    chat=scrapy.Field()
    source_cdoe=scrapy.Field()
    tech_doc=scrapy.Field()
    announcement=scrapy.Field()
    image_url = scrapy.Field()



