# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field

class ItemTrade(Item):
    item_name      = Field()
    log_date       = Field()
    world          = Field()
    map_name       = Field()
    price          = Field()
    item_count     = Field()
    cards          = Field()
    random_options = Field()
    refining_level = Field()
