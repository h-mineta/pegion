# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field

class ItemDetail(Item):
    id        = Field()
    world     = Field()
    datetime  = Field()
    item_name = Field()
    cost      = Field()
    count     = Field()
    cards     = Field()
    enchants  = Field()
    options   = Field()
    refining  = Field()
