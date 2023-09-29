# -*- coding: utf-8 -*-

#
# Copyright (c) 2023 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

import json
import re
from warnings import filterwarnings

import MySQLdb
import scrapy
from pigeon.items import ItemTrade
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.spiders import CrawlSpider
from twisted.internet.error import DNSLookupError, TimeoutError


class ItemTradeLogFilteredSearch(CrawlSpider):
    name = 'item_trade'

    allowed_domains = [
        'rotool.gungho.jp'
    ]

    def __init__(self, settings, *args, **kwargs):
        filterwarnings('ignore', category = MySQLdb.Warning)

        self.mysql_args = {
            'host'       : settings.get('MYSQL_HOST', 'localhost'),
            'port'       : settings.get('MYSQL_PORT', 3306),
            'user'       : settings.get('MYSQL_USER', 'pigeon'),
            'passwd'     : settings.get('MYSQL_PASSWORD', 'pigeonpw!'),
            'db'         : settings.get('MYSQL_DATABASE', 'pigeon'),
            'unix_socket': settings.get('MYSQL_UNIXSOCKET', '/var/lib/mysql/mysql.sock'),
            'charset'    : 'utf8mb4'
        }

        self.log_index = settings.get('ITEM_START_INDEX', 0)

    def start_requests(self):
        self.connection = MySQLdb.connect(**self.mysql_args)
        self.connection.autocommit(False)

        with self.connection.cursor() as cursor:
            sql_select: str = '''
                SELECT item_id FROM `item_data_tbl`
                ORDER BY 1 ASC;
            '''

            cursor.execute(sql_select)

            for row in cursor:
                yield scrapy.Request(
                    "https://rotool.gungho.jp/item_trade_log_filtered_search/?item_id={}".format(row[0]),
                    meta = {
                        "dont_redirect": True
                    },
                    errback=self.errback_httpbin,
                    callback=self.parse_httpbin
                )

        self.connection.close()
        self.connection = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(settings = crawler.settings)

    def parse_httpbin(self, response):
        matches = re.search(r"/item_trade_log_filtered_search/.*$", response.url)
        if matches is None:
            return

        # HTMLエスケープされてない箇所を修正
        data_strings = response.body.decode("utf-8")
        if data_strings is not None and data_strings == '"none"':
            return

        try:
            data_json = json.loads(data_strings)
            self.logger.info('Got successful response from {}'.format(response.url))
        except:
            self.logger.warning('Got failed response from {}'.format(response.url))
            return

        for original in data_json:
            item = ItemTrade()
            item["item_name"]      = original["item_name"]
            item["log_date"]       = original["log_date"]
            item["world"]          = original["world"]
            item["map_name"]       = original["mapname"]
            item["price"]          = int(original["price"])
            item["item_count"]     = int(original["item_count"])
            item["cards"]          = [original["card1"], original["card2"], original["card3"], original["card4"]]
            item["random_options"] = [original["RandOption1"], original["RandOption3"], original["RandOption3"], original["RandOption4"], original["RandOption5"]]
            item["refining_level"] = int(original["refining_level"])

            yield item

    def errback_httpbin(self, failure):
        # log all failures
        self.logger.error(repr(failure))

        if failure.check(HttpError):
            # you can get the response
            response = failure.value.response
            self.logger.warning('HttpError on {} (status:{})'.format(response.url, response.status))

        #elif isinstance(failure.value, DNSLookupError):
        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.warning('DNSLookupError on {}'.format(request.url))

        #elif isinstance(failure.value, TimeoutError):
        elif failure.check(TimeoutError):
            request = failure.request
            self.logger.warning('TimeoutError on {}'.format(request.url))
