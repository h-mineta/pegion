# -*- coding: utf-8 -*-

#
# Copyright (c) 2019 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#

import re
from warnings import filterwarnings

import MySQLdb
import scrapy
import regex
from pigeon.items import ItemDetail
from scrapy.linkextractors import LinkExtractor
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.spiders import CrawlSpider, Rule
from twisted.internet.error import DNSLookupError, TimeoutError


class ItemDetailSpider(CrawlSpider):
    name = 'item_detail'

    allowed_domains = [
        'rotool.gungho.jp'
    ]

    request_loop = True

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
            sql_select = '''
                SELECT MAX(id)+1 AS next FROM `item_detail_tbl`;
            '''
            cursor.execute(sql_select)
            for row in cursor:
                if row[0] is not None:
                    self.log_index = row[0]

        self.connection.close()
        self.connection = None

        while self.request_loop is True and self.log_index > 0:
            yield scrapy.Request(
                'https://rotool.gungho.jp/torihiki/log_detail.php?log={}'.format(self.log_index),
                meta = {
                    'dont_redirect': True
                },
                errback=self.errback_httpbin,
                callback=self.parse_httpbin
            )
            self.log_index+=1

    @classmethod
    def from_crawler(cls, crawler):
        return cls(settings = crawler.settings)

    def parse_httpbin(self, response):
        matches = re.search(r"/log_detail.php\?log=([0-9]+)$", response.url)
        if matches is None:
            pass

        # HTMLエスケープされてない箇所を修正
        html_data = response.body.decode('utf-8')
        html_data = regex.sub(r'<([\P{Ascii}]+)>', r'&lt;\1&gt;', html_data)
        html_data = html_data.replace('<Overclock>', '&lt;Overclock&gt;')
        response = response.replace(body=html_data)

        world = response.xpath('//*[@id="tradebox"]/div[1]/div[1]/div[1]/p[1]/text()').get()
        if world is None or world == '':
            self.logger.warning('Got failed response from {} (status:{})'.format(response.url, response.status))
            self.request_loop = False
            pass

        self.logger.info('Got successful response from {}'.format(response.url))

        item = ItemDetail()
        item['id']        = int(matches.group(1))
        item['world']     = response.xpath('//*[@id="tradebox"]/div[1]/div[1]/div[1]/p[1]/text()').get()
        item['datetime']  = response.xpath('//*[@id="tradebox"]/div[1]/div[1]/div[2]/p[1]/text()').get()
        item['item_name'] = response.xpath('//*[@id="tradebox"]/div[1]/div[2]/text()').get()
        item['cost']      = 0
        item['count']     = 0
        item['cards']     = []
        item['enchants']  = []
        item['options']   = []
        item['smelting']  = None

        for list_tr in response.xpath('//*[@id="tradebox"]/div[2]/table[@class="datatable"]/tr'):
            key = list_tr.xpath('th[1]/text()').extract()
            value = list_tr.xpath('td[1]/text()').extract()
            if key[0] == '価格':
                item['cost'] = int(value[0].replace(',',''))
                if item['world'] == 'Noatun':
                    item['cost']*=1000 #1000倍
            elif key[0] == '個数':
                item['count'] = int(value[0].replace(',',''))
            elif key[0] == '精錬値':
                item['smelting'] = int(value[0].replace(',',''))
            elif key[0] == 'カード':
                for data in value:
                    data = data.strip()

                    data_list: list = data.split("・")

                    for value2 in data_list:
                        if value2 == "" or value2 == "なし":
                            continue

                        data_type: str = self.adjudication_data(item["item_name"], value2)

                        if data_type == "card":
                            item['cards'].append(value2)
                        elif data_type == "enchant":
                            item['enchants'].append(value2)
                        elif data_type == "option":
                            item['options'].append(value2)

        yield item

    def adjudication_data(self, name: str, value: str):
        if re.search('カード$', value) \
            or value == 'アリエス' \
            or value == 'カプリコーン' \
            or value == 'キャンサー' \
            or value == 'サジタリウス' \
            or value == 'ジェミニ' \
            or value == 'スコーピオ' \
            or value == 'タウロス' \
            or value == 'パイシーズ' \
            or value == 'リーブラ' \
            or value == 'レオ' \
            or value == 'レオの欠片' \
            or regex.search(r'^魔神の[\P{Ascii}]+\d$', value) \
            or re.search(r'^.*カード\(逆位置\)$', value):
            return "card"

        elif re.search(r"^アビス", name) \
            or re.search(r"^ディーヴァ", name) \
            or re.search(r"^ニーヴ", name) \
            or re.search(r"^ラーヴァ", name) \
                and (re.search(r"^物理攻撃時、", value) \
                or re.search(r"^魔法攻撃時、", value) \
                or re.search(r"^.属性攻撃で受ける", value) \
                or re.search(r"^武器に.属性を付与する$", value) \
                or re.search(r"^ボスモンスターから受けるダメージ", value) \
                or re.search(r"^Atk \+ \d+$", value) \
                or re.search(r"^Def \+ \d+$", value) \
                or re.search(r"^Matk \+ \d+$", value) \
                or re.search(r"^Mdef \+ \d+$", value) \
                or re.search(r"^Str \+ \d+$", value) \
                or re.search(r"^Agi \+ \d+$", value) \
                or re.search(r"^Vit \+ \d+$", value) \
                or re.search(r"^Int \+ \d+$", value) \
                or re.search(r"^Dex \+ \d+$", value) \
                or re.search(r"^Luk \+ \d+$", value) \
                or re.search(r"^Hit \+ \d+$", value) \
                or re.search(r"^Flee \+ \d+$", value) \
                or re.search(r"^MaxSP \+ \d+", value) \
                or re.search(r"^MaxHP \+ \d+", value) \
                or re.search(r"^スキルディレイ \- \d+", value) \
                ):
            return "option"

        else:
            return "enchant"


    def errback_httpbin(self, failure):
        # log all failures
        self.logger.error(repr(failure))
        self.request_loop = False

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
