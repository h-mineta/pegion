# -*- coding: utf-8 -*-

#
# Copyright (c) 2018 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#
from warnings import filterwarnings
import json
import MySQLdb

class MysqlPipeline(object):
    def __init__(self, settings, *args, **kwargs):
        filterwarnings('ignore', category = MySQLdb.Warning)

        self.mysql_args = {
            'host'       : settings.get('MYSQL_HOST', 'localhost'),
            'port'       : settings.get('MYSQL_PORT', 3306),
            'user'       : settings.get('MYSQL_USER', 'pegion'),
            'passwd'     : settings.get('MYSQL_PASSWORD', 'pegionpw!'),
            'db'         : settings.get('MYSQL_DATABASE', 'pegion'),
            'unix_socket': settings.get('MYSQL_UNIXSOCKET', '/var/lib/mysql/mysql.sock'),
            'charset'    : 'utf8mb4'
        }

        self.initialize()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            settings         = crawler.settings,
        )

    def initialize(self):
        self.connection = MySQLdb.connect(**self.mysql_args)
        self.connection.autocommit(False)

        with self.connection.cursor() as cursor:

            sql_create_tbl = '''
                CREATE TABLE IF NOT EXISTS `item_detail_tbl` (
                `id` bigint(1) UNSIGNED NOT NULL,
                `world` varchar(16) NOT NULL,
                `datetime` datetime NOT NULL,
                `item_name` varchar(255) NOT NULL,
                `cost` bigint(1) UNSIGNED NOT NULL,
                `count` int(1) UNSIGNED NOT NULL,
                `cards` json DEFAULT NULL,
                `enchants` json DEFAULT NULL,
                `smelting` int(1) UNSIGNED DEFAULT NULL,
                `update_time` timestamp NOT NULL DEFAULT current_timestamp(),
                PRIMARY KEY (`id`),
                KEY `datetime` (`datetime`),
                KEY `item_name` (`item_name`),
                KEY `world-item_name` (`world`, `item_name`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='アイテム詳細テーブル' ROW_FORMAT=DYNAMIC;
            '''
            cursor.execute(sql_create_tbl)

            self.connection.commit()

        self.connection.close()
        self.connection = None

    def open_spider(self, spider):
        self.connection = MySQLdb.connect(**self.mysql_args)
        self.connection.autocommit(False)

    def close_spider(self, spider):
        if self.connection:
            try:
                self.connection.commit()
                self.connection.close()
            except MySQLdb.Error as ex:
                self.logger.warning(ex)

    def process_item(self, item, spider):
        if spider.name in ['item_detail']:
            self.process_item_detail(item, spider)

        return item

    def process_item_detail(self, item, spider):
        sql_insert = '''
            INSERT IGNORE INTO item_detail_tbl(
                id,
                world,
                `datetime`,
                item_name,
                cost,
                count,
                cards,
                enchants,
                smelting
            )
            VALUES(
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            ;
        '''

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_insert,(
                    item['id'],
                    item['world'],
                    item['datetime'],
                    item['item_name'],
                    item['cost'],
                    item['count'],
                    json.dumps(item['cards'], ensure_ascii=False),
                    json.dumps(item['enchants'], ensure_ascii=False),
                    item['smelting']
                    ))

        except MySQLdb.Error as ex:
            self.connection.rollback()
            self.logger.error(ex)
