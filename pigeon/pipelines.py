# -*- coding: utf-8 -*-

#
# Copyright (c) 2023 h-mineta <h-mineta@0nyx.net>
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
            'user'       : settings.get('MYSQL_USER', 'pigeon'),
            'passwd'     : settings.get('MYSQL_PASSWORD', 'pigeonpw!'),
            'db'         : settings.get('MYSQL_DATABASE', 'pigeon'),
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
                CREATE TABLE IF NOT EXISTS `item_trade_tbl` (
                `id` bigint(1) UNSIGNED NOT NULL AUTO_INCREMENT,
                `item_name` varchar(255) NOT NULL,
                `log_date` datetime NOT NULL,
                `world` varchar(16) NOT NULL,
                `map_name` text DEFAULT NULL,
                `price` bigint(1) UNSIGNED NOT NULL,
                `unit_price` bigint(1) UNSIGNED NOT NULL,
                `item_count` int(1) UNSIGNED NOT NULL,
                `cards` json DEFAULT NULL,
                `random_options` json DEFAULT NULL,
                `refining_level` int(1) UNSIGNED DEFAULT NULL,
                `update_time` timestamp NOT NULL DEFAULT current_timestamp(),
                PRIMARY KEY (`id`),
                UNIQUE KEY `item_name-log_date` (`item_name`, `log_date`, `world`, `map_name`, `price`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='アイテム取引テーブル' ROW_FORMAT=DYNAMIC AUTO_INCREMENT=20000001;
            '''
            cursor.execute(sql_create_tbl)

            self.connection.commit()

        self.connection.close()
        self.connection = None

    def open_spider(self, spider):
        self.connection = MySQLdb.connect(**self.mysql_args)
        self.connection.autocommit(True)

    def close_spider(self, spider):
        if self.connection:
            try:
                #self.connection.commit()
                self.connection.close()
            except MySQLdb.Error as ex:
                self.logger.warning(ex)

    def process_item(self, item, spider):
        if spider.name in ["item_trade"]:
            self.process_item_detail(item, spider)

        return item

    def process_item_detail(self, item, spider):
        sql_insert = '''
            INSERT IGNORE INTO item_trade_tbl(
                id,
                item_name,
                log_date,
                world,
                map_name,
                price,
                unit_price,
                item_count,
                cards,
                random_options,
                refining_level
            )
            VALUES(
                NULL,
                %s,
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
                    item["item_name"],
                    item["log_date"],
                    item["world"],
                    item["map_name"],
                    item["price"],
                    int(item["price"] / item["item_count"]),
                    item["item_count"],
                    json.dumps(item["cards"], ensure_ascii=False),
                    json.dumps(item["random_options"], ensure_ascii=False),
                    item["refining_level"]
                    ))

        except MySQLdb.IntegrityError as ex:
            pass

        except MySQLdb.Error as ex:
            #self.connection.rollback()
            self.logger.error(ex)
