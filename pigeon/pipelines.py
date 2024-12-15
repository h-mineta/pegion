# -*- coding: utf-8 -*-

#
# Copyright (c) 2023 h-mineta <h-mineta@0nyx.net>
# This software is released under the MIT License.
#
import json

from sqlalchemy import create_engine, text

class MysqlPipeline(object):
    def __init__(self, settings, *args, **kwargs):
        mysql_args = {
            'host'       : settings.get('MYSQL_HOST', 'localhost'),
            'port'       : settings.get('MYSQL_PORT', 3306),
            'user'       : settings.get('MYSQL_USER', 'pigeon'),
            'passwd'     : settings.get('MYSQL_PASSWORD', 'pigeonpw!'),
            'dbname'     : settings.get('MYSQL_DATABASE', 'pigeon'),
            'unix_socket': settings.get('MYSQL_UNIXSOCKET', '/var/lib/mysql/mysql.sock'),
            'charset'    : 'utf8mb4'
        }

        self.sqlalchemy_url: str = "mysql+pymysql://{user:s}:{passwd:s}@{host:s}:{port:d}/{dbname:s}?charset={charset:s}"\
            .format(**mysql_args)

        self.initialize()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            settings         = crawler.settings,
        )

    def initialize(self):
        engine = create_engine(self.sqlalchemy_url)
        with engine.connect() as session:
            sql_create_tbl = '''
                CREATE TABLE IF NOT EXISTS `item_trade_tbl` (
                `id` bigint(1) UNSIGNED NOT NULL AUTO_INCREMENT,
                `item_id` bigint(1) UNSIGNED DEFAULT NULL,
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
                KEY `item_id` (`item_id`),
                KEY `item_name` (`item_name`),
                UNIQUE KEY `unique_trade` (`item_name`, `log_date`, `world`, `price`, `refining_level`, `cards`, `random_options`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='アイテム取引テーブル' ROW_FORMAT=DYNAMIC AUTO_INCREMENT=20000001;
            '''
            session.execute(text(sql_create_tbl))
            session.commit()

    def open_spider(self, spider):
        engine = create_engine(self.sqlalchemy_url)
        self.session = engine.connect()

    def close_spider(self, spider):
        if self.session:
            try:
                #self.connection.commit()
                self.session.close()
            except Exception as ex:
                self.logger.warning(ex)

    def process_item(self, item, spider):
        if spider.name in ["item_trade"]:
            self.process_item_detail(item, spider)

        return item

    def process_item_detail(self, item, spider):
        sql_insert = '''
            INSERT IGNORE INTO item_trade_tbl(
                id,
                item_id,
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
                :item_id,
                :item_name,
                :log_date,
                :world,
                :map_name,
                :price,
                :unit_price,
                :item_count,
                :cards,
                :random_options,
                :refining_level
            )
            ;
        '''

        try:
            self.session.execute(
                text(sql_insert),
                {
                    "item_id"        : item["item_id"],
                    "item_name"      : item["item_name"],
                    "log_date"       : item["log_date"],
                    "world"          : item["world"],
                    "map_name"       : item["map_name"],
                    "price"          : item["price"],
                    "unit_price"     : int(item["price"] / item["item_count"]),
                    "item_count"     : item["item_count"],
                    "cards"          : json.dumps(item["cards"], ensure_ascii=False),
                    "random_options" : json.dumps(item["random_options"], ensure_ascii=False),
                    "refining_level" : item["refining_level"]
                    }
                )
            self.session.commit()

        except Exception as ex:
            self.logger.error(ex)
