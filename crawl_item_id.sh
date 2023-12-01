#!/usr/bin/bash

if [[ $1 =~ ^[0-9]+$ ]]; then
    scrapy crawl item_trade -a item_id=$1
fi
