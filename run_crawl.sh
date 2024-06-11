#!/bin/bash

cd /opt/pigeon-ro
PATH=$PATH:~/.local/bin
export PATH
while true; do
    scrapy crawl item_trade --loglevel=WARNING
done
