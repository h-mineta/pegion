#!/bin/bash

cd /opt/pigeon-ro
PATH=$PATH:~/.local/bin
export PATH
scrapy crawl item_detail --loglevel=WARNING
