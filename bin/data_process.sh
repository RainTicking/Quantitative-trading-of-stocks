#!bin/bash

source /etc/profile

pt=$1

spark-sql -d pt=$pt -f /home/hadoop/Work/Quantitative-trading-of-stocks/dwd_stocks_quotes_di.sql