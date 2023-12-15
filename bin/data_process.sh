#!bin/bash

pt=$1

spark-sql -d pt=20231213000000 -f ./dwd_stocks_quotes_da.sql