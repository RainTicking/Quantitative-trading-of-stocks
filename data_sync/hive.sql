

create database IF NOT EXISTS stg;

use stg;

create table stg_stocks_list(
market  string comment '市场',
stocks_code  string comment '股票编码',
stocks_name  string comment '股票名称',
listing_date  string comment '上市日期'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS ORC;