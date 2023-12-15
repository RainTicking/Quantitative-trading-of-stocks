

create database IF NOT EXISTS stg;


create table stg.stg_stocks_list(
market  string comment '市场',
stocks_code  string comment '股票编码',
stocks_name  string comment '股票名称',
listing_date  string comment '上市日期'
)
PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS ORC;


create table stg.stg_stocks_quotes(
stocks_date    string comment '股票日期' 
,stocks_code    string comment '股票编码'
,request_status string comment '请求状态码'
,quotes_data    string comment '股票行情'
,create_time    timestamp comment '创建时间'
,update_time    timestamp comment '修改时间'
)
PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS ORC;



create database dwd;

create table stg.stg_stocks_quotes(
stocks_date    string comment '股票日期' 
,stocks_code    string comment '股票编码'
,request_status string comment '请求状态码'
,quotes_data    string comment '股票行情'
,create_time    timestamp comment '创建时间'
,update_time    timestamp comment '修改时间'











)
PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS ORC;





select regexp_extract(quotes_data,'_last\\((.*)\\)',1) 
from stg.stg_stocks_quotes limit 1;



