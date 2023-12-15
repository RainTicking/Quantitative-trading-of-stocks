-- 股票列表
create table stocks_list
(
market varchar(10) comment '市场',
stocks_code varchar(20) primary key comment '股票编码',
stocks_name varchar(50) comment '股票名称',
listing_date varchar(10) comment '上市日期'
) default charset = 'utf8'
;

-- 股票行情
create table stocks_quotes
(
stocks_date varchar(10) comment '股票日期',
stocks_code varchar(20) comment '股票编码',
request_status varchar(20) comment '请求状态码',
quotes_data varchar(3000) comment '股票行情',
create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
primary key (stocks_date,stocks_code)
) default charset = 'utf8'
;


-- 任务执行日志
create table task_logs
(
id int AUTO_INCREMENT PRIMARY KEY comment '任务id',
task_date varchar(10) comment '任务日期',
task_name varchar(20) comment '任务名称(spider/dataSync)',
task_status varchar(20) comment '任务状态(success/fail)',
remark varchar(100) comment '备注',
create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间'
) default charset = 'utf8'
;


