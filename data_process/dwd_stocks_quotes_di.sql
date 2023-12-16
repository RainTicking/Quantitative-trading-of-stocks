
insert overwrite table dwd.dwd_stocks_quotes_di partition (pt='${pt}')
select
     t1.trading_date  -- 股票日期 
    ,t2.market  -- 市场
    ,t1.stocks_code  -- 股票编码
    ,t2.stocks_name  -- 股票名称
    ,t2.listing_date  -- 上市日期
    ,t1.request_status  -- 请求状态码
    ,t1.quotes_data  -- 股票行情
    ,t1.create_time  -- 创建时间
    ,t1.update_time  -- 修改时间
    ,cast(ytd_closing_price as decimal(18,2)) ytd_closing_price  -- 昨收
    ,cast(today_opening_price as decimal(18,2)) today_opening_price -- 今开
    ,cast(today_closing_price as decimal(18,2)) today_closing_price -- 今收
    ,cast(rose_percent as decimal(18,2)) rose_percent -- 涨幅%
    ,cast(rose_price as decimal(18,2)) rose_price -- 涨跌
    ,cast(limit_up as decimal(18,2)) limit_up -- 涨停
    ,cast(limit_down as decimal(18,2)) limit_down -- 跌停
    ,cast(high_price as decimal(18,2)) high_price -- 最高
    ,cast(low_price as decimal(18,2)) low_price -- 最低
    ,cast(avg_price as decimal(18,2)) avg_price -- 均价 
    ,cast(amplitude_percent as decimal(18,2)) amplitude_percent -- 振幅% 
    ,cast(deal_cnt as bigint) deal_cnt -- 成交量
    ,cast(deal_amt as decimal(18,2)) deal_amt -- 成交额 
    ,cast(turnover_rate as decimal(18,2)) turnover_rate -- 换手%
    ,cast(per_static as decimal(18,2)) per_static -- 市盈率(静)
    ,cast(per_moving as decimal(18,2)) per_moving -- 市盈率(动)
    ,cast(per_ttm as decimal(18,2)) per_ttm -- 滚动市盈率(TTM)
    ,cast(pbr as decimal(18,2)) pbr -- 市净率
    ,cast(market_cap as decimal(18,2)) market_cap -- 总市值
    ,cast(float_cap as decimal(18,2)) float_cap -- 流通市值
    ,cast(total_equity as decimal(18,2)) total_equity -- 总股本
    ,cast(float_stock as decimal(18,2)) float_stock -- 流通股
    ,cast(after_hours_deal_cnt as bigint) after_hours_deal_cnt -- 盘后成交量
    ,cast(after_hours_deal_amt as decimal(18,2)) after_hours_deal_amt -- 盘后成交额
    ,cast(super_inflow_amt as decimal(18,2)) super_inflow_amt  -- 超大单流入金额
    ,cast(big_inflow_amt as decimal(18,2)) big_inflow_amt  -- 大单流入金额
    ,cast(mid_inflow_amt as decimal(18,2)) mid_inflow_amt  -- 中单流入金额
    ,cast(small_inflow_amt as decimal(18,2)) small_inflow_amt  -- 小单流入金额
    ,cast(super_outflow_amt as decimal(18,2)) super_outflow_amt  -- 超大单流出金额
    ,cast(big_outflow_amt as decimal(18,2)) big_outflow_amt  -- 大单流出金额
    ,cast(mid_outflow_amt as decimal(18,2)) mid_outflow_amt  -- 中单流出金额
    ,cast(small_outflow_amt as decimal(18,2)) small_outflow_amt  -- 小单流出金额
    ,cast(super_inflow_amt as decimal(18,2))+cast(big_inflow_amt as decimal(18,2))
     -cast(super_outflow_amt as decimal(18,2))-cast(big_outflow_amt as decimal(18,2)) big_net_amt  -- 主力净额（大单流入金额减大单流出金额）
    ,cast(super_inflow_amt as decimal(18,2))+cast(big_inflow_amt as decimal(18,2))
     +cast(mid_inflow_amt as decimal(18,2))+cast(small_inflow_amt as decimal(18,2))
     -cast(super_outflow_amt as decimal(18,2))-cast(big_outflow_amt as decimal(18,2)) 
     -cast(mid_outflow_amt as decimal(18,2))-cast(small_outflow_amt as decimal(18,2))  net_inflow_amt  -- 资金净流入(流入金额减流出金额)
    ,cast(appoint_rate as decimal(18,2)) appoint_rate -- 委比%
    ,cast(appoint_diff as bigint) appoint_diff -- 委差
    ,cast(highest_buy_price as decimal(18,2)) highest_buy_price -- 买一价格
    ,cast(hightest_buy_cnt as bigint) hightest_buy_cnt -- 买一委托量
    ,cast(highest_sale_price as decimal(18,2))  -- 卖一价格
    ,cast(highest_sale_cnt as bigint) highest_sale_cnt -- 卖一委托量
    ,cast(outside_dish as bigint) outside_dish -- 外盘
    ,cast(inside_dish as bigint) inside_dish -- 内盘
from 
(
    select
        *
    from
    (
        select 
            stocks_date  trading_date  -- 股票日期
            ,stocks_code   -- 股票编码
            ,request_status-- 请求状态码
            ,quotes_data  -- 股票行情
            ,create_time   -- 创建时间
            ,update_time   -- 修改时间
            ,case 
                when request_status = '200' then get_json_object(regexp_extract(quotes_data,'_last\\((.*)\\)',1),'$.items') 
            end quotes_json
        from stg.stg_stocks_quotes 
        where pt = '${pt}'
    ) tt lateral view json_tuple(quotes_json,'6','7','10','199112','264648','69','70','8','9','1378761','526792','13','19','1968584','134152','2034120','3153','1149395','3541450','3475914','402','407','74','75','223','225','259','237','224','226','260','238','461256','395720','24','25','30','31','14','15')  b as 
        ytd_closing_price  -- 昨收
        ,today_opening_price  -- 今开
        ,today_closing_price  -- 今收
        ,rose_percent  -- 涨幅%
        ,rose_price  -- 涨跌
        ,limit_up  -- 涨停
        ,limit_down  -- 跌停
        ,high_price  -- 最高
        ,low_price  -- 最低
        ,avg_price  -- 均价 
        ,amplitude_percent  -- 振幅% 
        ,deal_cnt  -- 成交量
        ,deal_amt  -- 成交额 
        ,turnover_rate  -- 换手%
        ,per_static  -- 市盈率(静)
        ,per_moving  -- 市盈率(动)
        ,per_ttm   -- 滚动市盈率(TTM)
        ,pbr  -- 市净率
        ,market_cap  -- 总市值
        ,float_cap  -- 流通市值
        ,total_equity  -- 总股本
        ,float_stock  -- 流通股
        ,after_hours_deal_cnt  -- 盘后成交量
        ,after_hours_deal_amt  -- 盘后成交额
        ,super_inflow_amt  -- 超大单流入金额
        ,big_inflow_amt  -- 大单流入金额
        ,mid_inflow_amt  -- 中单流入金额
        ,small_inflow_amt  -- 小单流入金额
        ,super_outflow_amt  -- 超大单流出金额
        ,big_outflow_amt  -- 大单流出金额
        ,mid_outflow_amt  -- 中单流出金额
        ,small_outflow_amt  -- 小单流出金额
        ,appoint_rate  -- 委比%
        ,appoint_diff  -- 委差
        ,highest_buy_price  -- 买一价格
        ,hightest_buy_cnt  -- 买一委托量
        ,highest_sale_price  -- 卖一价格
        ,highest_sale_cnt  -- 卖一委托量
        ,outside_dish  -- 外盘
        ,inside_dish  -- 内盘
) t1 
left join stg.stg_stocks_list t2 
on t1.stocks_code = t2.stocks_code
and t2.pt = '${pt}'