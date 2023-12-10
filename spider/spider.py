# coding:utf-8

import urllib3
import certifi
import datetime
import time
import random
import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
print(os.getcwd())
import sys
sys.path.append(os.getcwd()) # 引入conn模块在主程序所在目录的父目录下
from connDB import conn



class spider():
    def __init__(self):
        urllib3.disable_warnings()               # 关闭ssl警告
        self.base = 'https://d.10jqka.com.cn'
        self.headers = urllib3.util.make_headers(accept_encoding='gzip, deflate, br',keep_alive=True,user_agent="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36")
        self.headers['Accept-Language'] = "zh-CN,zh;q=0.9"
        self.headers['Connection'] = 'keep-alive'
        self.headers['Accept'] = "*/*"
        self.http = urllib3.PoolManager(
                cert_reqs='CERT_REQUIRED',
                ca_certs=certifi.where(),  # 开启ssl证书自动验证
                num_pools=8,    # 池子的数量, 假如有10个池子, 当你访问第11个ip的时候第1个池子会被干掉, 然后建一个新的供第11个使用。一个池子是作用于同一个ip下的, 即http://aaa.com/a和http://aaa.com/b是会共用一个池子的
                maxsize=3,      # 一个池子缓存的最大连接数量.没有超过最大链接数量的链接都会被保存下来.在block为false的情况下, 添加的额外的链接不会被保存，一般多用于多线程之下, 一般情况是设置为和线程数相等的数量, 保证每个线程都能访问一个链接.
                                # 还有一个参数是block, 默认为False, 如果线程数量大于池子最大链接数量.这时设置block为true, 则会阻塞线程, 因为线程会等其他线程使用完链接,如果设置为False, 则不会阻塞线程, 但是会新开一个链接.有一个弊端是, 使用完之后这个链接会关闭, 所以如果多线程经常建立链接会影响性能, 多占用多余的资源
                timeout=3.0,    # 总超时
                retries=urllib3.Retry(connect=2, read=2, redirect=10) # 默认重试3次， 参数：connect连接重试，read读取重试，redirect重定向
            )    # 创建连接池管理对象
        # 获取当前日期时间对象
        current_time = datetime.datetime.now()
        new_time = current_time - datetime.timedelta(hours=8) #将当前时间减去8小时
        # 格式化日期为指定格式的字符串
        self.formatted_date = new_time.strftime("%Y-%m-%d")
        # 连接数据库，获取股票列表
        self.mysqlConn = conn.Conn()

    '''
      发送爬虫请求
    '''
    def getUrl(self,market,stocks_code):
        try:
            url = self.base + '/v2/realhead/hs_' + stocks_code + '/last.js'
            # print("url:",url)
            res = self.http.request('GET', url, headers=self.headers)    # 发送GET请求
            status = res.status              # 请求状态码
            data = res.data.decode('utf-8')  # 返回结果
            # print(data)
            return [stocks_code,status,data]
        #记录异常请求
        except Exception  as e:
            print("request: %s error：%s" % (stocks_code, e))
            current_datetime = datetime.datetime.now()
            formatted_time = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
            file_name = './logs/spider_error_' + self.formatted_date + '.log'
            with open(file_name, "a") as file:  # ”w"代表着每次运行都覆盖内容
                file.write(formatted_time + ' crawl: ' + stocks_code + ' error: ' + str(e) + '\n')
                file.close()
            return [stocks_code,'error','']

    '''
      运行爬虫程序，结果写入mysql
    '''
    def run(self):
        i = 1
        vals = []
        # 查询股票清单
        table_name = "stocks_list"
        fields = ["market", "stocks_code"]
        condition = "stocks_code not in(select stocks_code from stocks_quotes where request_status = '200' and stocks_date= '" + self.formatted_date + "')"
        select_res = self.mysqlConn.select_mysql(table_name, fields, condition)
        # 写入股票行情
        table_name = "stocks_quotes"
        fields = ["stocks_date", "stocks_code", "request_status", "quotes_data"]
        updatefields = ["request_status","quotes_data"]
        for row in select_res:
            market = row[0]
            stocks_code = row[1]
            res = [self.formatted_date] + self.getUrl(market,stocks_code)
            print(i,market,stocks_code,res[2])
            vals.append(res)
            time.sleep(random.uniform(0.5,3))  # 随机等待，模拟人的行为
            if i % 10 == 0:
                self.mysqlConn.insert_mysql(table_name, fields, vals, updatefields)
                vals = []
            i += 1
        self.mysqlConn.insert_mysql(table_name, fields, vals, updatefields)
        # 写入成功日志
        table_name = "stocks_quotes"
        fields = ["stocks_code"]
        condition = "stocks_date= '" + self.formatted_date + "' and request_status != '200'"
        fail_res = self.mysqlConn.select_mysql(table_name, fields, condition)
        fail_cnt = '%s' % len(fail_res)
        task_status = 'success'
        remark = 'all successful records'
        if fail_cnt != '0':
            task_status = 'fail'
            remark = fail_cnt + ' failed records'
        table_name = "task_logs"
        fields = ["task_date", "task_name", "task_status", "remark"]
        vals = [[self.formatted_date, 'spider', task_status, remark]]
        self.mysqlConn.insert_mysql(table_name, fields, vals)
        return fail_cnt

if __name__ == '__main__':
    mySpider = spider()
    i = 1
    fail_cnt = mySpider.run()
    while fail_cnt != '0' and i < 3:
        fail_cnt = mySpider.run()
        i += 1
    mySpider.mysqlConn.close()



