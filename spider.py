import  requests
from bs4 import BeautifulSoup
import pandas as pd
import threading
import os
import urllib3
import re 
import json
import certifi
import datetime
import time
import random



class spider():
    def __init__(self):
        urllib3.disable_warnings()               # 关闭ssl警告
        self.base = 'https://d.10jqka.com.cn'
        self.headers = urllib3.util.make_headers(accept_encoding='gzip, deflate, br',keep_alive=True,user_agent="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36")
        headers['Accept-Language'] = "zh-CN,zh;q=0.9"
        headers['Connection'] = 'keep-alive'
        headers['Accept'] = "*/*"
        self.http = urllib3.PoolManager
            (
                cert_reqs='CERT_REQUIRED',
                ca_certs=certifi.where(),  # 开启ssl证书自动验证
                num_pools=8,    # 池子的数量, 假如有10个池子, 当你访问第11个ip的时候第1个池子会被干掉, 然后建一个新的供第11个使用。一个池子是作用于同一个ip下的, 即http://aaa.com/a和http://aaa.com/b是会共用一个池子的
                maxsize=3,      # 一个池子缓存的最大连接数量.没有超过最大链接数量的链接都会被保存下来.在block为false的情况下, 添加的额外的链接不会被保存，一般多用于多线程之下, 一般情况是设置为和线程数相等的数量, 保证每个线程都能访问一个链接.
                                # 还有一个参数是block, 默认为False, 如果线程数量大于池子最大链接数量.这时设置block为true, 则会阻塞线程, 因为线程会等其他线程使用完链接,如果设置为False, 则不会阻塞线程, 但是会新开一个链接.有一个弊端是, 使用完之后这个链接会关闭, 所以如果多线程经常建立链接会影响性能, 多占用多余的资源
                timeout=3.0,    # 总超时
                retries=urllib3.Retry(connect=2, read=2, redirect=10) # 默认重试3次， 参数：connect连接重试，read读取重试，redirect重定向
            )    # 创建连接池管理对象
        # 连接数据库，获取股票列表
        conn = Conn()
        table_name = "stocks_list"
        fields = ["market", "stocks_code"]
        condition = ''
        self.select_res = conn.select_mysql(table_name, fields, condition)
        conn.close()
        # 获取当前日期时间对象
        current_date = datetime.datetime.now()
        # 格式化日期为指定格式的字符串
        self.formatted_date = current_date.strftime("%Y-%m-%d")

    # 发送请求
    def getUrl(self,market,stocks_code):
        try:
            url = self.base + '/v2/realhead/hs_' + stocks_code + '/last.js'
            res = self.http.request('GET', url, headers=self.headers)    # 发送GET请求
            status = res.status              # 请求状态码
            data = res.data.decode('utf-8')  # 返回结果
            # print(data)
            return [stocks_code,status,data]
        #记录异常请求
        except Exception  as e:
            print(e)
            file_name = './logs/spider_error_' + self.formatted_date + '.log'
            with open(file_name, "a") as file:  # ”w"代表着每次运行都覆盖内容
                file.write(stocks_code + " : " + e + "\n")
                file.close()
            return [stocks_code,'error','']


    def run(self):
        vals = []
        for row in self.select_res:
            market = row[0]
            stocks_code = row[1]
            res = getUrl(market,stocks_code)
            vals.append(res)
            time.sleep(random.uniform(0.5,2))  # 随机等待，模拟人的行为
        # 写入数据
        table_name = "skew_info"
        fields = ["task_id", "task_name", "version_id", "instance_id", "app_id","skew_loc"]
        updatefields = ["skew_list","skew_loc","ast_json"]
        conn.truncate_mysql(table_name)
        conn.insert_mysql(table_name, fields, vals)
        conn.close()























class areainfo():
    def __init__(self):
        self.areacode=''   #行政区划编码
        self.areaname=''   #行政区划名称
        self.parentcode='' #父级区划编码
        self.leve=''       #地址级别
        self.href=''       #连接地址

    def as_dict(self):
        return {'areacode': self.areacode, 'areaname': self.areaname, 'parentcode': self.parentcode,'leve': self.leve,'href': self.href}
class china_city():
    def __init__(self):
        # self.base = 'http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/2022/'
        self.base = 'http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/2015/'
        # self.base = 'http://www.stats.gov.cn/sj/tjbz/tjyqhdmhcxhfdm/2009/'
    '''
      获取web信息
    '''
    def getUrl(self,url):
        try:
            headers = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'}
            resp = requests.get(url, headers=headers)
            resp.encoding = 'gbk'
            text = resp.text
            soup = BeautifulSoup(text, "html.parser")
            return  soup
        #记录异常请求
        except  Exception  as e:
            print(e)
            with open('err.log', "a") as file:  # ”w"代表着每次运行都覆盖内容
                file.write(url  + "\n")

            return  None

    '''
       获取一级省份
    '''
    def getPronvice(self):
        soup = self.getUrl(self.base)
        if soup is None :
            return None

        provincesoups = soup.find_all(name='tr', attrs={"class": "provincetr"})  # 按照字典的形式给attrs参数赋值
        provinceList=[]
        for provincesoup in provincesoups:
            for k in provincesoup.find_all('a'):
                province = areainfo()
                province.href=k['href']
                province.areaname= k.get_text()
                province.areacode= k['href'].replace(".html","0000")
                province.parentcode="0"
                province.leve = "1"
                print(province.__dict__)
                provinceList.append(province)
        return provinceList
    '''
        获取二级城市
    '''
    def getCity(self,parent):
        url=self.base + parent.href
        list =self.initAreainfo(url,"citytr",parent.areacode,"2")
        return list
    '''
       获取三级城市
    '''
    def getCounty(self,parent):
        url = self.base + parent.href
        list  = self.initAreainfo(url,"countytr",parent.areacode,"3")
        return  list
    '''
       获取四级地址
    '''
    def getTown(self,parent):
        url = parent.href
        if url=='' :
            return None
        url = self.base + parent.areacode[0:2]+'/'+parent.href
        list = self.initAreainfo(url,"towntr",parent.areacode,"4")
        return  list
    '''
      获取五级地址
    '''
    def getVillagetr(self,parent):
        url = parent.href
        if url=='' :
            return None
        url = self.base + parent.areacode[0:2]+'/'+parent.areacode[2:4]+'/'+parent.href
        list = self.initAreainfo(url,"villagetr",parent.areacode,"5")
        return  list

    '''
       soup解析
    '''
    def initAreainfo(self,url,classname,parnetcode,leve):
        print( "页面便签 %s -- 地址等级 %s --- url  %s  \n" % (classname,leve,url))
        soup = self.getUrl(url)
        if soup is None:
            return  None

        classes = soup.find_all(name='tr', attrs={"class": classname})  # 按照字典的形式给attrs参数赋值
        list = []
        for classesoup in classes:
            group = classesoup.find_all('a')
            entity = areainfo()
            entity.leve = leve
            entity.parentcode = parnetcode
            if len(group) > 0:
                entity.href = group[0]['href']
                entity.areacode = group[0].string
                entity.areaname = group[1].string
            else:
                tds = classesoup.find_all('td')
                entity.href = ''
                if len(tds)==2 :
                    entity.areacode = tds[0].string
                    entity.areaname = tds[1].string
                if len(tds)==3:
                    entity.areacode = tds[0].string
                    entity.areaname = tds[2].string
                    entity.parentcode = parnetcode
            list.append(entity)
        return list

    '''
      通过省份获取该省份下所有地址信息
    '''
    def finAllPronvinceCity(self,pro,dir):
        listall=[]
        listall.append(pro)
        citylist =  self.getCity(pro)
        for city in citylist :
            listall.append(city)
            #print(city.__dict__)
            conlist =  self.getCounty(city)
            if conlist is not None :
                for county in conlist:
                    #print(county.__dict__)
                    listall.append(county)
                    townlist = self.getTown(county)
                    if townlist is not None:
                        for town in townlist:
                            #print(town.__dict__)
                            listall.append(town)
                            villagelist = self.getVillagetr(town)
                            if villagelist is not None:
                                listall.extend(villagelist)
        df = pd.DataFrame([x.as_dict() for x in listall])
        #print(df)
        isExists = os.path.exists(dir)
        if not isExists:
            os.makedirs(dir)
        filepath = os.path.join(dir,pro.areaname+'.xlsx')
        writer = pd.ExcelWriter(filepath)
        df.to_excel(writer, float_format='%.5f')
        writer.save()

    '''
       异步调用
    '''
    def ruanthread(self):
        provinces = self.getPronvice()
        for province in provinces:
            # threading.Thread(target= self.finAllPronvinceCity, args=(province,'F://areainfo')).start()
            threading.Thread(target= self.finAllPronvinceCity, args=(province,'C://WorkSpace//VSCODE//scan_area')).start()

    '''
      获取所有省份下所有地址信息
    '''
    def findAllCity(self,dir):
        listall=[]
        provinces = self.getPronvice()
        for pro in provinces:
            listall.append(pro)
            citylist =  self.getCity(pro)
            for city in citylist :
                listall.append(city)
                print(city.__dict__)
                conlist =  self.getCounty(city)
                if conlist is not None :
                    for county in conlist:
                        #print(county.__dict__)
                        listall.append(county)
                        '''
                        townlist = self.getTown(county)
                        if townlist is not None:
                            for town in townlist:
                                #print(town.__dict__)
                                listall.append(town)
                                villagelist = self.getVillagetr(town)
                                if villagelist is not None:
                                    listall.extend(villagelist)
                        '''
        df = pd.DataFrame([x.as_dict() for x in listall])
        #print(df)
        isExists = os.path.exists(dir)
        if not isExists:
            os.makedirs(dir)
        filepath = os.path.join(dir,'All.xlsx')
        writer = pd.ExcelWriter(filepath)
        df.to_excel(writer, float_format='%.5f')
        writer.save()

if __name__ == '__main__':

    china_city=china_city()
    # china_city.ruanthread()
    china_city.findAllCity('C://WorkSpace//VSCODE//scan_area')

