import  requests
from bs4 import BeautifulSoup
import pandas as pd
import threading
import os
import urllib3
import re 
import json

# A股：
# https://d.10jqka.com.cn/v2/realhead/hs_688578/last.js


url = 'https://d.10jqka.com.cn/v6/realhead/hk_HK0005/last.js'
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'}
http = urllib3.PoolManager()   # 创建连接池管理对象
res = http.request('GET', url, headers=headers)    # 发送GET请求
# print(r.status)                # 打印请求状态码
data = res.data.decode('utf-8')  # 返回结果

items = re.search(r'\((.*)\)',res).group(1)  # 成交指标


data = json.loads(items)
print(data['items']['10'])










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

