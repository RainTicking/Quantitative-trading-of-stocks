import urllib3
import certifi

base = 'https://d.10jqka.com.cn'
headers = {"User_Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"}
http = urllib3.ProxyManager('http://101.34.72.57:7890')  

stocks_code = '000005'
url = base + '/v2/realhead/hs_' + stocks_code + '/last.js'
print("url:",url)
res = http.request('GET', url, headers=headers)  
status = res.status          
data = res.data.decode('utf-8')
print(status,data)


import urllib3
proxy_http = urllib3.ProxyManager('http://114.231.45.138:8089')
headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36"}
resp = proxy_http.request('GET','https://www.baidu.com/',headers=headers)
print(resp.headers)#响应头信息
print(resp.status)#状态码
resp.release_conn()#释放这个http连接
