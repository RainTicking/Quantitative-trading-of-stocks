import urllib3
import certifi

base = 'https://d.10jqka.com.cn'
headers = urllib3.util.make_headers(accept_encoding='gzip, deflate, br',keep_alive=True,user_agent="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36")
headers['Accept-Language'] = "zh-CN,zh;q=0.9"
headers['Connection'] = 'keep-alive'
headers['Accept'] = "*/*"
http = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED',
        ca_certs=certifi.where(),  
        num_pools=8,    
        maxsize=3,      
        timeout=3.0,  
        retries=urllib3.Retry(connect=2, read=2, redirect=10)
        )  

stocks_code = '000005'
url = base + '/v2/realhead/hs_' + stocks_code + '/last.js'
print("url:",url)
res = http.request('GET', url, headers=headers)  
status = res.status          
data = res.data.decode('utf-8')
print(status,data)


