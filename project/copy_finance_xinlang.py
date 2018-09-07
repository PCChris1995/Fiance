#  -*- coding: utf-8 -*-

import requests
import re
from lxml import etree
import os
import time
import json


'''
从新浪财经爬取所有A股上市公司的三大财务报表，理论上支持续传，但有风险
代码虽然不够简洁，但还算易懂，没有好用的免费代理，就只好用时间来换了
无有散人 2018.6
'''
#取得HTML数据
def get_html_text(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        return r.text
    except:
        return ""
#取得文件
def get_file(url,filename):
    r=requests.get(url)
    with open(filename,'wb') as f:
        f.write(r.content)
#取得股票列表，存储在字典中
def get_stocks():
    stockdict={}
    html = get_html_text("http://quote.eastmoney.com/stocklist.html")
    s = html.lower()
    page = etree.HTML(s)
    if page==None:
        return
    href = page.xpath('//a')
    for i in href:
        try:
            s = i.text
            d = re.findall(r"[630][0]\d{4}",s)
            if len(d)>0:
                n = s.find(d[0])
                d1 = s[0:n-1]
                if len(d1)>5:
                    continue
                #StockList.append(d[0])
                stockdict.update({d[0]:d1})
        except:
            continue
    return stockdict
#查看文件是否完整
def check_file(filename):
    if os.path.exists(filename):
        with open(filename,'r') as f:
            line = f.readline()
            if 'Doc' in line:#反扒了
                return False
            else:
                return True
    else:
        return False
#查看是否已经下载，供断点续传用
def check_item(k,v):
    f1 = '利润表\\' + k + '-' + v + '.csv'
    f2 = '现金流表\\' + k + '-' + v + '.csv'
    f3 = '负债表\\' + k + '-' + v + '.csv'
    if check_file(f1)==False | check_file(f2)==False | check_file(f3)==False:
        return False
    else:
        return True
#格式化输出
def format_seconds(s):
    s = int(s)
    hh = s // 3600
    s = s % 3600
    mm = s // 60
    s = s % 60
    return str(hh)+':'+str(mm)+':'+str(s)
#计算时间
start_time = time.time()

dict = get_stocks()
with open('stocks.json','w',encoding='GB18030') as f:
    json.dump(dict,f,ensure_ascii=False,indent=2)

#with open('stocks.json','r',encoding='GB18030') as f:
#    dict = json.load(f)

#建立相关文件夹
if os.path.exists('利润表')==False:
    os.makedirs('利润表')
if os.path.exists('负债表')==False:
    os.makedirs('负债表')
if os.path.exists('现金流表')==False:
    os.makedirs('现金流表')
#变量用于显示进度
icount=0
length = len(dict)
for (k,v) in dict.items():
    v = v.replace('*','#')
    icount+=1
    if check_item(k,v)==True:
        continue
    # 利润表
    url = 'http://money.finance.sina.com.cn/corp/go.php/vDOWN_ProfitStatement/displaytype/' \
    '4/stockid/600000/ctrl/all.phtml'.replace('600000',k)
    filename = '利润表\\'+k+'-'+v+'.csv'
    get_file(url,filename)
    #现金流表
    url = 'http://money.finance.sina.com.cn/corp/go.php/vDOWN_CashFlow/displaytype/4/' \
    'stockid/600000/ctrl/all.phtml'.replace('600000',k)
    filename = '现金流表\\'+k+'-'+v+'.csv'
    get_file(url,filename)
    #资产负债表
    url = 'http://money.finance.sina.com.cn/corp/go.php/vDOWN_BalanceSheet/displaytype/' \
    '4/stockid/600000/ctrl/all.phtml'.replace(
        '600000', k)
    filename = '负债表\\' + k + '-' + v + '.csv'
    get_file(url,filename)
    time_used = time.time()-start_time
    time_last = 1/ (icount/length/time_used)
    if check_item(k,v)==False:
        print('被反爬了，休息……')
        break
    print('%d of %d %.2f%% - %s %s 用时 %s 剩余 %s '%(icount,length,icount/length*100,k,v,
    format_seconds(time_used), format_seconds(time_last)))
    time.sleep(5) #不睡就死，闭着眼睛爬一夜呗，10肯定安全
