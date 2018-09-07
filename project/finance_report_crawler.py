#  -*- coding: utf-8 -*-

import json, traceback, urllib3
from pymongo import UpdateOne
from database import DB_CONN
from stock_util import get_all_codes
import re
from bs4 import BeautifulSoup
import requests
from lxml import etree
import os
import time
import pandas as pd 
from finance_util import finance_util


class finance_wangyi:
    
    def __init__(self):
        
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537'\
                        '.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                        'Accept-Encoding': 'gzip, deflate',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7'}

        # 各个财务财务报表的url
        self.url = 'http://quotes.money.163.com/f10/zycwzb_{0}.html#01c01'
        self.url_main_financial_factors = 'http://quotes.money.163.com/service/zycwzb_{0}.html?type=report'
        self.url_cashflow = 'http://quotes.money.163.com/service/xjllb_{0}.html'
        self.url_income = 'http://quotes.money.163.com/service/lrb_{0}.html'
        self.url_balance = 'http://quotes.money.163.com/service/zcfzb_{0}.html'

    def financial_indicators_crawler(self, codes=None, option=None):
        '''
        从网易财经上获取全部A股财务报表数据

        parameter：
            codes: 要查找的股票code, type:list   
            option: 需要爬取的财务报表名称   'None':爬取主要指标  'income':利润表   'balance': 资产负债表  'cashflow'：现金流量表
        
        return:
        DataFrame格式的数据
        '''
        if isinstance(codes, list) is False:
            codes = [codes]

        # 获取url
        url = finance_util().tables_option(option)
        # 爬取数据
        finance_data = dict()
        for code in codes:
            req = requests.get(url=url.replace('{0}', code), headers=self.headers)
            html = req.text
            Soup = BeautifulSoup(html)
            texts_of_main_financial_indicators = Soup.find_all('p')
            # 对数据进行整理
            finance_data[code] = self.clean_finance_data_wangyi(texts_of_main_financial_indicators) 
        return finance_data

    def clean_finance_data_wangyi(self, texts):
        '''
        整理数据
        '''
        temp_main_financial_indicators = re.split('[\r\n\t]', texts[0].string)
        list_main_financial_indicators = []

        for temp_one_main_financial_indicators in temp_main_financial_indicators:
            if temp_one_main_financial_indicators != '':
                list_main_financial_indicators.append(temp_one_main_financial_indicators)

        for i in range(len(list_main_financial_indicators)):
            list_main_financial_indicators[i] = re.split(',', list_main_financial_indicators[i])
        
        df_main_financial_indicators = pd.DataFrame(list_main_financial_indicators)
        df_main_financial_indicators = df_main_financial_indicators.T[: -1]
        df_main_financial_indicators.columns = df_main_financial_indicators.iloc[0]
        df_main_financial_indicators = df_main_financial_indicators[1:]
        return df_main_financial_indicators


class finance_xiaoxiang:
    def __init__(self):
        self.user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML,'\
                        'like Gecko) Chrome/66.0.3359.181 Safari/537.36'
        self.url = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?' \
                    'type=YJBB20_YJBB&token=70f12f2f4f091e459a279469fe49eca5&st=reportdate&sr=-1' \
                    '&filter=(scode={0})&p={page}&ps={pageSize}&js={"pages":(tp),"data":%20(x)}'
        self.cookie = 'emstat_bc_emcount=21446959091031597218; pgv_pvi=8471522926; st_pvi=95785429701209; _' \
                        'ga=GA1.2.700565749.1496634081; Hm_lvt_557fb74c38569c2da66471446bbaea3f=1499912514; _' \
                        'qddaz=QD.g2d11t.ydltyz.j61eq2em; ct=YTJNd7eYzkV_0WPJBmEs-FB0AGfyz7Z9G-Z1' \
                        'HbsPTxwV9TxpuvcB2fM1xoG5PhqgTI5KlrQZKFZReg3g3ltIwo8fMyzHhEzVjltYwjAigMTdZvdEHnU7QW2' \
                        'O-7u0dCkmtsFOBI4vbW1ELaZ9iUS9qPFAtIkL9M8GJTj8liRUgJY; ut=FobyicMgeV4t8TZ4Md7eLYClhCqi0w' \
                        'XPSu3ZyZ4h4Q8vWCyLMuChP80vhfidM2802fUv5AJEgl9ddudfTRqObGqQ47QN4oJS5hoWxdsHCY6lvJEeXDTNKWsdP' \
                        'hsfzg0i-ukMlT11XfPMIsBG9DzhW3xDAR3flNcqE5csB2rT3cfVPchlihFWHk-f3F1-lSsBjduc9_Ws_jjJEsi46' \
                        'xEai2mCVGd_O41yhPU3MWXl2_2QJU_ILgnzruwDvjeoQRtf8COKmiJCtE6hhy04RvSjmbzBVeZXqUhd; pi=42660' \
                        '45025913572%3bb4266045025913572%3b%e8%82%a1%e5%8f%8bZTLUIt%3bo97rhoY6b5AbF5jETm3t72EC9RGp' \
                        'IhrLsDj7myRgKyWSJmYrdl1WGaA9dMGpydaY4AptuI0ZgKDj6PCir1z%2bY1if6G0iITYI4Rv%2bPXy6H%2f4u7Rg' \
                        'iD%2f2hCYAGnfitkw9HQXnqBETzflfUGnvGJysWiVyPlOp%2fZh4Hfe6NqssBxCqJUrGOCM06F7feAXC6Vapy%2fse' \
                        '0PT2a%3bVMsSChhqtxvtvecfLmv9FInLBANRLHpns2d%2bJGh272rIXhkWm%2bNK%2bXxkRKL2a0EgScqdtlcYN1QC' \
                        'hVUWT7gmrH9py08FBPk2n5EQA9m9Zt5o2m%2bMuQhON2f66vlq%2bGk3Z66s%2brgCQhSPqoUPxluzSwBk7I9NNA%3d' \
                        '%3d; uidal=4266045025913572%e8%82%a1%e5%8f%8bZTLUIt; vtpst=|; em_hq_fls=old; emstat_ss_emco' \
                        'unt=5_1505917025_902015979; st_si=83202211429810; em-quote-version=topspeed; showpr3guide=1; ' \
                        'qgqp_b_id=367cbd71ad5c205f172815cdab571db9; hvlist=a-000858-2~a-000651-2~a-600000-1~a-300017-2' \
                        '~a-600020-1~a-600005-1~a-600004-1~a-162605-2~a-159901-2~a-600015-1~a-002364-2~a-600128-1~a-0023' \
                        '57-2~a-002363-2~a-601106-1; HAList=a-sz-300059-%u4E1C%u65B9%u8D22%u5BCC%2Ca-sz-002607-%u4E9A%u590' \
                        'F%u6C7D%u8F66%2Ca-sh-603259-%u836F%u660E%u5EB7%u5FB7%2Ca-sz-000858-%u4E94%u7CAE%u6DB2%2Ca-sh-600165' \
                        '-%u65B0%u65E5%u6052%u529B%2Ca-sh-603013-%u4E9A%u666E%u80A1%u4EFD%2Ca-sz-002841-%u89C6%u6E90%u80A1%u4' \
                        'EFD%2Cf-0-399300-%u6CAA%u6DF1300%2Cf-0-000300-%u6CAA%u6DF1300%2Ca-sz-000651-%u683C%u529B%u7535%u5668%' \
                        '2Ca-sz-000735-%u7F57%u725B%u5C71'
        self.user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                            'Chrome/66.0.3359.139 Safari/537.36'

    def crawl_single_page(page):
        pass

    def crawl_finance_report(self):
        '''
        爬去股票股票的eps,并保存到数据库中
        '''
        # 获取数据库中所有股票的code
        codes = get_all_codes()
        # 测试函数功能
        # codes = ['000001', '600048']
        # 爬取股票财务数据
        conn_pool = urllib3.PoolManager()
        for code in codes:
            try:
                reponse = conn_pool.request('GET', self.url.replace('{0}', code),
                                            headers={
                                                'cookie': self.cookie,
                                                'User-Agent': self.user_agent
                                            })
                result = json.loads(reponse.data.decode('UTF-8'))
                reports = result['data']
                # 对数据进行整理
                update_requests = []
                for report in reports:
                    doc = {
                        'report_date': report['reportdate'][0:10],
                        'announced_date': report['latestnoticedate'][0:10],
                        'eps': report['basiceps'],
                        'code': code
                    } 
                    update_requests.append(
                        UpdateOne({'code': code, 'report_date': doc['report_date']},
                        {'$set': doc}, upsert=True)
                    )
            except:
                print('ERROR: finance, code: %s, reprot_date: %s, announced_date: %s' % 
                (code, report['reportdate'], doc['annouced_date']))
            # 将数据保存到数据库中
            if len(update_requests) > 0:
                DB_CONN['finance_report'].create_index([('code', 1), ('report_date', -1)], background=True)
                request_result = DB_CONN['finance_report'].bulk_write(update_requests, ordered=False)
                print('code: %s, finance data, update: %4d, insert %4d, match: %4d' %
                 (code, request_result.modified_count, request_result.upserted_count, request_result.matched_count))


if __name__ == '__main__':
    # aa = FinancialData()
    # a, b, c = aa.get_informations()
    # bb = FinancialData()
    # bb.get_informations()
    # test = finance_wangyi()
    # data = test.financial_indicators_crawler(['600048'], option='income')
    # print(data['600048'])
    dd = finance_xiaoxiang()
    dd.crawl_finance_report()