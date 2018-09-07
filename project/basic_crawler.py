'''/*
 * @Author: PCChris: https://github.com/PCChris1995/Fiance 
 * @Date: 2018-09-07 09:35:36 
 * @Last Modified by: PCChris
 * @Last Modified time: 2018-09-07 10:16:01
 */'''
#  -*- coding: utf-8 -*-

from pymongo import UpdateOne, ASCENDING, DESCENDING
from database import DB_CONN
from datetime import datetime
import tushare as ts 
from stock_util import get_trading_dates
import os


class Crawl_basic:
    def __init__(self):
        self.basic = DB_CONN['basic']

    def crawl_basic(self, begin_date=None, end_date=None):
        '''
        从tushare中抓取指定时间段的基本数据

        parameter：
        begin_date: 开始日期
        end_date: 结束日期
        '''
        # 没有指定日期范围时设置日期
        if begin_date is None:
            begin_date = '2016-08-08'
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        # 得到日期内所有的交易日
        all_dates = get_trading_dates(begin_date, end_date)
        
        for date in all_dates:
            # 从数据库中拿到需要补充基本信息的股票代码
            # 从tushare中得到股票代码的基本数据
            # 将股票代码的基本数据补充到原有的数据当中
            # 保存更新完的股票数据
            self.crawl_basic_at_date(date)
        
    def crawl_basic_at_date(self, date=None):
        # 从指定日期中得到数据库中所有的股票代码
        # 得到股票在指定日期的基本数据
        df_basics = ts.get_stock_basics(date)

        if df_basics is None:
            return

        codes = set(df_basics.index)
        updata_requests = []

        for code in codes:
            try:
                doc = dict(df_basics.loc[code])
                time_to_market = datetime.strptime(str(doc['timeToMarket']), '%Y%m%d').strftime('%Y-%m-%d')
                totals = float(doc['totals'])
                outstanding = float(doc['outstanding'])
                doc.update({
                    'code': code,
                    'date': date,
                    'timeToMarket': time_to_market,
                    'totals': totals,
                    'outstanding': outstanding
                })
                # 将股票的基本数据补充到原有的数据当中
                updata_requests.append(
                    UpdateOne(
                        {'code': doc['code'], 'date': doc['date']},
                        {'$set': doc}, upsert=True)
                )

            except:
                print('Error!, code: %s, date: %s' % (code, date), flush=True)
                print(doc, flush=True)
                
        # 将更新好的数据保存到数据库中
        if len(updata_requests) > 0:
            self.basic.create_index([("code", 1), ("date", -1)], background=True)
            request_result = self.basic.bulk_write(updata_requests, ordered=False)
            print('save basic data, date: %s, insert: %4d, update: %4d' % 
                (date, request_result.upserted_count, request_result.modified_count), flush=True)

    def crawl_basic_one_code(self, code):
        all_dates = get_trading_dates()    
        updata_requests = []
        for date in all_dates:
            try:
                df_basics = ts.get_stock_basics(date)
                if df_basics is None:
                    print('no basic data in tushare, code %s, date %s' % (code, date))
                    continue
                doc = dict(df_basics.loc[code])
                time_to_market = datetime.strptime(str(doc['timeToMarket']), '%Y%m%d').strftime('%Y-%m-%d')
                totals = float(doc['totals'])
                outstanding = float(doc['outstanding'])
                doc.update({
                    'code': code,
                    'date': date,
                    'timeToMarket': time_to_market,
                    'totals': totals,
                    'outstanding': outstanding
                })
                # 将股票的基本数据补充到原有的数据当中
                updata_requests.append(
                    UpdateOne(
                        {'code': doc['code'], 'date': doc['date']},
                        {'$set': doc}, upsert=True)
                )

            except:
                print('Error!, code: %s, date: %s' % (code, date), flush=True)
                print(doc, flush=True)
                
        # 将更新好的数据保存到数据库中
        if len(updata_requests) > 0:
            # self.basic.create_index([("code", 1), ("date", -1)], background=True)
            request_result = self.basic.bulk_write(updata_requests, ordered=False)
            print('save basic data for one code, code: %s, date: %s, insert: %4d, update: %4d' % 
                (code, date, request_result.upserted_count, request_result.modified_count), flush=True)

if __name__ == '__main__':
    a = Crawl_basic()
    # a.crawl_basic()
    a.crawl_basic_one_code('600048')
    os.system('pause')
    