#  -*- coding: utf-8 -*-

from database import DB_CONN
import tushare as ts 
from datetime import datetime
from pymongo import UpdateOne
import threading

'''
从tushare中保存数据到本地数据库中（暂时只有mongodb)
'''


class DailyCrawler:
    def __init__(self):
        self.daily_none = DB_CONN['daily_none']
        self.daily_hfq = DB_CONN['daily_hfq']
        
    def crawl_index(self, start_date=None, end_date=None):
        '''
        抓取指数的日线数据，并保存到本地数据库当中

        parameter:
        start_date: 开始抓取的时间
        end_date: 结束抓取的时间
        '''
        # 设定指数列表
        index_codes = ['000001', '000300', '399001', '399005', '399006']
        # 设置初始日期
        if start_date is None:
            start_date = '2008-01-01'
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        # 从tushare中获取数据
        for index in index_codes:
            qfq_index_D_data = ts.get_k_data(index, start=start_date, end=end_date)
        # 将整理后的数据保存到数据库中
            self.save_data_to_mongodb(index, qfq_index_D_data, self.daily_hfq, {'index':True})
       
    def crawl_stocks(self, codes=None, start_date=None, end_date=None):
        '''
        抓取股票数据，并保存到本地的数据库中

        parameter:
        code: 要抓取的股票代码，当为none时抓取所有的股票代码数据
        start_date: 开始抓取的时间
        end_date:结束抓取的时间
        '''
        # 设定要抓取的股票代码
        if codes is None:
            codes = list(ts.get_stock_basics().index)
        if isinstance(codes, list) is False:
            codes = [codes]
        # 设置初始日期
        if start_date is None:
            start_date = '2008-01-01'
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        # 从tushare中抓取数据
        for code in codes:
            # 抓取不复权
            None_stock_D_data = ts.get_k_data(code, autype=None, start=start_date, end=end_date) 
            # 将整理后的数据保存到本地的数据库中
            self.save_data_to_mongodb(code, None_stock_D_data, self.daily_none, {'index':False})
            # 抓取后复权
            hfq_stock_D_data = ts.get_k_data(code, autype='hfq', start=start_date, end=end_date)
            # 将整理后的数据保存到本地的数据库中
            self.save_data_to_mongodb(code, hfq_stock_D_data, self.daily_hfq, {'index':False})
  
    def save_data_to_mongodb(self, code, df_daily, collection, extra_fields=None):
        '''
        将数据写入mongodb当中

        parameter:
        codes:需要写入的代码
        df_daily:需要写入的数据
        collection:要保存的数据集
        extra_fields:除了data中的数据，其他需要格外保存的数据
        '''
        update_requests = []

        # 得到code的数据
        for date in df_daily.index:
            data = df_daily.loc[date]
            # 将数据更成规定的数据格式
            doc = self.daily_obj_2_doc(code, data)

            # 将额外的字段保存到dict中
            if extra_fields is not None:
                doc.update(extra_fields)
            # 更新需要保存的数据
            update_requests.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['date'], 'index': doc['index']},
                    {'$set': doc},
                    upsert=True)
            )

        # 将数据写入mongodb当中
        if len(update_requests) > 0:
            # 增加索引
            collection.create_index([('code', 1), ('date', -1)], background=True)
            update_result = collection.bulk_write(update_requests, ordered=False)
            print('save D_data, code: %s, index: %s, insert: %4d, update: %4d' % 
            (code, doc['index'], update_result.upserted_count, update_result.modified_count), flush=True)

    @staticmethod
    def daily_obj_2_doc(code, daily_obj):
        '''
        将数据转换为规定的格式

        parameter:
        code:股票代码
        daily_obj:需要转换的数据
        '''
        doc = {
            'code': code,
            'date': daily_obj['date'],
            'close': daily_obj['close'],
            'open': daily_obj['open'],
            'high': daily_obj['high'],
            'low': daily_obj['low'],
            'volume': daily_obj['volume']
        }
        return doc


    def threads_get_stocks(self, codes=None):
        '''
        多线程爬取股票日线数据

        使用说明：
        使用时在main中加入如下代码：
        codes, threads = DailyCrawler().threads_get_stocks()
        for i in range(len(codes)):
            threads[i].start()
         for i in range(len(codes)):
            threads[i].join()
        '''
        # codes = get_all_codes()
        if codes is None:
            codes = list(ts.get_stock_basics().index)
        threads = []
        # dates = get_trading_dates()
        for code in codes:
            t = threading.Thread(target=self.crawl_stocks, args=(code, None, None))
            threads.append(t)
        return codes, threads


if __name__ == '__main__':
    a = DailyCrawler()
    # data = a.crawl_index()
    codes, threads = a.threads_get_stocks()
    start = datetime.now()
    for i in range(len(codes)):
        threads[i].start()
    for i in range(len(codes)):
        threads[i].join()
    end = datetime.now()
    print('spend %s' % (end-start))
    # a.crawl_stocks()
    # None_stock_D_data = ts.get_k_data('600048', autype=None, start='2018-08-01', end='2018-08-31') 
    # print(None_stock_D_data)
    # codes = list(ts.get_stock_basics().index)
    # threads = []
    


