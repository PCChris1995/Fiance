# /*
#  * @Author: PCChris: https://github.com/PCChris1995/Fiance 
#  * @Date: 2018-09-07 10:40:15 
#  * @Last Modified by: PCChris
#  * @Last Modified time: 2018-09-07 10:55:40
#  */

from stock_util import get_all_codes
from database import finance_report_collection, daily_collection
from pymongo import DESCENDING, UpdateOne
from finance_report_crawler import finance_xiaoxiang
import threading

def pe_computing(codes=None):
    # 从finance_report中取出eps
    if codes is None:
        codes = get_all_codes()

    if isinstance(codes, list) is False:
        codes = [codes]

    for code in codes:
        # 从daily中找出close价格
        daily_cursor = daily_collection.find(
            {'code': code},
            projection={'close': True, 'date': True, '_id': False}
        )
        update_requests = []
        for daily in daily_cursor:
            date = daily['date']
            finance_eps_cursor = finance_report_collection.find_one(
                {'code': code, 'report_date': {'$regex': '\d{4}-12-31'}, 'announced_date': {'$lt': date}},
                projection={'code': True, 'eps': True, "_id": False},
                sort=[('announced_date', DESCENDING)]
            )

            if finance_eps_cursor is None:
                continue
            
            if date < '2008-01-01':
                print('have no date in finance_reprot, code: %s' % code)
                finance_xiaoxiang().crawl_finance_report(code)
                break
            # 计算市盈率
            eps = 0
            if finance_eps_cursor['eps'] != '-':
                eps = finance_eps_cursor['eps']

            if eps != 0:
                update_requests.append(UpdateOne(
                    {'code': code, 'date': date},
                    {'$set': {'pe': round(daily['close'] / eps, 4)}}))
        # 将市盈率更新到mongodb中       
        if len(update_requests) > 0:
            update_result = daily_collection.bulk_write(update_requests, ordered=False)
            print('update pe, code: %s, insert: %s, update: %s' % (code, update_result.upserted_count, update_result.modified_count))
            # print(code, len(update_requests))


def threads_cal_pe(codes=None):
        '''
        多线程计算股票pe

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
            codes = get_all_codes()
        threads = []
        # dates = get_trading_dates()
        for code in codes:
            t = threading.Thread(target=pe_computing, args=(code, ))
            threads.append(t)
        return codes, threads


if __name__ == '__main__':
    # find_pe_from_mongodb()
    codes, threads = threads_cal_pe()
    for i in range(len(codes)):
        threads[i].start()
    for i in range(len(codes)):
        threads[i].join()