# /*
#  * @Author: PCChris: https://github.com/PCChris1995/Fiance 
#  * @Date: 2018-09-14 10:56:39 
#  * @Last Modified by:   PCChris 
#  * @Last Modified time: 2018-09-14 10:56:39 
#  */

from database import DB_CONN
from stock_util import get_all_codes
from datetime import datetime, timedelta
from pandas import DataFrame
import time 
from pymongo import ASCENDING, UpdateOne,DESCENDING
import matplotlib.pyplot as plt 
import numpy as np 
import math 
from threads import threads_codes


def compute_ris(begin_date=None, end_date=None, codes=None):
    '''
    计算RSI的值并把RSI数值保存到数据库中

    '''
    
    if codes is None:
        codes = get_all_codes()
    if begin_date is None:
        begin_date = '2008-01-01'
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if isinstance(codes, list) is False:
        codes = [codes]

    N = 12
    for code in codes:

        try:
                # 获取后复权的价格，使用后复权的价格计算RSI
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id': False}
            ).hint([('code', 1), ('date', -1)])

            df_daily = DataFrame([daily for daily in daily_cursor])

            if df_daily.index.size < N:
                print('data is not enough: %s' % code, flush=True)
                continue

            df_daily.set_index(['date'], 1, inplace=True)
            df_daily['pre_close'] = df_daily['close'].shift(1)
            df_daily['change_pct'] = (df_daily['close'] - df_daily['pre_close']) * 100 / df_daily['pre_close']
            df_daily['up_pct'] = DataFrame({'up_pct': df_daily['change_pct'], 'zero': 0}).max(1)
            df_daily['RSI'] = df_daily['up_pct'].rolling(N).mean() / abs(df_daily['change_pct']).rolling(N).mean() * 100
            df_daily['PREV_RSI'] = df_daily['RSI'].shift(1)
            df_daily.drop(['pre_close', 'change_pct', 'up_pct', 'close'], axis=1, inplace=True)

            # df_daily['up'] = 80
            # df_daily['down'] = 20
            # df_daily.plot(kind='line', title='RSI', y=['RSI', 'up', 'down'])
            # plt.show()

            # 将数据保存到mongodb中
            update_requests = []
            for date in df_daily.index:
                update_requests.append(UpdateOne(
                    {'code': code, 'date': date},
                    {'$set':{'code': code, 'date': date, 'RSI': df_daily.loc[date]['RSI']}},
                    upsert=True))
            if len(update_requests) > 0:
                DB_CONN['RSI'].create_index([("code", 1), ("date", -1)], background=True)
                update_result = DB_CONN['RSI'].bulk_write(update_requests, ordered=True)
                print('Save RSI data, code: %s, insert: %4d, update: %4d' % 
                        (code, update_result.upserted_count, update_result.modified_count), flush=True)
        except:
            print('ERROR happend %s' % code, flush=True)


def RSI_is_over_bought(code, date):
    '''
    判断RSI是否是超买
    return：
    Ture/False
    '''
    date_before_one_day = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    RSI_cursor = DB_CONN['RSI'].find(
        {'code': code, 'date': {'$lte': date}},
        sort=[('date', DESCENDING)],
        limit=2,
        projection={'date': True, 'RSI': True, '_id': False})

    # df_daily = DataFrame(daily for daily in RSI_cursor)
    # df_daily.set_index('date', inplace=True)
    df_daily = dict([(x['date'], x['RSI']) for x in RSI_cursor])
    if math.isnan(df_daily[date]) or math.isnan(df_daily[date_before_one_day]):
        print('RSI data is None, code: %s, date: %s' % (code, date))
        return False

    df_daily_over_bought = (df_daily[date] < 80) & (df_daily[date_before_one_day] >= 80)
    # df_daily_over_sold = (df_daily['RSI'] > 20) & (df_daily['RSI'].shift(1) <= 20)
    return df_daily_over_bought


def RSI_is_over_sold(date, code):
    '''
    判断RSI是否是超卖
    return：
    Ture/False
    '''
    date_before_one_day = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d')

    RSI_cursor = DB_CONN['RSI'].find(
        {'code': code, 'date': {'$lte': date}},
        sort=[('date', DESCENDING)],
        limit=2,
        projection={'date': True, 'RSI': True, '_id': False})

    # df_daily = DataFrame(daily for daily in R
    df_daily = dict([(x['date'], x['RSI']) for x in RSI_cursor])

    if math.isnan(df_daily[date]) or math.isnan(df_daily[date_before_one_day]):
        print('RSI data is None, code: %s, date: %s' % (code, date))
        return False

    # df_daily_over_bought = (df_daily['RSI'] < 80) & (df_daily['RSI'].shift(1) >= 80)
    df_daily_over_sold = (df_daily[date] > 80) & (df_daily[date_before_one_day] <= 80)
    return df_daily_over_sold


if __name__ == '__main__': 
    # compute_ris(begin_date='2018-01-01', end_date='2018-09-01')
    # IS = RSI_is_over_bought(code='000001', date='2018-01-25')
    # print(IS)

    time_start = datetime.now()
    length, threads = threads_codes(compute_ris)
    for i in range(length):
        threads[i].start()
    for i in range(length):
        threads[i].join()
    time_end = datetime.now()
    print(time_start - time_end)