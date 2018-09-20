# /*
#  * @Author: PCChris: https://github.com/PCChris1995/Fiance 
#  * @Date: 2018-09-12 10:07:50 
#  * @Last Modified by:   PCChris 
#  * @Last Modified time: 2018-09-12 10:07:50 
#  */
'''
投资策略函数，10日均线法 
'''
from database import daily_collection, daily_hfq_collection
import pandas as pd 
from pymongo import DESCENDING
import numpy as np


def is_k_up_break_ma10(code, date):
    '''
    查看当日收盘价是否上穿10日均线
    parameter:
    code: 股票代码
    date: 日期
    return: True/False
    '''
    current_daily = daily_collection.find_one(
        {'code': code, 'date': date},
        projection={'code': True, '_id': False, 'close': True, 'is_trading': True}
    )
    if current_daily is None: 
        print('code: %s have no data in date: %s' % (code, date))
        return False

    daily_cursor = daily_collection.find(
        {'code': code, 'date': {'$lte': date}, 'index': False, 'is_trading': True},
        sort=[('date', DESCENDING)],
        limit=11,
        projection={'code': True, 'close': True, '_id': False}
    ).hint([('code', 1), ('date', -1)])
    closes = [daily['close'] for daily in daily_cursor]
    closes.reverse()
    # closes = [daily['close'] for daily in daily_cursor].reverse()
    if closes is None:
        print('data before 10 days of %s is None, code: %s' % (date, code))
        return False
    if len(closes) < 11:
        print('K line is not enough: code %s, date: %s' % (code, date))
        return False

    last_close_2_last_ma10 = compare_close_2_ma10(closes[0:10], current_daily)
    current_close_2_current_ma10 = compare_close_2_ma10(closes[1:], current_daily)
    if last_close_2_last_ma10 == -1 and current_close_2_current_ma10 == 1:
        return True
    else: 
        return False


def is_k_down_break_m10(code, date):
    '''
    查看当日收盘价是否上穿10日均线
    parameter:
    code: 股票代码
    date: 日期
    return: True/False
    '''
    current_daily = daily_collection.find_one(
        {'code': code, 'date': date},
        projection={'code': True, '_id': False, 'close': True, 'is_trading': True}
    )
    if current_daily is None: 
        print('code: %s have no data in date: %s' % (code, date))
        return False

    daily_cursor = daily_collection.find(
        {'code': code, 'date': {'$lte': date}, 'index': False, 'is_trading': True},
        sort=[('date', DESCENDING)],
        limit=11,
        projection={'code': True, 'close': True, 'date': True, '_id': False}
    ).hint([('code', 1), ('date', -1)])

    closes = [daily['close'] for daily in daily_cursor]
    closes.reverse()

    if closes is None:
        print('data before 10 days of %s is None, code: %s' % (date, code))
        return False
    if len(closes) < 11:
        print('K line is not enough: code %s, date: %s' % (code, date))
        return False
        
    last_close_2_last_ma10 = compare_close_2_ma10(closes[0:10], current_daily)
    current_close_2_current_ma10 = compare_close_2_ma10(closes[1:], current_daily)
    if last_close_2_last_ma10 == 1 and current_close_2_current_ma10 == -1:
        return True
    else: 
        return False


def compare_close_2_ma10(closes, current_daily):
    closes = np.array(closes)
    ma10 = round(np.mean(closes))
    current_close = current_daily['close']
    if current_close > ma10:
        return 1
    elif current_close < ma10:
        return -1
    elif current_close == ma10:
        return 0

if __name__ == '__main__':
    is_k_up_break_ma10(code='600048', date='2018-09-03')