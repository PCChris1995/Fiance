'''/*
 * @Author: PCChris: https://github.com/PCChris1995/Fiance 
 * @Date: 2018-09-07 09:35:36 
 * @Last Modified by: PCChris
 * @Last Modified time: 2018-09-07 10:22:37
 */'''

#  -*- coding: utf-8 -*-

from pymongo import ASCENDING
from database import DB_CONN
from datetime import datetime, timedelta


def get_trading_dates(begin_date=None, end_date=None):
    '''
    得到指定日期范围内按照正序排列的交易日列表
    如果没有指定日期则从'2008-01-01'至今所有的交易日

    parameter： 
    begin_date: 开始日期
    end_date： 结束日期

    return：
    dates: 总的交易日期
    '''
    # 指定日期，如果没有指定如期则是从现在往前365天
    if begin_date is None:
        begin_date = '2008-01-01'
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    # 从数据库总找到数据
    daily_cursor = DB_CONN.daily_hfq.find(
        {'code': '000001', 'date': {'$gte': begin_date, '$lte': end_date}, 'index': True},
        sort=[('date', ASCENDING)],
        projection={'date': True, '_id': False}
    ).hint([('code', 1), ('date', -1)])
    # 将日期转换为list格式
    dates = [x['date'] for x in daily_cursor]
    return dates


def get_all_codes(date=None):
    '''
    得到数据库中所有股票的code

    parameter: date,指定日期
    return： codes type: list
    '''
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    # 从数据库中取出code
    codes = []
    while len(codes) == 0:
        code_cursor = DB_CONN.basic.find(
            {'date': date}, 
            projection={'code': True, '_id': False}
        ).hint([('date', -1)])
        codes = [x['code'] for x in code_cursor]
        date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    return codes


if __name__ == '__main__':
    dates = get_trading_dates()
    print(dates)
    # codes = get_all_codes()
    # print(len(codes))