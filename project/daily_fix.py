#-*- coding:UTF-8 -*-

from database import DB_CONN
from pymongo import ASCENDING, UpdateOne
from datetime import datetime, timedelta
from stock_util import get_trading_dates, get_all_codes
import os
from daily_scrawler import DailyCrawler
'''
修复采集到的日线数据
'''


def fill_is_trading_between(begin_date=None, end_date=None):
    '''
    填充指定时间段内的is_trading字段

    parameter:
    begin_date: 开始日期
    end_date: 结束日期
    '''

    # 得到时间段
    all_trading_dates = get_trading_dates(begin_date, end_date)
    # 填充单个数据
    for trading_date in all_trading_dates:
        fill_single_date_is_trading(trading_date, 'daily')
        fill_single_date_is_trading(trading_date, 'daily_hfq')


def fill_single_date_is_trading(date=None, collection='daily_hfq'):
    '''
    填充指定日期的is_trading字段

    date: 指定日期
    collection: 数据集
    '''
    # 得到日线数据
    daily_cursor = DB_CONN[collection].find(
        {'date': date},
        projection={'code': True, '_id': False, 'volume': True},
        batch_size=1000
    )
    # 判断是否为交易日
    update_requests = []
    for daily in daily_cursor:
        is_trading = True
        if daily['volume'] == 0:
            is_trading = False
        # 将is_trading 填入
        update_requests.append(
            UpdateOne(
                {'code': daily['code'], 'date': date},
                {'$set': {'is_trading': is_trading}})
        )
        # 将填充好的数据保存到数据库
        if len(update_requests) > 0:
            update_result = DB_CONN[collection].bulk_write(update_requests, ordered=False)
            print('fill daily data,   feature: is_trading,   code: %s,   date: %s,  collection: %s,     insert: %4d, update: %4d' % 
            (daily['code'], date, collection, update_result.upserted_count, update_result.modified_count))


def fill_daily_k_supension_days(begin_date=None, end_date=None):
    '''
    补充股票在停牌日的数据

    parameter：
    begin_date: 开始日期，为None时即从‘2008-01-01’开始
    end_date: 结束日期， 为None时为数据库中能找到的最新日期
    '''
    # 找到指定时间段所有的交易日期
    all_dates = get_trading_dates(begin_date, end_date)

    # 找到所有股票的上市日期
    basic_date = datetime.now().strftime('%Y-%m-%d')
    while True:
        basic_cursor = DB_CONN['basic'].find(
            {'date': basic_date},
            projection={'code': True, 'date': True, 'timeToMarket': True, '_id': False},
            ).hint([('date', -1)])
        basics = [basic for basic in basic_cursor]
        if len(basics) > 0:
            break
        basic_date = (datetime.strptime(basic_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')

    for date in all_dates:
        for basic in basics:
            code = basic['code']
            # 判断股票是否在当前交易日期停牌
            is_supension_flag = is_supension(date, code)
            # 对停牌日期补充数据
            if is_supension_flag:
                doc_daily = fill_daily_k_supension_days_at_date_one_collection(date, code, basic, 'daily_none')
                doc_daily_hfq = fill_daily_k_supension_days_at_date_one_collection(date, code, basic, 'daily_hfq')
            # 将补充的数据更新到数据库中


def is_supension(date, code):
    '''
    判断股票是否停牌
    date: 日期
    code: 股票code
    '''
    daily = DB_CONN['daily_none'].find_one({'code': code, 'date': date})

    if daily is None:
        return True
    else:
        return False
    

def fill_daily_k_supension_days_at_date_one_collection(date, code, basic, collection):
    '''
    补充指定股票指定日期的停牌数据

    parameter：
    date: 单个日期
    code: 单只股票代码
    basic: 股票的上市日期数据
    collection: 数据集
    '''
    if date < basic['timeToMarket']:
        print('have not IPO,    code: %s,   date: %s,   timeToMarket: %s' %
            (code, date, basic['timeToMarket']))

    else:
        before_date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        date_supension = [date]
        while True:
            # 获取前一天的交易数据
            data_before = DB_CONN[collection].find_one({'code': code, 'date': before_date},
                                projection={'code': True, 'date': True, 'close': True, '_id': False})
            if data_before is not None:
                break
            date_supension.append(before_date)
            before_date = (datetime.strptime(before_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
            if before_date < basic['timeToMarket']:
                print('have no daily date in dbs, code: %s' % code)
                DailyCrawler().crawl_stocks(codes=code, start_date=basic['timeToMarket'])
                return 

        update_requests = []
        for date in date_supension:
            doc_supension = {
                'code': code,
                'date': date,
                'close': data_before['close'],
                'open': data_before['close'],
                'high': data_before['close'],
                'low': data_before['close'],
                'volume': 0,
                'is_trading': False
            }

            update_requests.append(
                UpdateOne(
                    {'code': code, 'date': date},
                    {'$set': doc_supension},
                    upsert=True))

        if len(update_requests) > 0:
            update_result = DB_CONN[collection].bulk_write(update_requests, ordered=False)
            print('fill supension data,  collection: %12s,   date: %s,   code: %s,   insert: %4d,   update: %4d' %
                     (collection, date, code, update_result.upserted_count, update_result.modified_count), flush=True)


def fill_au_factor_pre_close(begin_date=None, end_date=None):
    """
    为daily数据集填充：
    1. 复权因子au_factor，复权的因子计算方式：au_factor = hfq_close/close
    2. pre_close = close(-1) * au_factor(-1)/au_factor
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """
    all_codes = get_all_codes()

    if begin_date is None:
        begin_date = '2008-01-01'
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    for code in all_codes:
        hfq_daily_cursor = DB_CONN['daily_hfq'].find(
            {'code': code, 'date': {'$lte': end_date, '$gte': begin_date}, 'index': False},
            sort=[('date', ASCENDING)],
            projection={'date': True, 'close': True}).hint([('code', 1), ('date', -1)])

        date_hfq_close_dict = dict([(x['date'], x['close']) for x in hfq_daily_cursor])

        daily_cursor = DB_CONN['daily_none'].find(
            {'code': code, 'date': {'$lte': end_date, '$gte': begin_date}, 'index': False},
            sort=[('date', ASCENDING)],
            projection={'date': True, 'close': True}
        ).hint([('code', 1), ('date', -1)])

        last_close = -1
        last_au_factor = -1

        update_requests = []
        for daily in daily_cursor:
            date = daily['date']
            try:
                close = daily['close']

                doc = dict()

                au_factor = round(date_hfq_close_dict[date] / close, 2)
                doc['au_factor'] = au_factor
                if last_close != -1 and last_au_factor != -1:
                    pre_close = last_close * last_au_factor / au_factor
                    doc['pre_close'] = round(pre_close, 2)

                last_au_factor = au_factor
                last_close = close

                update_requests.append(
                    UpdateOne(
                        {'code': code, 'date': date, 'index': False},
                        {'$set': doc}))
            except:
                print('ERROR happen when calculate au_factor，code：%s，date：%s' % (code, date), flush=True)
                # 恢复成初始值，防止用错
                last_close = -1
                last_au_factor = -1

        if len(update_requests) > 0:
            update_result = DB_CONN['daily_none'].bulk_write(update_requests, ordered=False)
            print('fill au_factor and pre_close, code: %s, update: %4d, insert: %s' %
                  (code, update_result.modified_count, update_result.upserted_count), flush=True)


if __name__ == '__main__':
    # fill_is_trading_between()   
    # fill_daily_k_supension_days(begin_date='2011-02-01')
    fill_au_factor_pre_close()