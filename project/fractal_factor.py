#  -*- coding: utf-8 -*-

from database import DB_CONN
from stock_util import get_all_codes
from pymongo import ASCENDING, UpdateOne
from pandas import DataFrame
import traceback
import threads


def compute_fractal(begin_date=None, end_date=None, codes=None):
    """
    计算指定日期内的信号
    :param begin_date: 开始日期
    :param end_date: 结束日期
    """
    if codes is None:
        codes = get_all_codes()
    if begin_date is None:
        begin_date = '2008-01-01'
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if isinstance(codes, list) is False:
        codes = [codes]

    for code in codes:
        try:
            # 获取后复权的价格，使用后复权的价格计算分型
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'high': True, 'low': True, '_id': False}
            ).hint([('code', 1), ('date', -1)])

            df_daily = DataFrame([daily for daily in daily_cursor])

            df_daily.set_index(['date'], 1, inplace=True)

            df_daily_left_shift_1 = df_daily.shift(-1)
            df_daily_left_shift_2 = df_daily.shift(-2)
            df_daily_right_shift_1 = df_daily.shift(1)
            df_daily_right_shift_2 = df_daily.shift(2)

            df_daily['up'] = (df_daily['high'] > df_daily_left_shift_1['high']) & \
                             (df_daily['high'] > df_daily_left_shift_2['high']) & \
                             (df_daily['high'] > df_daily_right_shift_1['high']) & \
                             (df_daily['high'] > df_daily_right_shift_2['high'])

            df_daily['down'] = (df_daily['low'] < df_daily_left_shift_1['low']) & \
                               (df_daily['low'] < df_daily_left_shift_2['low']) & \
                               (df_daily['low'] < df_daily_right_shift_1['low']) & \
                               (df_daily['low'] < df_daily_right_shift_2['low'])

            df_daily = df_daily[(df_daily['up'] | df_daily['down'])]

            # 保存结果到数据库
            df_daily.drop(['high', 'low'], 1, inplace=True)

            print(df_daily)
            # 将信号保存到数据库
            update_requests = []
            for index in df_daily.index:
                doc = {
                    'code': code,
                    'date': index,
                    # 方向，向上突破 up，向下突破 down
                    'direction': 'up' if df_daily.loc[index]['up'] else 'down'
                }
                update_requests.append(
                    UpdateOne(doc, {'$set': doc}, upsert=True))

            if len(update_requests) > 0:
                DB_CONN['fractal_signal'].create_index([("code", 1), ("date", -1)], background=True)
                update_result = DB_CONN['fractal_signal'].bulk_write(update_requests, ordered=False)
                print('%s, upserted: %4d, modified: %4d' %
                      (code, update_result.upserted_count, update_result.modified_count),
                      flush=True)
        except:
            print('错误发生： %s' % code, flush=True)
            traceback.print_exc()

def is_fractal_up(code, date):
    count = DB_CONN['fractal_signal'].count({'code': code, 'date': date, 'signal': 'up'})
    return count == 1

def is_fractal_down(code, date):
    count = DB_CONN['fractal_signal'].count({'code': code, 'date': date, 'signal': 'down'})
    return count == 1


if __name__ == '__main__':
    compute_fractal('1990-01-01', '2018-06-30')

