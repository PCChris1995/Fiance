# /*
#  * @Author: PCChris: https://github.com/PCChris1995/Fiance 
#  * @Date: 2018-09-18 11:01:41 
#  * @Last Modified by:   PCChris 
#  * @Last Modified time: 2018-09-18 11:01:41 
#  */
from pymongo import UpdateOne, ASCENDING
from database import DB_CONN
from stock_util import get_all_codes
from datetime import datetime
from pandas import DataFrame
import traceback
from threads import threads_codes


def compute_boll(begin_date=None, end_date=None, codes=None):
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
            # 获取后复权的价格，使用后复权的价格计算Boll
            daily_cursor = DB_CONN['daily_hfq'].find(
                {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id': False}
            ).hint([('code', 1), ('date', -1)])

            df_daily = DataFrame([daily for daily in daily_cursor])

            # 计算MB，盘后计算，这里用当日的Close
            df_daily['MB'] = df_daily['close'].rolling(20).mean()
            # 计算STD20
            df_daily['std'] = df_daily['close'].rolling(20).std()
            # 计算UP
            df_daily['UP'] = df_daily['MB'] + 2 * df_daily['std']
            # 计算down
            df_daily['DOWN'] = df_daily['MB'] - 2 * df_daily['std']

            # 将日期作为索引
            df_daily.set_index(['date'], inplace=True)

            # 将close移动一个位置，变为当前索引位置的前收
            last_close = df_daily['close'].shift(1)

            # 突破上轨
            shifted_up = df_daily['UP'].shift(1)
            df_daily['up_mask'] = (last_close <= shifted_up) & (df_daily['close'] > shifted_up)

            # 突破下轨
            shifted_down = df_daily['DOWN'].shift(1)
            df_daily['down_mask'] = (last_close >= shifted_down) & (df_daily['close'] < shifted_down)

            # 过滤结果
            df_daily = df_daily[df_daily['up_mask'] | df_daily['down_mask']]
            df_daily.drop(['close', 'std', 'MB', 'UP', 'DOWN'], 1, inplace=True)

            # 将信号保存到数据库
            update_requests = []
            for index in df_daily.index:
                doc = {
                    'code': code,
                    'date': index,
                    # 方向，向上突破 up，向下突破 down
                    'direction': 'up' if df_daily.loc[index]['up_mask'] else 'down'
                }
                update_requests.append(
                    UpdateOne(doc, {'$set': doc}, upsert=True))

            if len(update_requests) > 0:
                DB_CONN['boll'].create_index([("code", 1), ("date", -1)], background=True)
                update_result = DB_CONN['boll'].bulk_write(update_requests, ordered=False)
                print('%s, upserted: %4d, modified: %4d' %
                      (code, update_result.upserted_count, update_result.modified_count),
                      flush=True)
        except:
            traceback.print_exc()


if __name__ == '__main__':
    # compute_boll(begin_date='2015-01-01', end_date='2018-06-30')
    time_start = datetime.now()
    length, threads = threads_codes(compute_boll)
    for i in range(length):
        threads[i].start()
    for i in range(length):
        threads[i].join()
    time_end = datetime.now()
    print(time_start - time_end)