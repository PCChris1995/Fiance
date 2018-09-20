# /*
#  * @Author: PCChris: https://github.com/PCChris1995/Fiance 
#  * @Date: 2018-09-13 21:24:53 
#  * @Last Modified by:   PCChris 
#  * @Last Modified time: 2018-09-13 21:24:53 
#  */
'''
计算macd值并把它保存到数据库中
'''
from database import daily_hfq_collection, DB_CONN
from stock_util import get_all_codes
from datetime import timedelta, datetime
from pymongo import ASCENDING, UpdateOne
from pandas import DataFrame 
import matplotlib.pyplot as plt 
import traceback
from threads import threads_codes
import time 

def macd_compute(begin_date=None, end_date=None, codes=None):
    
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
            daily_hfq_cursor = daily_hfq_collection.find(
                {'code': code, 'date': {'$lte': end_date, '$gte': begin_date}, 'index': False},
                sort=[('date', ASCENDING)],
                projection={'date': True, 'close': True, '_id': False},
            ).hint([('code', 1), ('date', -1)])

            df_daily = DataFrame(daily for daily in daily_hfq_cursor)
            # for i in range(len(df_daily['date'])):
            #     df_daily['date'][i] = datetime.strptime(df_daily['date'][i], '%Y-%m-%d')
            df_daily.set_index(['date'], inplace=True)

            EMA1 = []
            EMA2 = []
            N1 = 12
            N2 = 26
            index = 0

            for date in df_daily.index:
                if index == 0:
                    EMA1.append(df_daily.loc[date]['close'])
                    EMA2.append(df_daily.loc[date]['close'])
                else:
                    EMA1.append(2/(N1+1) * (df_daily.loc[date]['close'] - EMA1[-1]) + EMA1[-1])
                    EMA2.append(2/(N2+1) * (df_daily.loc[date]['close'] - EMA2[-1]) + EMA2[-1])
                index += 1

            df_daily['EMA1'] = EMA1
            df_daily['EMA2'] = EMA2

            df_daily['DIFF'] = df_daily['EMA1'] - df_daily['EMA2']

            index = 0
            DEA = []
            M = 9
            for date in df_daily.index:
                if index == 0:
                    DEA.append(df_daily.loc[date]['DIFF'])
                else:
                    DEA.append(2/(M+1) * (df_daily.loc[date]['DIFF'] - DEA[-1]) + DEA[-1])
                index += 1
            df_daily['DEA'] = DEA

            df_daily['delta'] = df_daily['DIFF'] - df_daily['DEA']
            df_daily['pre_delta'] = df_daily['delta'].shift(1)

            df_daily_gold = df_daily[(df_daily['delta'] > 0) & (df_daily['pre_delta'] <= 0)]
            df_daily_dead = df_daily[(df_daily['delta'] < 0) & (df_daily['pre_delta'] >= 0)]

            # print(df_daily_gold)
            # df_daily.plot(kind='line', title='macd', y=['DIFF', 'DEA'])
            # plt.show()

            updata_requests = []
            for date in df_daily_gold.index:
                updata_requests.append(
                    UpdateOne(
                        {'code': code, 'date': date},
                        {'$set':{'code': code, 'date': date, 'signal': 'gold'}},
                        upsert=True
                    )
                )

            for date in df_daily_dead.index:
                updata_requests.append(
                    UpdateOne(
                        {'code': code, 'date': date},
                        {'$set':{'code': code, 'date': date, 'signal': 'dead'}},
                        upsert=True
                    )
                )

            if len(updata_requests) > 0:
                DB_CONN['macd'].create_index([('code', 1), ('date', -1)], background=True)
                requests_result = DB_CONN['macd'].bulk_write(updata_requests, ordered=False)
                print('Save MACD data, code: %s, date: %s, update: %4d, insert: %4d' % 
                        (code, date, requests_result.upserted_count, requests_result.modified_count), flush=True)
        except:
            print('ERROR! code: %s' % code)
            traceback.print_exc()


def is_macd_gold(code, date):
    gold_signal = DB_CONN['macd'].find_one({'code': code, 'date': date},
                                            projection={'signal': True, '_id': False})
    if gold_signal is None:
        print('macd signal gold is None')
        return False
    else:
        if gold_signal['signal'] == 'gold':
            return True
        else:
            return False


def is_macd_dead(code, date):
    dead_signal = DB_CONN['macd'].find_one({'code': code, 'date': date},
                                            projection={'signal': True, '_id': False})
    if dead_signal is None:
        print('macd signal dead is None')
        return False
    else:
        if dead_signal['signal'] == 'dead':
            return True
        else:
            return False


if __name__ == '__main__':
    # macd_compute()
    time_before = time.now()
    length, threads = threads_codes(macd_compute)
    for i in range(length):
        threads[i].start()
    for i in range(length):
        threads[i].join()
    time_after = time.now()
    print(time_after - time_before)
    # is_macd_dead(code='000688', date='2018-08-20')