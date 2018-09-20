#  -*- coding: utf-8 -*-
from stock_util import get_trading_dates, get_all_codes
import threading
# from daily_fix import fill_is_trading_between
import tushare as ts 
# from test import test_fun, class_test


def threads_dates(begin_date, end_date, fun):
    '''
    date 多线程函数
    有几个date就有几个线程
    fun:需要多线程的函数

    使用说明：
    使用时在main中加入如下代码：
    dates, threads = threads_dates()
    for i in range(len(dates)):
        threads[i].start()
    for i in range(len(dates)):
        threads[i].join()
    '''
    # codes = get_all_codes()
    dates = get_trading_dates(begin_date, end_date)
    threads = []
    # dates = get_trading_dates()
    for date in dates:
        t = threading.Thread(target=fun, args=(None, None, date))
        threads.append(t)
    length = len(dates)
    return length, threads


def threads_codes(fun):
    '''
    code多线程
    code有多少个就有多少个线程
    fun: 需要多线程的函数
    parm: fun的参数，如： （code, none, none）
    使用说明：
    使用时在main中加入如下代码：
    time_start = time.now()
    length, threads = threads_codes()
    for i in range(length):
        threads[i].start()
    for i in range(length):
        threads[i].join()
    time_end = time.now()
    print(time_start - time_end)
    '''
    # codes = get_all_codes()
    codes = list(ts.get_stock_basics().index)
    threads = []
    # dates = get_trading_dates()
    for code in codes:
        t = threading.Thread(target=fun, args=(None, None, code))
        threads.append(t)
    length = len(codes)
    return length, threads


if __name__ == '__main__': 
    test_fun()
    class_test().fun_test()