# /*
#  * @Author: PCChris: https://github.com/PCChris1995/Fiance 
#  * @Date: 2018-09-20 10:27:09 
#  * @Last Modified by:   PCChris 
#  * @Last Modified time: 2018-09-20 10:27:09 
#  */
#
import matplotlib.pyplot as plt 
from pandas import DataFrame
from pymongo import ASCENDING
from datetime import datetime
import traceback
import os
import sys
sys.path.append(os.getcwd())
from database import daily_collection


def pingan_ma5(begin_date=None, end_date=None, code=None):
    if code is None:
        code = '000001'
    CASH = 100000
    daily_cursor = daily_collection.find(
        {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 'index': False},
        projection={'date': True, 'close': True, '_id': False},
        sort=[('date', ASCENDING)],
    ).hint([('code', 1), ('date', -1)])

    df_daily = DataFrame([daily for daily in daily_cursor])
    df_daily.set_index('date', inplace=True)
    # print(df_daily)
    df_daily['ma5'] = round(df_daily['close'].rolling(5).mean())

    df_daily['last_close'] = df_daily['close'].shift(1)

    df_daily['buy_signal'] = df_daily['close'] > 1.01*df_daily['ma5']
    df_daily['sell_signal'] = df_daily['close'] < df_daily['ma5']

    df_daily = df_daily[df_daily['buy_signal'] | df_daily['sell_signal']]

    record_stock_dict = dict()
    df_capital = dict()
    rest_cash = CASH
    hold_stock_dict = dict()
    hold_date = set()
    capital = CASH

    for date in df_daily.index:
        price = df_daily.loc[date]['close']
        if df_daily.loc[date]['buy_signal']:
            # price = df_daily.loc[date]['close']
            amount = rest_cash / price // 100 * 100
            if amount > 0:
                rest_cash = rest_cash - price * amount
                hold_stock_dict[date] = {
                    'price': price,
                    'amount': amount
                }
                capital = amount * price + rest_cash
                hold_date.add(date)
                print('buy,  date: %s, price: %5.2f, amount: %8.2f, rest_cash: %10.2f, capital: %10.2f' 
                        % (date, price, amount, rest_cash, capital))

        if df_daily.loc[date]['sell_signal']:
            try:
                if len(hold_date) > 0:
                    amount = 0
                    for _date in hold_date:
                        amount += hold_stock_dict[_date]['amount']
                    # amount = CASH / price // 100 * 100
                    rest_cash = rest_cash + price * amount
                    capital = rest_cash
                    print('sell, date: %s, price: %5.2f, amount: %8.2f, rest_cash: %10.2f, capital: %10.2f' 
                            % (date, price, amount, rest_cash, capital))
                    del hold_stock_dict[_date]   
                    hold_date.remove(_date)
            except:
                traceback.print_exc()


        dt_date = datetime.strptime(date, '%Y-%m-%d')
        holding_amount = 0
        if len(hold_date) > 0:
            for _date in hold_date:
                holding_amount += hold_stock_dict[_date]['amount']
            # holding_amount = hold_stock_dict[date]['amount']
            df_capital[dt_date] = rest_cash + price * holding_amount
        else:
            df_capital[dt_date] = rest_cash
            
    df_capital = DataFrame.from_dict(df_capital, orient='index', columns=['capital'])
    df_capital['profit'] = round((df_capital['capital'] - CASH) / CASH, 2)
    print(df_capital)
    df_capital.plot(title='Backtest Result', y=['profit'], kind='line')
    plt.grid()
    plt.show()


'''def buy_or_sell(close, ma5):
    if close > 1.01*ma5:
        return 1
    elif close < ma5:
        return -1
    else:
        return 0'''
        

if __name__ == '__main__': 
    pingan_ma5(begin_date='2018-01-01', end_date='2018-09-01')