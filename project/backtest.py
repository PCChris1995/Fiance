# /*
#  * @Author: PCChris: https://github.com/PCChris1995/Fiance 
#  * @Date: 2018-09-10 11:13:15 
#  * @Last Modified by:   PCChris 
#  * @Last Modified time: 2018-09-10 11:13:15 
#  */
import pandas as pd 
from stock_util import get_trading_dates
from stock_pool_strategy import stock_pool, find_out_stocks
from database import daily_collection, daily_hfq_collection
from datetime import datetime
import matplotlib.pyplot as plt
from trading_strategy import is_k_down_break_m10, is_k_up_break_ma10


def backtest(begin_date, end_date):
    '''
    回测系统
    '''
    # 设置初始值
    cash = 1E7
    single_positon = 2E5
    df_profit = pd.DataFrame(columns=['net_value', 'profit', 'hs300'])
    # 得到回测日期
    all_dates = get_trading_dates(begin_date, end_date)
    adjust_dates, date_codes_dict = stock_pool(begin_date, end_date)
    hs300_begin_value = daily_collection.find_one(
        {'code': '000300', 'index': True, 'date': adjust_dates[0]},
        projection={'close': True, '_id': False})['close']

    holding_code_dict = dict()
    last_date = None
    this_phase_codes = None
    last_phase_codes = None
    to_be_bought_codes = set()
    to_be_sold_codes = set()

    for _date in all_dates:
        print('Back test begin at: %s' % _date)
        before_sell_holding_codes = list(holding_code_dict.keys())

        # 对于每一个回测日期处理复权
        if last_date is not None and len(before_sell_holding_codes) > 0:
            # produce_au(before_sell_holding_codes)
            last_daily_cursor = daily_collection.find(
                {'code': {'$in': before_sell_holding_codes}, 'date': last_date},
                projection={'code': True, '_id': False, 'au_factor': True}).hint([('code', 1), ('date', 1)])

            code_last_aufactor_dict = dict()
            for last_daily in last_daily_cursor:
                code_last_aufactor_dict[last_daily['code']] = last_daily['au_factor']

            current_daily_cursor = daily_collection.find(
                {'code': {'$in': before_sell_holding_codes}, 'date': _date},
                projection={'code': True, '_id': False, 'au_factor': True}).hint([('code', 1), ('date', 1)])

            for current_daily in current_daily_cursor:
                current_aufactor = current_daily['au_factor']
                code = current_daily['code']
                before_volume = holding_code_dict[code]['volume']
                if code in code_last_aufactor_dict:
                    last_aufactor = code_last_aufactor_dict[code]
                    after_volume = int(before_volume * (current_aufactor / last_aufactor))
                    holding_code_dict[code]['volume'] = after_volume
                    print('hold volume adjust: code: %s, %6d, %10.6f, %6d, %10.6f' %
                          (code, before_volume, last_aufactor, after_volume, current_aufactor))

        # 卖出上一期持仓股
        # print('to sell stocks: %s' % to_be_sold_codes, flush=True)
        if len(to_be_sold_codes) > 0:
            sell_daily_cursor = daily_collection.find(
                {'code': {'$in': list(to_be_sold_codes)}, 'date': _date, 'index': True, 'is_trading': True},
                projection={'open': True, 'code': True, '_id': False}
            ).hint([('code', 1), ('date', -1)])

            for sell_daily in sell_daily_cursor:
                sell_code = sell_daily['code']
                if sell_code in before_sell_holding_codes:
                    holding_stock = before_sell_holding_codes[code]
                    sell_price = sell_daily['open']
                    holding_volume = holding_stock['volume']
                    sell_amount = holding_volume * sell_price
                    cash += sell_amount

                    cost = holding_stock['cost']
                    single_profit = (sell_amount - cost) * 100 / cost
                    print('sell: %s, %6d, %6.2f, %8.2f, %4.2f' % (code, holding_volume, sell_price, sell_amount, single_profit))
                    del holding_code_dict[code]
                    to_be_sold_codes.remove(code)
        print('cash after sell: %10.2f' % cash)

        # 买入这一期股票
        # print('to buy stocks: ', to_be_bought_codes, flush=True)
        if len(to_be_bought_codes) > 0:
            buy_daily_cursor = daily_collection.find(
                {'code': {'$in': list(to_be_bought_codes)}, 'date': _date, 'index': False, 'is_trading': True},
                projection={'code': True, '_id': False, 'open': True}
            ).hint([('code', 1), ('date', -1)])

            for buy_daily in buy_daily_cursor:
                if cash > single_positon:
                    code = buy_daily['code']
                    buy_price = buy_daily['open']
                    buy_volume = int(int(single_positon / buy_price) / 100) * 100
                    buy_amount = buy_price * buy_volume
                    cash -= buy_amount
                    holding_code_dict[code] = {
                        'volume': buy_volume,
                        'last_value': buy_amount,
                        'cost': buy_amount
                    }
                    print('buy %s, %6d, %6.2f, %8.2f' % (code, buy_volume, buy_price,  buy_amount))
    
        print('cash after buy: %10.2f' % cash)

        # 计算收益率
        holding_codes = list(holding_code_dict.keys())

        if _date in adjust_dates:
            print('stock pool adjust date: %s' % _date)

            if this_phase_codes is not None:
                last_phase_codes = this_phase_codes
            this_phase_codes = date_codes_dict[_date]
            # print(this_phase_codes, flush=True)

            if last_phase_codes is not None:
                out_codes = find_out_stocks(last_phase_codes, this_phase_codes)
                for out_code in out_codes:
                    if out_code in holding_code_dict:
                        to_be_sold_codes.add(out_code)

        for holding_code in holding_codes:
            if is_k_down_break_m10(holding_code, _date):
                to_be_sold_codes.add(holding_code)
                
        to_be_bought_codes.clear()
        if this_phase_codes is not None:
            for _code in this_phase_codes:
                if _code not in holding_codes and is_k_up_break_ma10(_code, _date):
                    to_be_bought_codes.add(_code)

        # 计算持仓股票市值
        total_value = 0
        holding_daily_cursor = daily_collection.find(
            {'code': {'$in': holding_codes}, 'date': _date, 'index': False},
            projection={'code': True, '_id': False, 'close': True}
        ).hint([('code', 1), ('date', -1)])

        for holding_daily in holding_daily_cursor:
            code = holding_daily['code']
            holding_stock = holding_code_dict[code] 
            value = holding_daily['close'] * holding_stock['volume']
            total_value += value

            profit = (value - holding_stock['cost']) * 100 / holding_stock['cost']
            one_day_profit = (value - holding_stock['last_value']) * 100 / holding_stock['last_value']

            holding_stock['last_value'] = value 
            # print('holding stocks: %s, %10.2f, %4.2f, %4.2f' % 
            #         (code, value, profit, one_day_profit))
        # 计算总资产
        total_capital = total_value + cash

        # 计算基准收益
        hs300_current_value = daily_collection.find_one(
            {'code': '000300', 'date': _date, 'index': True},
            projection={'code': True, 'close': True, '_id': False}
        )['close']
        print('after close, cash: %10.2f, total_capital: %10.2f' % 
                (cash, total_capital))
        dt_date = datetime.strptime(_date, '%Y-%m-%d')
        df_profit.loc[dt_date] = {
            'net_value': round(total_capital / 1e7, 2),
            'profit': round(100 * (total_capital - 1e7) / 1e7, 2),
            'hs300': round(100 * (hs300_current_value - hs300_begin_value) / hs300_begin_value, 2)
        }
    drawdown = compute_drawdown(df_profit['net_value'])
    annual_profit, sharpe_ratio = compute_sharpe_ratio(df_profit['net_value'])
    print('Backtest result: %s - %s,  annual_profit: %7.3f, maxdrawdown：%7.3f, sharpe ratio: %4.2f' %
      (begin_date, end_date, annual_profit, drawdown, sharpe_ratio))

    df_profit.plot(title='Backtest Result', y=['profit', 'hs300'], kind='line')
    plt.show()


def compute_drawdown(net_values):
    '''
    得到最大回测

    parameter：
    net_values: 净值
    '''
    max_drawdown = 0
    index = 0
    for net_value in net_values:
        for sub_net_value in net_values[index:]:
            drawdown = 1 - sub_net_value / net_value
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        index += 1
    return max_drawdown
    

def produce_au():
    '''
    处理复权
    '''
    pass


def sell():
    '''
    卖出股票
    '''
    pass


def buy(code, to_be_bought_codes, cash):
    '''
    买入股票
    '''
    print('to buy stocks: ', to_be_bought_codes, flush=True)
    if len(to_be_bought_codes) > 0:
        buy_daily_cursor = daily_collection.find(
            {'code': {'$in': to_be_bought_codes}, 'date': _date, 'index': False, 'is_trading': False},
            projection={'code': True, '_id': False, 'open': True}
        ).hint([('code', 1), ('date', -1)])

        for buy_daily in buy_daily_cursor:
            if cash > single_position:
                buy_price = buy_daily['open']
                buy_volume = int(single_positon / buy_price)
                buy_amount = buy_price * buy_volume
                cash -= buy_amount
                holding_code_dict[code] = {
                    'volume': buy_volume,
                    'last_value': buy_price,
                    'cost': buy_amount
                }
                print('buy %s, %6d, %6.2f, %8.2f' % (code, buy_volume, buy_amount))
    print('cash after buy: %10.2f' % cash)
    return holding_code_dict, cash 


def compute_annual_profit(trading_days, net_value):
    '''
    计算年化收益率
    '''
    annual_profit = 0
    if trading_days > 0:
        years = trading_days / 245
        annual_profit = pow(net_value, 1/years) -1
    annual_profit = round(annual_profit, 2)
    return annual_profit


'''def compute_annual_profit_test(trading_days, net_value):
    init_net_profit = 1
    annual_profit = (net_value - init_net_profit) / init_net_profit * 245 / trading_days
    return round(annual_profit, 2)'''


def compute_sharpe_ratio(net_profit):
    '''
    计算夏普比例
    parameter:
    net_profit: 净值
    '''
    profit_df = pd.DataFrame(columns={'profit'})
    profit_df.loc[0] = {'profit': round((net_profit[0] - 1) * 100, 2)}
    for i in range(1, len(net_profit)):
        profit = (net_profit[i] - net_profit[i-1]) / net_profit[i-1]
        profit = round(profit * 100, 2)
        profit_df.loc[i] = {'profit': profit}
    trading_days = len(net_profit)
    annual_profit = compute_annual_profit(trading_days, net_profit[-1])
    profit_std = pow(profit_df.var()['profit'], 1/2)
    sharpe_ratio = (annual_profit - 4.75) / (profit_std*pow(245, 1/2))
    return annual_profit, sharpe_ratio


if __name__ == '__main__':
    backtest('2018-01-20', '2018-09-01')