# /*
#  * @Author: PCChris: https://github.com/PCChris1995/Fiance 
#  * @Date: 2018-09-08 11:21:48 
#  * @Last Modified by: PCChris
#  * @Last Modified time: 2018-09-08 11:22:45
#  */

"""
实现股票池，条件是0 < PE <30， 按照PE正序排列，最多取100只票；
再平衡周期为7个交易日
主要的方法包括：
stock_pool：找到两个日期之间所有出的票
find_out_stocks：找到当前被调出的股票
evaluate_stock_pool：对股票池的性能做初步验证，确保股票池能够带来Alpha
"""

from stock_util import get_trading_dates
from pymongo import ASCENDING
from database import daily_collection
import pandas as pd 
import matplotlib.pyplot as plt
from datetime import datetime


def stock_pool(begin_date, end_date):
    
    adjust_date_codes_dict = dict()
    # 调整周期
    adjust_interval = 7
    all_adjust_dates = []
    last_phase_codes = []

    # 从数据库中挑选出0<pe<30的股票
    all_dates = get_trading_dates(begin_date=begin_date, end_date=end_date)

    for _index in range(0, len(all_dates), adjust_interval):
        adjust_date = all_dates[_index]
        all_adjust_dates.append(adjust_date)
        print('adjust date: %s' % adjust_date)
        daily_cursor = daily_collection.find(
            {'date': adjust_date, 'pe': {'$gt': 0, '$lt': 30}, 'is_trading': True, 'index': False},
            projection={'code': True, '_id': False},
            sort=[('pe', ASCENDING)],
            limit=100
        )

        codes = [x['code'] for x in daily_cursor]
        if codes == []:
            continue
        this_phase_codes = []

        # 判断是否在调整日停牌
        supension_codes = []
        if len(last_phase_codes) > 0:
            supension_cursor = daily_collection.find(
                {'code': {'$in': last_phase_codes}, 'date': adjust_date, 'is_trading': False},
                projection={'code': True}
            ).hint([('code', 1), ('date', -1)])
            supension_codes = [x['code'] for x in supension_cursor]
            this_phase_codes = supension_codes

        # 判断是否在上一期股票池中
        print('last phase code supended in this adjust day:')
        print(supension_codes)

        # 得到这一期的股票池 
        this_phase_codes += codes[0:100-len(this_phase_codes)]
        last_phase_codes = this_phase_codes
        # 建立该调整日和股票列表的对应关系
        adjust_date_codes_dict[adjust_date] = this_phase_codes

        print('this phase codes:')
        print(this_phase_codes)

    return all_adjust_dates, adjust_date_codes_dict


def evaluate_stock_pool(begin_date, end_date):
    '''
    评价股票池的的alpha值，基准选为沪深300
    '''
    # 得到每一期的股票池信息
    adjust_dates, codes_dict = stock_pool(begin_date, end_date)
    # 得到第一组和最后一组的日期
    first_phase_date = adjust_dates[0]
    last_phase_date = adjust_dates[-1]
    # 计算沪深300的收益率
    hs300_begin_value = daily_collection.find_one({'code': '000300', 'index': True, 'date': first_phase_date})['close']

    df_profit = pd.DataFrame(columns=['profit', 'hs300'])
    df_profit.loc[datetime.strptime(adjust_dates[0], '%Y-%m-%d')] = {'profit': 1, 'hs300': 1}
    
    # 计算每一期相对于上一期的收益率
    net_value = 1
    for _index in range(1, len(adjust_dates) - 1):
        last_adjust_date = adjust_dates[_index - 1]
        current_adjust_date = adjust_dates[_index]
        # 上一期的股票代码
        last_phase_codes = codes_dict[last_adjust_date]
        buy_cursor = daily_collection.find(
            {'code': {'$in': last_phase_codes}, 'date': last_adjust_date, 'index': False},
            projection={'code': True, 'close': True, '_id': False}
        ).hint([('code', 1), ('date', -1)])

        code_buy_close_dict = dict()
        for buy_daily in buy_cursor:
            code = buy_daily['code']
            code_buy_close_dict[code] = buy_daily['close']
        
        sell_cursor = daily_collection.find(
            {'code': {'$in': last_phase_codes}, 'date': current_adjust_date, 'index': False},
            projection={'code': True, 'close': True, '_id': False}
        ).hint([('code', 1), ('date', -1)])

        profit_sum = 0
        count = 0
        for sell_daily in sell_cursor:
            _code = sell_daily['code']
            if _code in code_buy_close_dict:
                buy_close = code_buy_close_dict[_code]
                sell_close = sell_daily['close']

                profit_sum += (sell_close - buy_close) / buy_close
                count += 1

        if count > 0:
            profit = round(profit_sum / count, 4)
            hs300_current_value = daily_collection.find_one({'code': '000300', 'index': True, 'date': current_adjust_date})['close']

            # 计算净值和累计收益
            net_value = net_value * (1 + profit)
            dt_current_adjust_date = datetime.strptime(current_adjust_date, '%Y-%m-%d')
            df_profit.loc[dt_current_adjust_date] = {
                'profit': round((net_value), 4),
                'hs300': round((hs300_current_value - hs300_begin_value) / hs300_begin_value + 1, 4)}

    # 绘制图片
    df_profit.plot(title='Stock Pool Evaluation Result', grid=True, kind='line')
    plt.show()


def find_out_stocks(this_phase_codes, last_phase_codes):
    '''
    找到需要跳出的股票code

    parameter：
    this_phase_codes: 这一期的股票list
    last_phase_codes: 上一期的股票list

    retrun:
    out_codes: 需要跳出的股票list
    '''
    out_codes = []
    for last_phase_code in last_phase_codes:
        if last_phase_code not in this_phase_codes:
            out_codes.append(last_phase_code)
    return out_codes

if __name__ == '__main__':
    # all_adjust_dates, adjust_date_codes_dict = stock_pool(begin_date='2018-01-01', end_date='2018-09-01')
    evaluate_stock_pool('2018-01-01', '2018-09-01')
    # print('aa')