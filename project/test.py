# /*
#  * @Author: PCChris: https://github.com/PCChris1995/Fiance 
#  * @Date: 2018-09-07 10:21:57 
#  * @Last Modified by: PCChris
#  * @Last Modified time: 2018-09-07 10:22:20
#  */

# console.log('aa')
from backtest import compute_annual_profit, compute_annual_profit_test

if __name__ == '__main__':
    annual_profit = compute_annual_profit(trading_days=245, net_value=1.2)
    annual_profit_test = compute_annual_profit_test(trading_days=245, net_value=1.2)
    print(annual_profit, annual_profit_test)