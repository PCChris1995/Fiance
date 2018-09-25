# /*
#  * @Author: PCChris: https://github.com/PCChris1995/Fiance 
#  * @Date: 2018-09-25 10:15:50 
#  * @Last Modified by:   PCChris 
#  * @Last Modified time: 2018-09-25 10:15:50 
#  */
# 

'''
分析公司的基本面情况，得到盈利能力，营运能力，偿债能力，投资回报以及现金流等方面的相关指标
'''

from finance_report_crawler import finance_wangyi


def get_finance_factors(code, option, season):
    finance_wangyi_class = finance_wangyi()
    df_income = finance_wangyi_class.financial_indicators_crawler(codes=code, option='income')
    df_balance = finance_wangyi_class.financial_indicators_crawler(codes=code, option='balance')
    df_income = df_income[code].set_index('报告日期')
    df_balance = df_balance[code].set_index('报告日期')
    profit_analysis(df_income, season)
    operation_analysis(df_balance, season)


def profit_analysis(df_income, season):
    if season == 'years': 
        data = dict()  
        for date in df_income.index:
            if date[-5:] != '12-31':
                continue
            gross_profit_margin = cal_gross_profit_margin(date, float(df_income.loc[date]['营业总收入(万元)']), 
                                                            float(df_income.loc[date]['营业总成本(万元)']))
            net_profit_rate = cal_net_profit_rate(date, float(df_income.loc[date]['营业总收入(万元)']), 
                                                    float(df_income.loc[date]['净利润(万元)']))
            # account_rvb_turnover = cal_account_receivable_turnover(date, float())                         
            data[date] = {
                'gross_profit_rate': gross_profit_margin,
                'net_profit_rate': net_profit_rate
            }
    elif season == 'half_years': 
        data = dict()  
        for date in df_income.index:
            if date[-5:] != '06-30':
                continue
            gross_profit_margin = cal_gross_profit_margin(date, float(df_income.loc[date]['营业总收入(万元)']), 
                                                            float(df_income.loc[date]['营业总成本(万元)']))
            net_profit_rate = cal_net_profit_rate(date, float(df_income.loc[date]['营业总收入(万元)']), 
                                                    float(df_income.loc[date]['净利润(万元)']))
            # account_rvb_turnover = cal_account_receivable_turnover(date, float())                         
            data[date] = {
                'gross_profit_rate': gross_profit_margin,
                'net_profit_rate': net_profit_rate
            }
    elif season == 'newly': 
        data = dict()  
        count = 1
        for date in df_income.index:
            # if date[-5:] != '12-31':
            #     continue
            gross_profit_margin = cal_gross_profit_margin(date, float(df_income.loc[date]['营业总收入(万元)']), 
                                                            float(df_income.loc[date]['营业总成本(万元)']))
            net_profit_rate = cal_net_profit_rate(date, float(df_income.loc[date]['营业总收入(万元)']), 
                                                    float(df_income.loc[date]['净利润(万元)']))
            # account_rvb_turnover = cal_account_receivable_turnover(date, float())                         
            data[date] = {
                'gross_profit_rate': gross_profit_margin,
                'net_profit_rate': net_profit_rate
            }
            count += 1
            if count > 13:
                break
    
    print(data)


def cal_gross_profit_margin(date, main_bus_income, main_bus_cost):
    gross_profit_margin = (main_bus_income - main_bus_cost) / main_bus_income * 100
    print('毛利率: %4.2f, date: %s' % (gross_profit_margin, date))
    return round(gross_profit_margin, 2)


def cal_net_profit_rate(date, net_profit, main_bus_income):
    net_profit_rate = (net_profit / main_bus_income) 
    print('净利润率： %4.2f, date: %s' % (net_profit_rate, date))
    return round(net_profit_rate, 2)


def operation_analysis(df_income, season):
    pass
    

if __name__ == '__main__':
    # finance_wangyi = finance_wangyi()
    # df_income = finance_wangyi.financial_indicators_crawler(codes='600048', option='income')
    # print(df_income['600048'].set_index('报告日期'))

    get_finance_factors(code='600048', option='income', season='years')