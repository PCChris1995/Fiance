class finance_util:
        
    def __init__(self):
        
        self.url_main_financial_factors = 'http://quotes.money.163.com/service/zycwzb_{0}.html?type=report'
        self.url_cashflow = 'http://quotes.money.163.com/service/xjllb_{0}.html'
        self.url_income = 'http://quotes.money.163.com/service/lrb_{0}.html'
        self.url_balance = 'http://quotes.money.163.com/service/zcfzb_{0}.html'

    def tables_option(self, option=None):
            '''
            选择相应的报表

            parameter：
            option: 需要爬取的财务报表名称   'None':爬取主要指标  'income':利润表   'balance': 资产负债表  'cashflow'：现金流量表

            return: url
            '''
            if option == None:
                return self.url_main_financial_factors
            elif option == 'cashflow':
                return self.url_cashflow
            elif option == 'income':
                return self.url_income
            elif option == 'balance':
                return self.url_balance
            else:
                print('没有相应的报表')
                return


if __name__ == '__main__':
    pass