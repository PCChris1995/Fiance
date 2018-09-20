/*
 * @Author: PCChris: https://github.com/PCChris1995/Fiance 
 * @Date: 2018-09-07 10:24:34 
 * @Last Modified by: PCChris
 * @Last Modified time: 2018-09-18 11:48:11
 */


### 2018-9-1 
创建一个projects, 复现数据下载和存储功能
`daily_scrawler.py` 从tushare上下载数据并保存到mongodb中
`database.py:`       创建mongodb连接
`basic_crawler.py`： 爬去股票的基本数据
`stock_util.py` 目前只是得到任意时间段的股票交易日期

**股票数据爬取基本完成，接下来是一些财务数据的爬取：如市盈率，财务报表数据**

### 2018-09-02
完成了基本的财务数据下载，但是还没有完成三张财务报表所有内容的下载，下一步进行
`financial.py`: 从CSDN上找到的基本的爬取财务数据的代码
`finance_report_crawler.py`: 爬取eps财务数据，后续加上爬取三张财务报表的的程序

### 2018-09-03
完成三张财务报表数据的下载
`finance_report_crawler.py`:爬取pes和三张财务报表数据
`finance_util.py`: 以后将所有和财务数据处理有关的代码都放在这里面

### 2018-09-06
完成了数据的修复功能的代码
`daily_fix.py`: 完成数据的修复功能，并加上复权因子的计算
`scheduled_crawl_task.py `:增加定时入库功能
将`daily_scrawler.py`增加多线程功能，数据下载时间缩短到两个小时以内
`pe_computing`：计算pe,并存入数据库中

### 2018-09-07
增加挑选股票池函数
`stock_pool_strategy.py`

### 2018-09-10
`threads.py`:增加多线程函数，实现数据爬取，修复的多线程功能

### 2018-09-11
`backtest.py`增加回测函数

### 2018-09-12
`trading_strategy.py`: 增加策略函数，目前是策略和10日均线有关

### 2018-09-18
`strategy_ma10.py`：将10日均线策略改名
#### 增加技术指标文件:
`rsi_factor.py`
`macd_factor.py`
`boll_factor.py`
`fractal_factor.py`


### 2018-09-18
将文件进行整理，将策略统一放到stragegy文件夹中
`pingan_ma5.py`： 编写了一个简单策略和聚宽回测的结果进行比较，发现买入时期都是吻合的，但是由于没有设置手续费和直接用收盘价去买入，所以造成了结果有一定的差别
下一步，设置手续费和滑点，并改进回测系统