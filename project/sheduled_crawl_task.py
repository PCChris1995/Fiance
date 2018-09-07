'''/*
 * @Author: PCChris: https://github.com/PCChris1995/Fiance 
 * @Date: 2018-09-07 09:35:36 
 * @Last Modified by: PCChris
 * @Last Modified time: 2018-09-07 10:16:01
 */'''

from daily_scrawler import DailyCrawler
import schedule
import time 
from datetime import datetime
from finance_report_crawler import finance_xiaoxiang


def schedule_crawl_tasker():
    daily_crawl = DailyCrawler()
    basic_crawl = finance_xiaoxiang()
    now_date = datetime.now()
    weekday = int(now_date.strftime('%w'))

    if 0 < weekday < 6:
        now = now_date.strftime('%Y-%m-%d')
        daily_crawl.crawl_index(start_date=now)
        daily_crawl.crawl_stocks(start_date=now)
        basic_crawl.crawl_finance_report()

if __name__ == '__main__':
    schedule.every().day.at("17:08").do(schedule_crawl_tasker)
    while True:
        schedule.run_pending()
        # print('wait 10 seconds...')
        time.sleep(10)
    # schedule_crawl_tasker()