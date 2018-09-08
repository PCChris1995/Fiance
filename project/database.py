from pymongo import MongoClient

DB_CONN = MongoClient('mongodb://127.0.0.1:27017')['pc_quant']

daily_collection = DB_CONN['daily_none']
daily_hfq_collection = DB_CONN['daily_hfq']
finance_report_collection = DB_CONN['finance_report']