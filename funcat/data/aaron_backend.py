# -*- coding: utf-8 -*-
#

import pandas as pd
import  psycopg2
#from time import clock
#from Stocks import *
from funcat.data.HData_eastmoney_zlpm import *

from cached_property import cached_property

from .backend import DataBackend
from ..utils import lru_cache, get_str_date_from_int, get_int_date

stocks=HData_eastmoney_zlpm("usr","usr")

dbg = 0

def debug(message):
    import sys
    import inspect
    callerframerecord = inspect.stack()[1]
    frame = callerframerecord[0]
    info = inspect.getframeinfo(frame)
    print(info.filename, 'func=%s' % info.function, 'line=%s:' % info.lineno, message)

class AaronDataBackend(DataBackend):
    #skip_suspended = True
    skip_suspended = False


    @cached_property
    def ts(self):
        try:
            import tushare as ts
            # debug("test")
            return ts
        except ImportError:
            print("-" * 50)
            print(">>> Missing tushare. Please run `pip install tushare`")
            print("-" * 50)
            raise
    @cached_property
    def stock_basics(self):
        #debug("test")
        s_df = stocks.get_data_from_hdata()
        if dbg:
            debug("stock_basics %s" % s_df)
        return s_df;

    @cached_property
    def code_name_map(self):
        code_name_map = self.stock_basics[["stock_name"]].to_dict()["stock_name"]
        if dbg:
            debug("code_name_map %s" % code_name_map)
        return code_name_map

    @lru_cache(maxsize=4096)
    def get_price(self, order_book_id, start, end, freq):
        """
        :param order_book_id: e.g. 000002.XSHE
        :param start: 20160101
        :param end: 20160201
        :param freq: 1m 1d 5m 15m ...
        :returns:
        :rtype: numpy.rec.array
        """
        # debug("test")
        # print("order_book_id:%s" % (order_book_id))

        '''
        if order_book_id[0:1] == '6':
            order_book_id = 'SH' + order_book_id
        else:
            order_book_id = 'SZ' + order_book_id
        '''
        
        start = get_str_date_from_int(start)
        end = get_str_date_from_int(end)
        #print("start=%s, end=%s" % (start, end))
        
        conn = psycopg2.connect(database="usr", user="usr", password="usr", host="127.0.0.1", port="5432")
        cur = conn.cursor()
        db_columns = "record_date , stock_code , open , close , high , low , volume ,\
                amount , percent "

        sql_temp="select " + db_columns +  "from \
                (select " + db_columns + " from eastmoney_d_table where stock_code="\
                +"\'"+order_book_id+"\'  and  \
                record_date between "+"\'"+start+"\' and "+"\'"+end+"\' \
                order by record_date desc\
                ) as tbl order by record_date asc;"
        #sql_temp="select * from hdata_d_table where stock_code="+"\'"+order_book_id+"\';"
        #print("sql_temp=%s"%(sql_temp))
        cur.execute(sql_temp)
        rows = cur.fetchall()
        #print('rows = %s' % rows)

        conn.commit()
        conn.close()


        #dataframe_cols=[tuple[0] for tuple in cur.description]#列名和数据库列一致
        #dataframe_cols=['date', 'open', 'close', 'high', 'low', 'volume', 'code']
        dataframe_cols=['date','code', 'open', 'close', 'high', 'low', 'volume', 'amount', 'percent']
        df = pd.DataFrame(rows, columns=dataframe_cols)
        df["date"]=df["date"].apply(lambda x: str(x))
        df["datetime"] = df["date"].apply(lambda x: int(x.replace("-", "")) * 1000000)
        del df["code"]
        del df["amount"]
        del df['percent']
        arr = df.to_records()


        #print(arr)
        #print(arr[-1])
        
        return arr


    @lru_cache()
    def get_order_book_id_list(self):
        """获取所有的股票代码列表
        """
        info = self.ts.get_stock_basics()
        code_list = info.index.sort_values().tolist()
        # debug("test")
        return code_list
        
    @lru_cache()
    def get_trading_dates(self, start, end):
        """获取所有的交易日

        :param start: 20160101
        :param end: 20160201
        """
        start = get_str_date_from_int(start)
        end = get_str_date_from_int(end)
        df = self.ts.get_k_data("000001", index=True, start=start, end=end)
        trading_dates = [get_int_date(date) for date in df.date.tolist()]
        # debug("test")
        return trading_dates

    @lru_cache(maxsize=4096)
    def symbol(self, order_book_id):
        """获取order_book_id对应的名字
        :param order_book_id str: 股票代码
        :returns: 名字
        :rtype: str
        """
        # debug("test")
        return "{}[{}]".format(order_book_id, self.code_name_map.get(order_book_id))
