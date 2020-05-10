#!/usr/bin/env python  
# -*- coding: utf-8 -*-

import tushare as ts
import psycopg2
import pandas as pd
import numpy as np


token='21dddafc47513ea46b89057b2c4edf7b44882b3e92274b431f199552'
pro = ts.pro_api(token)

class Stocks(object):#这个类表示"股票们"的整体(不是单元)
    def get_stock_basic(self):
        self.stock_basic = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

    def get_codestock_local(self):#从本地获取所有股票代号和名称
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1",
                                port="5432")
        cur = conn.cursor()
        # 创建stocks表
        cur.execute('''
                select * from stocks order by stock_code asc;
               ''')
        rows =cur.fetchall()
        conn.commit()
        conn.close()

        return rows
        pass
    def __init__(self,user,password):
        self.stock_basic=[]
        self.user=user
        self.password=password

    def db_perstock_insertsql(self,stock_code,name, area, industry, list_date):#返回的是插入语句
        sql_temp="insert into stocks values("
        sql_temp+="\'"+stock_code+"\'"+","+"\'"+name+"\'"+","
        sql_temp+="\'"+area+"\'"+","+"\'"+industry+"\'"+","
        sql_temp+="\'"+list_date+"\'"
        sql_temp +=");"
        return sql_temp
        pass

    def db_stocks_update(self):# 根据stock_basic的情况插入原表中没的。。stock_basic中有的源表没的保留不删除#返回新增行数
        ans=0
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1", port="5432")
        cur = conn.cursor()
        self.get_stock_basic()

        for i in range(0,len(self.stock_basic)):
            sql_temp='''select * from stocks where stock_code='''
            sql_temp+="\'"+self.stock_basic["symbol"][i]+"\';"
            cur.execute(sql_temp)
            rows=cur.fetchall()
            if(len(rows)==0):
                #如果股票代码没找到就插
                ans+=1
                cur.execute(self.db_perstock_insertsql(
                    self.stock_basic["symbol"][i],
                    self.stock_basic["name"][i],
                    self.stock_basic["area"][i],
                    self.stock_basic["industry"][i],
                    self.stock_basic["list_date"][i]
                    ))
                pass
        conn.commit()
        conn.close()
        print("db_stocks_update finish")
        return ans

    def db_stocks_create(self):
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1", port="5432")
        cur = conn.cursor()
        # 创建stocks表
        cur.execute('''
            drop table if exists stocks;
            create table stocks(
                stock_code varchar primary key,
                name varchar,
                area varchar,
                industry varchar,
                list_date date
                );
        ''')
        conn.commit()
        conn.close()
        print("db_stocks_create finish")
        pass
        
    def get_all_data(self): 
        conn = psycopg2.connect(database="usr", user=self.user, password=self.password, host="127.0.0.1", port="5432")
        cur = conn.cursor()
        sql_temp="select * from stocks order by stock_code asc;"
        cur.execute(sql_temp)
        rows = cur.fetchall()

        conn.commit()
        conn.close()

        dataframe_cols=[tuple[0] for tuple in cur.description]#列名和数据库列一致
        df = pd.DataFrame(rows, columns=dataframe_cols)
        #index =  df["stock_code"]
        #df = pd.DataFrame(rows, index=index, columns=dataframe_cols)
        df=df.set_index('stock_code')

        return df


