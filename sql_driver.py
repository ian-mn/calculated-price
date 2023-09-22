from contextlib import contextmanager
from datetime import datetime

import pandas as pd
import pyodbc


class SQLDriver:
    def __init__(self, db, server="DWH-VDI", log=True):
        self.db = db
        self.server = server
        self.log = log

    @contextmanager
    def conn_context(self):
        conn = pyodbc.connect(
            f"Driver=SQL Server;Server={self.server};Database={self.db};Trusted_Connection=yes;SERVER=SQLSERVER2017;"
        )
        yield conn
        conn.close()

    def __log_msg(self, msg):
        if self.log:
            print(msg)

    def create_table(self, schema, table, cols):
        with self.conn_context() as c:
            curs = c.cursor()
            query_str = self.__create_table_query(schema, table, cols)
            curs.execute(query_str)
            curs.commit()

    def __create_table_query(self, schema, table, cols):
        query_str = f"if not exists (select * from sysobjects where name='{table}' and xtype='U') CREATE TABLE {schema}.{table} ("
        for col, type in cols.items():
            query_str += col + " " + type + ","
        query_str = query_str[:-1]
        query_str += ")"
        return query_str

    def drop_table(self, schema, table):
        with self.conn_context() as c:
            query_str = self.__drop_table_query(schema, table)
            curs = c.cursor()
            curs.execute(query_str)
            curs.commit()

    def truncate(self, schema, table):
        with self.conn_context() as c:
            curs = c.cursor()
            curs.execute(f"TRUNCATE TABLE {schema}.{table}")
            curs.commit()

    def __replace_params(self, query, params={}):
        for key, value in params.items():
            query = query.replace(key, value)
        return query

    def read_file(self, path, params={}):
        start_time = datetime.now()
        with open(path, "r") as f:
            query = f.read()
            query = self.__replace_params(query, params)

        self.__log_msg(f"{path} loading...")
        df = self.read(query)

        end_time = datetime.now()
        elapsed_time = end_time - start_time
        self.__log_msg(f"{path} loaded in {elapsed_time}")
        if self.log:
            df.info(memory_usage="deep")
        return df

    def read(self, query_str):
        with self.conn_context() as c:
            return pd.read_sql(query_str, c)

    def read_table(self, table):
        with self.conn_context() as c:
            return pd.read_sql("SELECT * FROM " + table, c)

    def bulk_insert(self, schema, table, df):
        with self.conn_context() as c:
            query_str = self.__insert_query(schema, table, df)
            curs = c.cursor()
            curs.fast_executemany = True
            curs.executemany(query_str, df.values.tolist())
            curs.commit()

    def __insert_query(self, schema, table, df):
        query_str = f"INSERT INTO {schema}.{table} VALUES ("
        for _ in df.columns:
            query_str += "?,"
        query_str = query_str[0:-1]
        query_str += ")"
        return query_str

    def get_agg(self, agg_func, schema, table, column):
        return self.read(
            f"""
            select {agg_func}({column}) from {schema}.{table}
            """
        ).values[0][0]
