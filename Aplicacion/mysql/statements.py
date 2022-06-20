from ast import Try
from typing import final
import pymysql
import config
from pymysql.constants import CLIENT 
from mysql.queries import *
conn = pymysql.connect(host=config.mysql_server,user=config.mysql_user,passwd=config.mysql_password,db=config.mysql_database,client_flag=CLIENT.MULTI_STATEMENTS)

class StatementsMySQL(object):
    def __init__(self):
        pass

    def delete_temporal_table(self):
        try:
            cursor = conn.cursor()
            cursor.execute(DROP_TABLES)
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()

    def build_temporal_table(self):
        try:
            cursor = conn.cursor()
            cursor.execute(TEMPORAL_CREATION)
            cursor.execute(COLLATE)
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()
    
    def load_temporal_pib(self,df):
        try:
            cursor = conn.cursor()
            for row in df:
                cursor.execute(row)
            conn.commit()
        except Exception as e:
            raise e from Exception
        # finally: 
            # conn.close()