from ast import Try
from typing import final
import pymssql
import config
from sqlserver.queries import * 
conn = pymssql.connect(host=config.sql_server, user=config.sql_user, password=config.sql_password, database=config.sql_database)
#conexion a sql server
class StatementsSQL(object):
    def __init__(self):
        pass
    def delete_temporal_table(self):
        try:
            cursor = conn.cursor()
            cursor.execute(DROP_TABLES)
            conn.commit()
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()

    def build_temporal_table(self):
        try:
            cursor = conn.cursor()
            cursor.execute(TEMPORAL_CREATION)
            conn.commit()
            # cursor.execute(COLLATE)
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()
    
    def load_temporal_data(self,df):
        try:
            cursor = conn.cursor()
            for row in df:
                cursor.execute(row)
            conn.commit()
        except Exception as e:
            raise e from Exception
