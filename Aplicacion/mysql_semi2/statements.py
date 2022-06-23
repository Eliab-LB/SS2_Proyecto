from ast import Try
from typing import final
import pymysql
import config
from pymysql.constants import CLIENT 
from mysql_semi2.queries import *
conn = pymysql.connect(host=config.mysql_server,user=config.mysql_user,passwd=config.mysql_password,db=config.mysql_database,client_flag=CLIENT.MULTI_STATEMENTS)

class StatementsMySQL(object):
    def __init__(self):
        pass

    def delete_temporal_table(self):
        try:
            cursor = conn.cursor()
            cursor.execute(DROP_TABLES)
        except Exception as e:
            conn.close()
            raise e from Exception
        # finally:
            # conn.close()

    def build_temporal_table(self):
        try:
            cursor = conn.cursor()
            cursor.execute(TEMPORAL_CREATION)
            cursor.execute(COLLATE)
        except Exception as e:
            conn.close()
            raise e from Exception
        # finally:
            # conn.close()

    def build_model(self):
        try:
            cursor = conn.cursor()
            cursor.execute(CREATE_MODEL)
            conn.commit()
        except Exception as e:
            conn.close()
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
            conn.close()
            raise e from Exception
        # finally: 
            # conn.close()
    def fill_model_region(self):
        try:
            cursor = conn.cursor()
            cursor.execute(REGION)
            conn.commit()
        except Exception as e:
            conn.close()
            raise e from Exception
        # finally:
            # conn.close()
    def fill_model_sub_region(self):
        try:
            cursor = conn.cursor()
            cursor.execute(SUB_REGION)
            conn.commit()
        except Exception as e:
            conn.close()
            raise e from Exception
        # finally:
            # conn.close()
    def fill_model_fecha(self):
        try:
            cursor = conn.cursor()
            cursor.execute(FECHA)
            conn.commit()
        except Exception as e:
            conn.close()
            raise e from Exception
        # finally:
            # conn.close()
    def fill_model_dimension(self):
        try:
            cursor = conn.cursor()
            cursor.execute(DIMENSION)
            conn.commit()
        except Exception as e:
            conn.close()
            raise e from Exception
        # finally:
            # conn.close()
    def fill_model_periodicidad(self):
        try:
            cursor = conn.cursor()
            cursor.execute(PERIODICIDAD)
            conn.commit()
        except Exception as e:
            conn.close()
            raise e from Exception
        # finally:
            # conn.close()
    def fill_model_pais(self):
        try:
            cursor = conn.cursor()
            cursor.execute(PAIS)
            conn.commit()
        except Exception as e:
            conn.close()
            raise e from Exception
        # finally:
            # conn.close()
    def delete_model(self):
        try:
            cursor = conn.cursor()
            cursor.execute(CLEAN_MODEL)
            conn.commit()
        except Exception as e:
            conn.close()
            raise e from Exception
