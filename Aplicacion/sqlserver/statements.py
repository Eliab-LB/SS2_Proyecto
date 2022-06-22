from ast import Try
from typing import final
import pymssql
import config
from sqlserver.queries import *
conn = pymssql.connect(host=config.sql_server, user=config.sql_user,password=config.sql_password, database=config.sql_database)
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

    def build_model(self):
        try:
            cursor = conn.cursor()
            cursor.execute(CREATE_MODEL)
            conn.commit()
            # cursor.execute(COLLATE)
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()

    def load_temporal_data(self, df):
        try:
            cursor = conn.cursor()
            for row in df:
                cursor.execute(row)
            conn.commit()
        except Exception as e:
            raise e from Exception

    def fill_model_region(self):
        try:
            cursor = conn.cursor()
            cursor.execute(REGION)
            conn.commit()
            # cursor.execute(COLLATE)
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()

    def fill_model_sub_region(self):
        try:
            cursor = conn.cursor()
            cursor.execute(SUB_REGION)
            conn.commit()
            # cursor.execute(COLLATE)
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()

    def fill_model_fecha(self):
        try:
            cursor = conn.cursor()
            cursor.execute(FECHA)
            conn.commit()
            # cursor.execute(COLLATE)
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()

    def fill_model_dimension(self):
        try:
            cursor = conn.cursor()
            cursor.execute(DIMENSION)
            conn.commit()
            # cursor.execute(COLLATE)
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()

    def fill_model_periodicidad(self):
        try:
            cursor = conn.cursor()
            cursor.execute(PERIODICIDAD)
            conn.commit()
            # cursor.execute(COLLATE)
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()

    def fill_model_pais(self):
        try:
            cursor = conn.cursor()
            cursor.execute(PAIS)
            conn.commit()
            # cursor.execute(COLLATE)
        except Exception as e:
            raise e from Exception
        # finally:
            # conn.close()
