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
            conn.close()
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
            conn.close()
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
            conn.close()
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
            conn.close()
            raise e from Exception

    def fill_model_region(self):
        try:
            cursor = conn.cursor()
            cursor.execute(REGION)
            conn.commit()
            # cursor.execute(COLLATE)
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
            conn.close()
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
            conn.close()
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
            conn.close()
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
    
    def fill_data_marts(self):
        try:
            cursor = conn.cursor()
            cursor.execute(DATAMART_PIB)
            cursor.execute(DATAMART_COMBINADO)
            cursor.execute(DATAMART_INFLACION)
            conn.commit()
        except Exception as e:
            conn.close()
            raise e from Exception

    def fill_model_reporte(self):
        try:
            cursor = conn.cursor()
            REPORTE = """INSERT INTO reporte 
                SELECT ti.[{0}] as 'Inflacion',  t.[{0}] as 'PIB', p.id as 'Codigo pais', '1' as 'Periodicidad', '1' as Dimension, '{1}' as 'Anio'  
                FROM temporal t, temporal_inflacion ti, pais p 
                WHERE t.country_name = ti.country_name AND ti.country_name = p.nombre;"""

            for x in range(23):
                if x == 0:
                    cursor.execute(REPORTE.format('1990', '1'))
                    conn.commit()
                else:
                    anio = 1999 + x
                    anio_codigo = x + 1
                    cursor.execute(REPORTE.format(str(anio), str(anio_codigo)))
                    conn.commit()
            # cursor.executemany(FILL_REPORTE)
            conn.commit()
        except Exception as e:
            conn.close()
            raise e from Exception

    def execute_query(self,query):
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor
        except Exception as e:
            conn.close()
            raise e from Exception