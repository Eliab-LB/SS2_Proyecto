#!/usr/bin/python3
#Las librerias necesarias
# import pyodbc
import itertools
import threading
import pandas as pd
import time
import sys
import os 
from path_config.definitions import ROOT_DIR
from imprimir import *
from tabulate import tabulate
from mysql.statements import * 
from sqlserver.statements import * 

import config
import logging

#Configurar nuestro log
logger = logging.getLogger('Seminario_2_Proyecto_Grupo_E')
logger.setLevel(logging.DEBUG)
ch = logging.FileHandler('logs.log')
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

#Configurar nuestra conexion
MYSQL = StatementsMySQL()
SQL = StatementsSQL()

CONNECTION_STRING_SQLServer = f"SERVER={config.sql_server};UID={config.sql_user};PWD={config.sql_password};DATABASE={config.sql_database}"
CONNECTION_STRING_MYSQL=f"SERVER={config.mysql_server};UID={config.mysql_user};PWD={config.mysql_password};DATABASE={config.mysql_database}"    
logger.info("Iniciando nuestra aplicacion")

logger.info(" CONNECTION_STRING_SQLServer: ".center(80, "-"))
before, after = CONNECTION_STRING_SQLServer.split("PWD=")
logger.info(before + f"PWD=<{len(after)} characters>")

logger.info(" CONNECTION_STRING_MYSQL: ".center(80, "-"))
mysql_before, mysql_after = CONNECTION_STRING_MYSQL.split("PWD=")
logger.info(mysql_before + f"PWD=<{len(mysql_after)} characters>")

done=False

def main():
    menu()

def menu():
    while True:
        print_main_menu()
        opcion = input('Seleccione una opcion: ')
        if opcion=='1':
            logger.info("Usuario selecciona opcion de creacion")
            proceso = threading.Thread(name='creacion', target=creacion)
            proceso.start()
            while proceso.is_alive():
                animate("creando tablas temporales ")
        elif opcion=='2':
            logger.info('Usuario selecciona opcion de llenar modelo')
            cargando_informacion = threading.Thread(name='cargando_info', target=ejecutar_llenado)
            cargando_informacion.start()
            while cargando_informacion.is_alive():
                animate("cargando información")
        elif opcion=='3':
            logger.info('Usuario selecciona opcion de ejecutar consultas')
            consultas = threading.Thread(name='consultas', target=ejecutar_consultas)
            consultas.start()
            while consultas.is_alive:
                animate("Ejecutando consultas")
        else:
            conn.close()
            logger.info('Conexion finalizada')
            exit()

def animate(message):
    chars = "/—\|" 
    for char in chars:
        sys.stdout.write('\r'+ message + '...'+char+'\r')
        time.sleep(.1)
        sys.stdout.flush() 
def llenar_MySQL():
    MYSQL.fill_model_region()
    logger.info("region - completado -  MySQL")
    MYSQL.fill_model_sub_region()
    logger.info("sub_region - completado - MySQL")
    MYSQL.fill_model_fecha()
    logger.info("fecha - completado - MySQL")
    MYSQL.fill_model_dimension()
    logger.info("dimension - completado - MySQL")
    MYSQL.fill_model_periodicidad()
    logger.info("periodicidad - completado - MySQL")
    MYSQL.fill_model_pais()
    logger.info("pais - completado - MySQL") 

def llenar_SQL_server():
    SQL.fill_model_region()
    logger.info("region - completado -  SQLServer")
    SQL.fill_model_sub_region()
    logger.info("sub_region - completado - SQLServer")
    SQL.fill_model_fecha()
    logger.info("fecha - completado - SQLServer")
    SQL.fill_model_dimension()
    logger.info("dimension - completado - SQLServer")
    SQL.fill_model_periodicidad()
    logger.info("periodicidad - completado - SQLServer")
    SQL.fill_model_pais()
    logger.info("pais - completado - SQLServer") 

def ejecutar_llenado():
    # print('a implementar')
    logger.info("Llenando modelo MySQL")
    llenar_MySQL()
    logger.info("MySQL - Completado")

    # logger.info("Llenando modelo SQLServer")
    # llenar_SQL_server()
    # logger.info("SQLServer - Completado")

def ejecutar_consultas():
    print('a implementar ')

def creacion():
    logger.info("Eliminando tablas...")
    MYSQL.delete_temporal_table()
    SQL.delete_temporal_table()
    logger.info("Tablas eliminadas correctamente")
    logger.info("Creando tabla temporal PIB_PERCAPITA (crecimiento anual) ")
    MYSQL.build_temporal_table()
    SQL.build_temporal_table()
    logger.info("Creando Modelo - MySQL ")
    MYSQL.build_model()
    logger.info("Creando Modelo - SQLServer ")
    SQL.build_model()
    logger.info("Comenzando a procesar el dataset")
    try:
        iso_country_code=pd.read_csv(os.path.join(ROOT_DIR,'dataset','ISO-3166Countries-with-Regional-Codes.csv'))
        df_iso=pd.DataFrame(iso_country_code)
        df_iso=df_iso.fillna(0)
        iso_queries_to_run=transform_data_iso(df_iso)
        logger.info("Dataset ISO-3166 leido exitosamente")
        logger.info("Cargando tabla temporal ISO-3166 - mysql")
        MYSQL.load_temporal_data(iso_queries_to_run)
        logger.info("Cargando tabla temporal ISO-3166 - SQL_Server")
        SQL.load_temporal_data(iso_queries_to_run)

        data = pd.read_csv(os.path.join(ROOT_DIR,'dataset','PIB_PERCAPITA.csv'))
        # print(data)
        df = pd.DataFrame(data)
        df = df.fillna(0)
        logger.info("Dataset leido exitosamente (crecimiento pib)")
        queries_to_run = transform_data(df, table_name="temporal")
        logger.info("Cargando tabla temporal pib - mysql")
        MYSQL.load_temporal_data(queries_to_run)
        logger.info("Cargando tabla temporal pib - SQL_Server")
        SQL.load_temporal_data(queries_to_run)
        

        data_inflacion=pd.read_csv(os.path.join(ROOT_DIR,'dataset','PIB_Inflacion.csv'))
        df_inflacion = pd.DataFrame(data_inflacion)
        df_inflacion = df_inflacion.fillna(0)
        logger.info("Dataset leido exitosamente (inflación)")
        queries_to_run_inflacion=transform_data(df_inflacion, table_name="temporal_inflacion")
        logger.info('Cargando tabla temporal inflacion - mysql')
        MYSQL.load_temporal_data(queries_to_run_inflacion)
        logger.info("Cargando tabla temporal inflacion - SQL_Server")
        SQL.load_temporal_data(queries_to_run_inflacion)
        logger.info("Tablas temporales cargadas a bases de datos")
        done=True

    except Exception as e:
        logger.error(e)
        conn.close()
        done=True
        exit()

def transform_data_iso(df):
    to_run = list()
    i = 0
    for row in df.itertuples():
        if(i==0):
            i = i +1
            continue
        country_name=row[1]
        country_name = country_name.replace("'","''")
        alpha3=row[3]
        country_code=(0 if row[4] == 0 else row[4])
        # ('NA' if row[3] == 0 else row[3])
        region=row[6]
        sub_region=row[7]
        region_code=(0 if row[9] == 0 else row[9])
        sub_region_code=(0 if row[10] == 0 else row[10])
        query = (f'INSERT INTO temporalISO3166 VALUES(\'{country_name}\',\'{alpha3}\',{country_code},\'{region}\',{region_code},\'{sub_region}\',{sub_region_code})')
        to_run.append(query)

        logger.info(query)
        i=i+1
    logger.info(f'Generados {i} inserts')
    return to_run

def transform_data(df,table_name):
    to_run = list()
    i = 0
    for row in df.itertuples():
        if(i==0):
            i = i +1
            continue
        country_name = ('NA' if row[3] == 0 else row[3])
        country_code = ('NA' if row[4] == 0 else row[4])
        country_name = country_name.replace("'","''")
        if(country_name == 'NA' and country_code == 'NA'):
            print('fin del archivo')
            break
        years={}

        for x in range(5,28):
            years[x] = (0 if row[x] == '..'else round(float(row[x]),4))

        query = (f'INSERT INTO {table_name} VALUES(\'{row[1]}\',\'{country_name}\',\'{country_code}\',{years[5]},{years[6]},{years[7]},{years[8]},{years[9]},{years[10]},{years[11]},{years[12]},{years[13]},{years[14]},{years[15]},{years[16]},{years[17]},{years[18]},{years[19]},{years[20]},{years[21]},{years[22]},{years[23]},{years[24]},{years[25]},{years[26]},{years[27]})')
        to_run.append(query)

        logger.info(query)
        i=i+1
    logger.info(f'Generados {i} inserts')
    return to_run

def cargar_temporal_inflacion(df):
    cursor = conn.cursor()
    i = 0
    for row in df.itertuples():
        if(i==0):
            i = i +1
            continue
        country_name = ('NA' if row[3] == 0 else row[3])
        country_code = ('NA' if row[4] == 0 else row[4])
        country_name = country_name.replace("'","''")
        if(country_name == 'NA' and country_code == 'NA'):
            print('row 0 = vacio')
            break
        years={}

        for x in range(5,28):
            years[x] = (0 if row[x] == '..'else round(float(row[x]),4))

        query = (f'INSERT INTO temporal VALUES(\'{country_name}\',\'{country_code}\',{years[5]},{years[6]},{years[7]},{years[8]},{years[9]},{years[10]},{years[11]},{years[12]},{years[13]},{years[14]},{years[15]},{years[16]},{years[17]},{years[18]},{years[19]},{years[20]},{years[21]},{years[22]},{years[23]},{years[24]},{years[25]},{years[26]},{years[27]})')
        print('query:',query)
        logger.info(query)
        cursor.execute(query)
        i=i+1
    conn.commit()
    logger.info(f'Se insertaron correctamente {i} filas')

if __name__ == "__main__":
    main()
    logger.info('Aplicacion finalizada')