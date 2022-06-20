#!/usr/bin/python3
#Las librerias necesarias
# import pyodbc
import itertools
import threading
import pandas as pd
import time
import sys

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

def ejecutar_llenado():
    print('a implementar')
def ejecutar_consultas():
    print('a implementar ')

def creacion():
    logger.info("Eliminando tablas...")
    MYSQL.delete_temporal_table()
    SQL.delete_temporal_table()
    logger.info("Tablas eliminadas correctamente")
    logger.info("Creando las tablas necesarias")
    logger.info("Creando tabla temporal")
    MYSQL.build_temporal_table()
    SQL.build_temporal_table()
    logger.info("Comenzando a procesar el dataset")
    try:
        print('leyendo csv')
        data = pd.read_csv("PIB_PERCAPITA.csv")
        # print(data)
        df = pd.DataFrame(data)
        df = df.fillna(0)
        logger.info("Dataset leido exitosamente")
        queries_to_run = transform_data(df)
        logger.info("Cargando tabla temporal pib - mysql")
        MYSQL.load_temporal_pib(queries_to_run)
        logger.info("Cargando tabla temporal pib - SQL_Server")
        SQL.load_temporal_pib(queries_to_run)
        logger.info("Tablas temporales cargadas a bases de datos")
        done=True
    except Exception as e:
        logger.error(e)
        conn.close()
        done=True
        exit()

def transform_data(df):
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
            print('row 0 = vacio')
            break
        years={}

        for x in range(5,28):
            years[x] = (0 if row[x] == '..'else round(float(row[x]),4))

        query = (f'INSERT INTO temporal VALUES(\'{country_name}\',\'{country_code}\',{years[5]},{years[6]},{years[7]},{years[8]},{years[9]},{years[10]},{years[11]},{years[12]},{years[13]},{years[14]},{years[15]},{years[16]},{years[17]},{years[18]},{years[19]},{years[20]},{years[21]},{years[22]},{years[23]},{years[24]},{years[25]},{years[26]},{years[27]})')
        print('query:',query)
        to_run.append(query)

        logger.info(query)
        # cursor.execute(query)
        i=i+1
    # conn.commit()
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