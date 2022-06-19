# Importamos las librerías necesarias
import mysql.connector
import pyodbc
import pandas as pd
import logging

# Configuracion del logger
logger = logging.getLogger('Grupo E')
logger.setLevel(logging.DEBUG)
ch = logging.FileHandler('logs.log')
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

#Configurar nuestra conexion

CONNECTION_STRING = f"DRIVER={{{config.driver}}};SERVER={config.server};UID={config.sql_user};PWD={config.sql_password};DATABASE={config.database}"

logger.info("Iniciando nuestra aplicacion")

logger.info(" CONNECTION_STRING: ".center(80, "-"))
before, after = CONNECTION_STRING.split("PWD=")
logger.info(before + f"PWD=<{len(after)} characters>")

logger.info("Iniciando la realizacion de la conexion")
conn = pyodbc.connect(CONNECTION_STRING,autocommit=True)
logger.info("Conexion realizada con exito")