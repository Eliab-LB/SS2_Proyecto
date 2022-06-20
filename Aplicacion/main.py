#!/usr/bin/python3
#Las librerias necesarias
# import pyodbc
import itertools
import pyodbc
import threading
import mysql.connector
import pandas as pd
import time
import sys


from imprimir import *
from creacion import *
from tabulate import tabulate
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

#Configurar nuestra conexion a SQL Server
CONNECTION_STRING = f"DRIVER={{{config.sql_driver}}};SERVER={config.sql_server};UID={config.sql_user};PWD={config.sql_password};DATABASE={config.sql_database}"

logger.info("Iniciando nuestra aplicacion")

logger.info(" CONNECTION_STRING: ".center(80, "-"))
before, after = CONNECTION_STRING.split("PWD=")
logger.info(before + f"PWD=<{len(after)} characters>")

logger.info("Iniciando la realizacion de la conexion")
conn_sql = pyodbc.connect(CONNECTION_STRING,autocommit=True)

logger.info("Conexion SQL realizada con exito")

#Configurar nuestra conexion a MySQL
conn_mysql = mysql.connector.connect(user=config.mysql_user, password=config.mysql_password, host=config.mysql_server, database=config.mysql_database)
logger.info("Conexion MySQL realizada con exito")

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
                animate("creando modelo")
        elif opcion=='2':
            logger.info('Usuario selecciona opcion de cargar informacion')
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
            conn_sql.close()
            logger.info('Conexion finalizada')
            exit()

def animate(message):
    chars = "/—\|" 
    for char in chars:
        sys.stdout.write('\r'+ message + '...'+char+'\r')
        time.sleep(.1)
        sys.stdout.flush() 

def ejecutar_consultas():
    f = open("consultas.txt","w")
    f.close()
    f = open("consultas.txt","a")
    logger.info("Ejecutar consultas")
    cursor = conn.cursor()
    logger.info("Top 10 artistas con mayor reproducciones")
    cursor.execute(TOP10_ARTISTAS_REPRODUCCIONES)
    f.write('Top 10 artistas con mayor reproducciones\n')
    f.write(tabulate(cursor,headers=['Artista', 'Reproducciones']))
    f.write('\n')
    cursor.execute(CANCIONES_MAS_REPRODUCIDAS)
    f.write('\nCanciones mas reproducidas\n')
    f.write(tabulate(cursor, headers=['Canción', 'Duración', 'Año', 'Explicita','Reproducciones']))
    f.write('\n')
    cursor.execute(TOP5_GENEROS_MAS_REPRODUCIDOS)
    f.write('\nTop 5 generos con mayor reproducciones\n')
    f.write(tabulate(cursor))
    f.write('\n')
    cursor.execute(ARTISTA_MAS_REPRODUCIDO_GENERO)
    f.write('\nArtista mas reproducido por genero\n')
    f.write(tabulate(cursor))
    f.write('\n')
    cursor.execute(CANCION_REPRODUCIDA_GENERO)
    f.write('\nCanción mas reproducida por genero\n')
    f.write(tabulate(cursor))
    f.write('\n')
    cursor.execute(CANCION_REPRODUCIDA_ANIO_LANZAMIENTO)
    f.write('\nCanción mas reproducida por año lanzamiento\n')
    f.write(tabulate(cursor))
    f.write('\n')
    cursor.execute(ARTISTAS_POPULARES)
    f.write('\nArtistas mas populares\n')
    f.write(tabulate(cursor))
    cursor.execute(CANCIONES_POPULARES)
    f.write('\nCanciones mas populares\n')
    f.write(tabulate(cursor))
    f.write('\n')
    cursor.execute(TOP_5_GENEROS_POPULARES)
    f.write('\nGeneros mas populares\n')
    f.write(tabulate(cursor))
    f.write('\n')
    cursor.execute(CANCIONES_MAS_REPRODUCIDA_EXPLICITO)
    f.write('\nCancion explicita mas reproducida\n')
    f.write(tabulate(cursor))
    f.write('\n')
    f.close()

def ejecutar_llenado():
    logger.info("Llenando")
    cursor = conn.cursor()
    logger.info("genero")
    buildGenero()           
    logger.info("artista")
    cursor.execute(LLENAR_ARTISTA)
    logger.info("cancion")
    llenar_cancion()
    logger.info("Data Warehouse creado")
    conn.commit()


def getIdArtista(artist):
    cursor = conn.cursor()
    # print('select * from artist where name = %s',artist)
    cursor.execute('select id from artist where name = %s',artist)
    result=cursor.fetchall()[0]
    return result[0]

def getIdGenero(song_genre):
    cursor = conn.cursor()
    generos=song_genre.split(',')
    genre_ids=[]
    for to_search in generos:
        cursor.execute("select id from genre where name =%s",to_search.strip())
        result=cursor.fetchall()[0]
        genre_ids.append(result[0])
    return list(dict.fromkeys(genre_ids))

def llenar_cancion():
    logger.info("llenando data canciones")
    cursor = conn.cursor()
    cursor.execute(OBTENER_CANCIONES)
    cursor2 = conn.cursor()
    for row in cursor:
        # print("cancion:", row[1]) # caancion
        artista = row[0]
        genero= row[5]
        id_artista=getIdArtista(artista)
        id_genero=getIdGenero(genero)
        song_name=str(row[2].replace('"', '\\\\"'))
        # print(song_name)
        song_statement = "INSERT INTO song (name,duration,year,explicit,reproductions) VALUES (\"{}\",'{}',{},{},{})".format(row[1],song_name,row[4],row[3],row[6]) 
        # print(song_statement)
        cursor2.execute(song_statement)
        id_cancion=cursor2.lastrowid
        # print("id_cancion: ",id_cancion)
        statement="INSERT INTO song_artist (id_song, id_artist) values ({},{})".format(id_cancion,id_artista)
        # print(statement)
        cursor2.execute(statement)
        # print(id_genero)
        for genero_cancion in id_genero:
            genero_statement = "INSERT INTO song_genre(id_song,id_genre) values({},{})".format(id_cancion,genero_cancion)
            cursor2.execute(genero_statement)
    logger.info('relacion cancion-genero creada')
    logger.info('relacion cancion-artista creada')
    conn.commit()
    # logger.info('cancion agregada')
        # print(id_genero)
        # print(id_artista)
        #get id_genero

def creacion():
    logger.info("Eliminando tablas...")
    cursor = conn_sql.cursor()
    cursor.execute(DROP_TABLES)
    logger.info("Tablas eliminadas correctamente")
    logger.info("Creando las tablas necesarias")
    logger.info("Creando tabla temporal")
    cursor.execute(TEMPORAL_CREATION)
    cursor.execute(COLLATE)
    # logger.info("Creando tabla genero")
    # cursor.execute(GENERO)
    # logger.info("Creando tabla artista")
    # cursor.execute(ARTISTA)
    # logger.info("Creando tabla cancion")
    # cursor.execute(CANCION)
    # cursor.execute(COLLATE_SONG)
    # logger.info("Creando tabla cancion_genero")
    # cursor.execute(CANCION_GENERO)
    # logger.info("Creando tabla cancion_artista")
    # cursor.execute(CANCION_ARTISTA)
    logger.info("Comenzando a procesar el dataset")
    try:
        print('leyendo csv')
        data = pd.read_csv("PIB_PERCAPITA.csv")
        # print(data)
        df = pd.DataFrame(data)
        df = df.fillna(0)
        logger.info("Dataset leido exitosamente")
        cargar_temporal(df)
        done=True
    except Exception as e:
        logger.error(e)
        conn.close()
        done=True
        exit()

    
def buildGenero():
    genero = set()
    cursor = conn.cursor()
    cursor.execute('select distinct genre from temporal;')
    for row in cursor:
        # print(row[0])
        genre_value=row[0].strip()
        generos_dirty = genre_value.split(',')
        for g in generos_dirty:
            genero.add(g.strip())
    for clean in genero:
        cursor.execute('INSERT INTO genre(name) values(%s)',clean)
    conn.commit()

def convertMillis(millis):
    millis = int(millis)
    seconds=(millis/1000)%60
    seconds = int(seconds)
    minutes=(millis/(1000*60))%60
    minutes = int(minutes)
    hours=(millis/(1000*60*60))%24
    return "%d:%d:%d" % (hours, minutes, seconds)

def cargar_temporal(df):
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