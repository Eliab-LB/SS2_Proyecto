from tabulate import tabulate
import sys
def print_main_menu():
    sys.stdout.flush()
    print(tabulate([['Crear Modelo'], ['Cargar Información'], ['Ejecutar consultas'], ['Para salir presione cualquier otra tecla']],headers=['Main Menu'],showindex=['1','2','3','#'],tablefmt='pretty'))