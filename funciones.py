#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 20 12:34:20 2022

@author: alumno
"""
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
import json

VALORES = ['user_day_code','user_type','ageRange','idplug_station',
         'idunplug_station','unplug_hourTime','travel_time']

def adaptar(dicc):
        """
        Desempaqueta las fechas en forma unplug_hourTime: {$date: string}.
        """
    
        if isinstance(dicc['unplug_hourTime'], dict):
            dicc['unplug_hourTime'] = dicc['unplug_hourTime']['$date']
            
        return dicc


def leer(lista):
    """
    Recibe una lista de archivos json, se queda con los datos relevantes, y ajusta el formato de las fechas.
    Devuelve un unico rdd con todos los datos.
    """
    
    if not isinstance(lista, list):
        lista = [lista]
        
    sc = SparkContext.getOrCreate()
    aux = lambda x: {a:x[a] for a in VALORES}
    
    total = sc.textFile(lista[0]).map(json.loads).map(aux)
    
    for archivo in lista[1:]:
        dat = sc.textFile(archivo).map(json.loads).map(aux)
        total = total.union(dat)
    
    return total.map(adaptar)



import re
import datetime
PATTERN = re.compile(r'[+][0-9]+$')

def tiempo(content):
    """
    Una funcion para transformar string a objeto datetime.
    Las fechas tienen dos formatos distintos y se tratan de la misma forma
    con una expresion regular.
    """
    
    match = PATTERN.search(content)
    if match:
        formato = '%Y-%m-%dT%H:%M:%S.%f%z'  
    else:
        formato = '%Y-%m-%dT%H:%M:%SZ'
        
    return datetime.datetime.strptime(content, formato)




def condicion(lista):
    """
    Los viajes se enlazan, forman "camino". Se considera que cada viaje sucesivo empieza
    en la estacion donde acabo el viaje previo.
    """
    
    b = True
    j = 1
    while j < len(lista):
        b = b and lista[j-1]['idunplug_station'] == lista[j]['idplug_station']
        j += 1
    return b



# por definir
def contador(dicc,modo=0):
    
    if modo == 0:
        sol = tiempo(dicc['unplug_hourTime']).month
     
    elif modo == 1:
        day = str(tiempo(dicc['unplug_hourTime']).day)
        month = str(tiempo(dicc['unplug_hourTime']).month)
        year= str(tiempo(dicc['unplug_hourTime']).year)
        sol = month + '-' + day + '-' + year
        
    elif modo == 2:
        sol = tiempo(dicc['unplug_hourTime']).hour
    
    return sol