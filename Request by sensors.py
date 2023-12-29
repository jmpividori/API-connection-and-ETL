from datetime import timedelta, datetime, time
from datetime import datetime
import pandas as pd
import numpy as np
import requests
from ZeusAPI import *

client = Client("200.45.71.124", "3030", False, "AuthAccount", "Password", "AuthPass") # AuthAccount, Password and AuthPass se obtienen del servidor
FechaIni = datetime(2023,12,6)   #fecha desde = año/mes/dia
FechaFin = datetime.now()
Dias = (FechaFin - FechaIni).days
hora_especifica = time(0, 0, 0)  # Por ejemplo, 12:00:00
fecha_hora_especifica = datetime.combine(datetime.now(), hora_especifica)
 
def obtener_datos_historicos(client, estacion_id, dias, nombre_archivo):
    Fecha = []
    Hora = []
    mH2O = []
    
    myHistorical = client.GetHistorical(estacion_id, fecha_hora_especifica - timedelta(days=dias), fecha_hora_especifica, [0, 1])
    
    if myHistorical == None:
        print(client.GetZeusApiLastError())
    else:
        for element in myHistorical:
            Fecha.append(element['DateOfRecord'].strftime('%d-%m-%Y'))
            Hora.append(element['DateOfRecord'].strftime('%H:%M:%S'))
            mH2O.append(str(element['Value']))
    
        df = pd.DataFrame(list(zip(Fecha, Hora, mH2O)), columns=['Fecha', 'Hora', 'mH2O'])
        df['mH2O'] = df['mH2O'].astype('float64')
        df['mH2O'] = pd.Series([round(val, 2) for val in df['mH2O']])
        df = df.set_index('Fecha')
    
        df.to_csv(nombre_archivo)

# Obtener una lista de IDs y nombres de estaciones
estaciones = client.GetAllStationExtendedProperties()
IDS = estaciones.keys()
Nombres = [estaciones[i].get('Name') for i in estaciones]

# Crear un DataFrame con los nombres y IDs de estaciones
df_estaciones = pd.DataFrame(list(zip(Nombres, IDS)), columns=['Nombre', 'ID'])
print(df_estaciones)
# df_estaciones.to_csv('a.csv')

# Iterar sobre las estaciones y obtener los datos históricos
i = 0
for _, row in df_estaciones.iterrows():
    nombre_archivo = f"{row['Nombre']}.csv"
    obtener_datos_historicos(client, row['ID'], Dias, nombre_archivo)
    i+=1
if i == 43:
    print("\nSe han descargado todos los informes\n")
