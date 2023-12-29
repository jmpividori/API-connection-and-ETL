from datetime import timedelta, datetime, time
import pandas as pd
import numpy as np
from ZeusAPI import *

client = Client("XXX.XX.XX.XXX", "XXXX", False, "AuthAccount", "Password", "AuthPass") # AuthAccount, Password and AuthPass se obtienen del servidor
FechaIni = datetime(2023, 12, 26)   # fecha desde = año/mes/dia
FechaFin = datetime.now()
Dias = (FechaFin - FechaIni).days
hora_especifica = time(0, 0, 0)  # Por ejemplo, 12:00:00
fecha_hora_especifica = datetime.combine(datetime.now(), hora_especifica)

def obtener_datos_historicos(client, estacion_id, dias, nombre_archivo):
    Fecha = []
    Hora = []
    mH2O = []
    
    myHistorical = client.GetHistorical(estacion_id, fecha_hora_especifica - timedelta(days=dias), fecha_hora_especifica, [0, 1])
    
    if myHistorical is None:
        print(client.GetZeusApiLastError())
    else:
        for element in myHistorical:
            Fecha.append(element['DateOfRecord'].strftime('%d-%m-%Y'))
            Hora.append(element['DateOfRecord'].strftime('%H:%M:%S'))
            mH2O.append(str(element['Value']))
    
        df = pd.DataFrame(list(zip(Fecha, Hora, mH2O)), columns=['Fecha', 'Hora', f'mH2O_{estacion_id}'])
        df['mH2O'] = df[f'mH2O_{estacion_id}'].astype('float64')
        df['mH2O'] = pd.Series([round(val, 2) for val in df['mH2O']])
        df = df.drop(columns=[f'mH2O_{estacion_id}'])
    
        df.to_csv(nombre_archivo, index=False)  # Ahora, se especifica index=False para evitar escribir el índice en el CSV

# Obtener una lista de IDs y nombres de estaciones
estaciones = client.GetAllStationExtendedProperties()
IDS = list(estaciones.keys())  # Convertir dict_keys a lista
Nombres = [estaciones[i].get('Name') for i in estaciones]

# Crear un DataFrame para almacenar todos los datos y las calidades de cada sensor
df_final = pd.DataFrame(columns=['Fecha', 'Hora'])

# Iterar sobre las estaciones y obtener los datos históricos
for _, row in enumerate(Nombres):
    nombre_archivo = f"{row}.csv"
    obtener_datos_historicos(client, IDS[_], Dias, nombre_archivo)
    
    # Leer el archivo CSV y fusionarlo con el DataFrame final
    df_temp = pd.read_csv(nombre_archivo)
    suffix = f"_{row}"  # Sufijo único basado en el nombre de la estación
    df_temp = df_temp.rename(columns={'mH2O': f'mH2O{suffix}'})  # Renombrar la columna mH2O
    df_final = pd.merge(df_final, df_temp, how='outer', on=['Fecha', 'Hora'])

    # Agregar la columna Calidades específica para cada sensor
    conditions = [
        (df_final[f'mH2O{suffix}'] > 15),
        (df_final[f'mH2O{suffix}'] > 7) & (df_final[f'mH2O{suffix}'] <= 15),
        (df_final[f'mH2O{suffix}'] > 5) & (df_final[f'mH2O{suffix}'] <= 7),
        (df_final[f'mH2O{suffix}'] > 2) & (df_final[f'mH2O{suffix}'] <= 5),
        (df_final[f'mH2O{suffix}'] > 0) & (df_final[f'mH2O{suffix}'] <= 2)
    ]
    choices = ["Muy buena", "Buena", "Aceptable", "Baja", "Insuficiente"]
    df_final[f'Calidades{suffix}'] = np.select(conditions, choices, default="ND")

# Guardar el DataFrame final en un archivo CSV
df_final.to_csv('datos_finales_calidades.csv', index=False)

# Agregar los nombres de los archivos CSV en la primera fila
with open('datos_finales_calidades.csv', 'r') as file:
    data = file.read()
with open('datos_finales_calidades.csv', 'w') as file:
    file.write(','.join(Nombres) + '\n' + data)

print("\nSe han descargado y agrupado todos los informes en 'datos_finales_calidades.csv'\n")
