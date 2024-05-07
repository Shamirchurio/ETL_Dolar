import requests
from datetime import datetime
import mysql.connector
import schedule
import time


def obtener_datos_y_guardar():
    try:
        limit = 1000
        offset = 0

        api_url = 'https://www.datos.gov.co/resource/ceyp-9c7c.json' 

        all_data = []

        while True:
            params = {'$limit': limit, '$offset': offset}
            
            response = requests.get(api_url, params=params)
            data = response.json()
            
            all_data.extend(data)
            
            if len(data) < limit:
                break
            
            offset += limit

        # Ordenar los datos por vigenciadesde descendente
        all_data.sort(key=lambda x: x['vigenciadesde'], reverse=True)
        last_data = all_data[0]
        
        vigenciadesde = [x['vigenciadesde'] for x in all_data]
        vigenciahasta = [x['vigenciahasta'] for x in all_data]

        vigenciadesde=list(map(lambda f: datetime.strptime(f, '%Y-%m-%dT%H:%M:%S.%f'), vigenciadesde))
        vigenciahasta=list(map(lambda f: datetime.strptime(f, '%Y-%m-%dT%H:%M:%S.%f'), vigenciahasta))
        last_data=[last_data]

        # Configuración de la conexión
        cnx = mysql.connector.connect(
            host='localhost', # IP del servidor MySQL remoto
            user='root', 
            database='trm_dolar',
            port='1100' 
        )

        cursor = cnx.cursor()

        # Realizar operaciones en la base de datos

        for i in range(len(last_data)):
            valor = last_data[i]['valor']
            sql = "INSERT INTO dolar_cop (valor, vigenciadesde, vigenciahasta) VALUES (%s, %s, %s)"
            val = (valor, vigenciadesde[i], vigenciahasta[i])
            cursor.execute(sql, val)
        # Confirmar los cambios en la base de datos
        cnx.commit()    

        # Cerrar la conexión
        cursor.close()
        cnx.close()
    except mysql.connector.Error as err:
        print(f"Error de MySQL: {err}")
        # Aquí puedes manejar el error de conexión como desees

# Programar la ejecución diaria a una hora específica
schedule.every().day.at("12:07").do(obtener_datos_y_guardar)

while True:
    schedule.run_pending()
    time.sleep(1)
