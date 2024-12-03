import requests
import pyodbc
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery


#lista con las ciudades de las que se solicitaran datos del clima
cities_list = [
    'buenos aires',
    'rosario',
    'catamarca',
    'resistencia',
    'rawson',
    'cordoba',
    'corrientes',
    'parana',
    'formosa',
    'san salvador de jujuy',
    'santa rosa,ar',
    'la rioja,ar',
    'mendoza',
    'posadas',
    'neuquen',
    'viedma',
    'salta',
    'san juan',
    'san luis',
    'rio gallegos',
    'santiago del estero',
    'ushuaia',
    'tucuman'
]

#cargando variables de entorno
load_dotenv('C:/Users/Soraya/Desktop/Tommy/CSE_110/weather_project/scripts/variables.env')

api_key = os.getenv('API_KEY')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PWD')
csv_path = os.getenv('CSV_ROUTE')
google_credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
projectID = os.getenv('PROJECT_ID')
datasetID = os.getenv('DATASET_ID')
tableID = os.getenv('TABLE_ID')


#función para generar la url con los datos requeridos
def generate_url(key, city, units, lang = 'en'):
    
    print(f'Requesting data for {city}')
    
    try:
    
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}&units={units}&lang={lang}'
        
        return url
    
    except Exception as e:
        print(f'Error: {e}')


#función para extraer y procesar los datos de la url
def extract_transform_data(url):
    
    try:
        
        #Se extraen los datos y se convierten en un diccionario de python
        response = requests.get(url)
        data = response.json()
        
        return data
    
    except Exception as e:
        print(f'Error: {e}')


#función para conectar con la base de datos y subir los datos disponibles
def upload_to_db(driver, server, database, table, user, password, data):
    
    try:
    
        print(f'Establishing connection with {server}...')
        
        conn = pyodbc.connect(f'DRIVER={{{driver}}}; SERVER={server};DATABASE={database};UID={user};PWD={password}')

        #se crea un cursor para ejecutar consultas SQL
        cursor = conn.cursor()
        
        print('Connection succesfully established')

        print(f'Inserting or updating data in {table}')

        #se escribe la consulta en formato de string y se guarda en una variable.
        #la consulta comprueba si los datos existen en la tabla; si existen los
        #actualiza, sino los inserta
        #los signos de interrogación son marcadores de posición
        query = f"""
        IF NOT EXISTS(SELECT 1 FROM {table} WHERE city = ?)
        BEGIN
            INSERT INTO {table} (city, countryID, weather, temperature, feels_like, humidity, wind_speed, date_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        END
        ELSE
        BEGIN
            UPDATE {table}
            SET countryID = ?, weather = ?, temperature = ?, feels_like = ?, humidity = ?, wind_speed = ?, date_time = ? WHERE city = ?
        END
        """

        #se ejecuta la consulta y se insertan los valores en sus respectivas posiciones
        cursor.execute(query, (data['name'],
                            data['name'], data['sys']['country'], data['weather'][0]['description'], data['main']['temp'], data['main']['feels_like'], data['main']['humidity'], data['wind']['speed'], datetime.now(),
                            data['sys']['country'], data['weather'][0]['description'], data['main']['temp'], data['main']['feels_like'], data['main']['humidity'], data['wind']['speed'], datetime.now(), data['name']))

        #se guardan los cambios
        conn.commit()
        
        print('Data inserted or updated successfully!')
        
    except Exception as e:
        print(f'Error: {e}')
        
    finally:
        #se cierra la conexión
        conn.close()

#función para guardar los datos a nivel local en un archivo CSV
def import_as_csv (driver, server, database, table, user, password, output_file):
    
    try:
    
        print(f'Establishing connection with {server}...')
        
        sql_conn = pyodbc.connect(f'DRIVER={{{driver}}}; SERVER={server};DATABASE={database};UID={user};PWD={password}')
        
        print('Connection successfully established')
        
        print(f'Retrieving data from {table}...')
        
        #se toman todos los datos de la tabla en la base de datos
        query = f"SELECT * FROM {table}"
        
        #se ejecuta la consulta y los datos se convierten en dataframe
        df = pd.read_sql(query, con=sql_conn)
        
        print('Importing as CSV...')
        
        #se convierte el dataframe en archivo CSV y se guarda en la
        #ruta y formato especificados
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        print('File imported succesfully!')
        
    except Exception as e:
        print(f'Error: {e}')


    finally:
        #se cierra la conexión
        sql_conn.close()

#función para subir los datos a Big Query
def upload_to_bigquery(credentials, csv_file_path, project_id, dataset_id, table_id):
    
    try:
        
        print('Checking credentials...')
        
        #verificando las credenciales de google cloud
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
        
        print('Locating project...')
        
        #se crea un cliente para interactuar con el proyecto en Big Query
        client = bigquery.Client(project=project_id)
        
        #se guarda la referencia de la tabla
        table_ref = f'{project_id}.{dataset_id}.{table_id}'
        
        #se especifica la configuración para cargar los datos a la tabla
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True
        )
        
        print('Uploading file...')
        
        #se abre el archivo en formato binario y se carga a la tabla especificada
        #con la configuración especificada
        with open(csv_file_path, 'rb') as csv_file:
            
            load_job = client.load_table_from_file(
                csv_file, table_ref, job_config=job_config
            )
        
        #espera hasta que acabe la sincronización de datos
        load_job.result()
        
        #se obtienen los datos de la tabla, se verifica la cantidad
        #de registros cargados y se muestran en la terminal
        table = client.get_table(table_ref)
        print(f"{table.num_rows} rows were loaded in {table_id}.")
    
    except Exception as e:
        print(f'Error: {e}')


for city in cities_list:
    
    api_url = generate_url(api_key, city, 'metric')

    weather_data = extract_transform_data(api_url)

    upload_to_db('ODBC Driver 17 for SQL Server', '.\SQLEXPRESS', 'weather_db', 'WeatherData', db_user, db_password, weather_data)

import_as_csv('ODBC Driver 17 for SQL Server', '.\SQLEXPRESS', 'weather_db', 'WeatherData', db_user, db_password, csv_path)

upload_to_bigquery(google_credentials, csv_path, projectID, datasetID, tableID)