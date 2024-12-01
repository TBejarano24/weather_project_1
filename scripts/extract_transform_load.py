import requests
import pyodbc
from datetime import datetime
from google.cloud import bigquery

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
    ''
]


def extract_data(key, city, units, lang = 'en'):
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}&units={units}&lang={lang}'
    
    return url

def transform_data(url):
    response = requests.get(url)
    data = response.json()
    
    return data

def upload_to_db(driver, server, database, table, user, password, data):
    conn = pyodbc.connect(f'DRIVER={{{driver}}}; SERVER={server};DATABASE={database};UID={user};PWD={password}')

    cursor = conn.cursor()

    query = f"INSERT INTO {table} (city, countryID, weather, temperature, feels_like, humidity, wind_speed, date_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

    cursor.execute(query, (data['name'], data['sys']['country'], data['weather'][0]['description'], data['main']['temp'], data['main']['feels_like'], data['main']['humidity'], data['wind']['speed'], datetime.now()))

    conn.commit()
    conn.close()
    
    print('Finnished process')
    
def upload_to_bigquery (driver, server, database, table, user, password, data):
    sql_conn = pyodbc.connect(f'DRIVER={{{driver}}}; SERVER={server};DATABASE={database};UID={user};PWD={password}')
    
    cursor = sql_conn.cursor()
    
    cursor.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()
    
    client = bigquery.Client()
    dataset_id = "vivid-argon-443221-h5.BigQuery_WeatherData"
    table_id = f"{dataset_id}.WeatherData_GoogleCloud"
    
    rows_to_insert = [{"weatherID": row[0], "city": row[1], "countryID": row[2], "weather": row[3], "temperature": float(row[4]), "feels_like": float(row[5]), "humidity": row[6], "wind_speed": float(row[7]), "date_time": row[8].isoformat()} for row in rows]
    
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("Errores al insertar:", errors)
    else:
        print("Datos subidos correctamente.")

    sql_conn.close()
    
    print("Finnished")

api_key = 'dbccfc4c26b643c2f0e9d5ed29a9f96d'

api_url = extract_data(api_key, 'cordoba', 'metric')

weather_data = transform_data(api_url)

upload_to_db('ODBC Driver 17 for SQL Server', '.\SQLEXPRESS', 'weather_db', 'WeatherData', 'Tommy', 'H1ban4', weather_data)

upload_to_bigquery('ODBC Driver 17 for SQL Server', '.\SQLEXPRESS', 'weather_db', 'WeatherData', 'Tommy', 'H1ban4', weather_data)

# conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=.\SQLEXPRESS;DATABASE=weather_db;UID=Tommy;PWD=H1ban4')

# cursor = conn.cursor()

# query = "INSERT INTO WeatherData (city, countryID, weather, temperature, feels_like, humidity, wind_speed) VALUES (?, ?, ?, ?, ?, ?, ?)"

# cursor.execute(query, (data['name'], data['sys']['country'], data['weather'][0]['description'], data['main']['temp'], data['main']['feels_like'], data['main']['humidity'], data['wind']['speed']))

# conn.commit()
# conn.close()

# print('Finalizado')