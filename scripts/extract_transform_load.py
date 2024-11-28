import requests
import pyodbc
import datetime


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

    cursor.execute(query, (data['name'], data['sys']['country'], data['weather'][0]['description'], data['main']['temp'], data['main']['feels_like'], data['main']['humidity'], data['wind']['speed'], datetime.datetime.now()))

    conn.commit()
    conn.close()
    
    print('Finnished process')

api_key = 'dbccfc4c26b643c2f0e9d5ed29a9f96d'

api_url = extract_data(api_key, 'berazategui', 'metric')

weather_data = transform_data(api_url)

upload_to_db('ODBC Driver 17 for SQL Server', '.\SQLEXPRESS', 'weather_db', 'WeatherData', 'Tommy', 'H1ban4', weather_data)

# conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; SERVER=.\SQLEXPRESS;DATABASE=weather_db;UID=Tommy;PWD=H1ban4')

# cursor = conn.cursor()

# query = "INSERT INTO WeatherData (city, countryID, weather, temperature, feels_like, humidity, wind_speed) VALUES (?, ?, ?, ?, ?, ?, ?)"

# cursor.execute(query, (data['name'], data['sys']['country'], data['weather'][0]['description'], data['main']['temp'], data['main']['feels_like'], data['main']['humidity'], data['wind']['speed']))

# conn.commit()
# conn.close()

# print('Finalizado')