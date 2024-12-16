from airflow import DAG  # Import the DAG class from Airflow
from airflow.providers.http.hooks.http import HttpHook  # Import HttpHook for making HTTP requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # Import SnowflakeHook
from airflow.decorators import task  # Import the @task decorator to define Airflow tasks
from airflow.utils.dates import days_ago  # Import the days_ago function for setting the start date of the DAG
from airflow.operators.python import PythonOperator  # Import PythonOperator for custom tasks
from datetime import datetime  # Import datetime for manipulating date and time
import pandas as pd  # Import pandas for handling data frames
import time  # Import time for adding delays between requests
from bs4 import BeautifulSoup  # Import BeautifulSoup for parsing HTML
import logging
from airflow.models import Variable
from pandas.errors import EmptyDataError
from geopy.geocoders import Nominatim

# Constants defining the endpoints for the API
COUNTRIES_ENDPOINT = "/countries"
CITIES_ENDPOINT_BASE = "/cities/"
SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_DATABASE = 'Snowflake_DATABASE'
SNOWFLAKE_SCHEMA = 'Snowflake_SCHEMA'
TABLE_NAME = 'weather_forecast'
HTTP_CONN_ID = 'world_population_api'
OPENWEATHER_CONN_ID = 'openweather_api'


# Helper functions to fetch data (keeping them unchanged)
def bring_file():
    file_path = '/usr/local/airflow/dags/country_continent.csv'
    return pd.read_csv(file_path)

def bring_file1():
    file_path = '/usr/local/airflow/dags/con_con.csv'
    return pd.read_csv(file_path)
# Function to get the continent for a given country code

def get_continent_by_country(country_code):
    continent_data = bring_file()
    # Check if the country code exists in the dataset and return the corresponding continent
    continent_row = continent_data[continent_data['cca2'] == country_code]
    if not continent_row.empty:
        return continent_row.iloc[0]['Continent']  # Directly access the value without .item()
    else:
        return "unknown"  # Return "unknown" if the country code is not found


def get_top_countries():
    """Fetches a list of countries and their slugs using HttpHook."""
    http_hook = HttpHook(http_conn_id=HTTP_CONN_ID, method='GET')
    response = http_hook.run(COUNTRIES_ENDPOINT)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch countries: {response.status_code}")
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="wpr-table")
    if not table:
        return []
    rows = table.find("tbody").find_all("tr")
    return [
        (row.find_all("td")[0].text.strip(), row.find_all("td")[0].text.strip().lower().replace(" ", "-"))
        for row in rows
    ]

def get_cities_for_country(country_name, country_slug):
    """Fetches a list of cities for a given country using HttpHook."""
    url = f"{CITIES_ENDPOINT_BASE}{country_slug}"
    http_hook = HttpHook(http_conn_id=HTTP_CONN_ID, method='GET')
    
    try:
        response = http_hook.run(url)
        # Check if the response status is successful
        if response.status_code != 200:
            return []

        # Attempt to parse the HTML content
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table")

        # If the table is not found, return an empty list
        if not table:
            return []

        # Extract city and population data
        return [
            {
                "City": row.find("th").text.strip(),
                "Population": row.find("td").text.strip()
            }
            for row in table.find("tbody").find_all("tr")
        ]
    
    except Exception as e:
        # Log the error (could be to a file or monitoring system)
        print(f"Error occurred while fetching cities for {country_name}: {str(e)}")
        # Return an empty list in case of any error
        return []




def get_weather_forecast(city):
    """Fetches the 5-day weather forecast for a given city using HttpHook."""
    http_hook = HttpHook(http_conn_id=OPENWEATHER_CONN_ID, method='GET')
    endpoint = '/data/2.5/forecast'
    
    try:
        connection = http_hook.get_connection(OPENWEATHER_CONN_ID)
        api_key = connection.extra_dejson.get('api_key')
        if not api_key:
            raise ValueError("API key for OpenWeather is missing!")
    except Exception as e:
        logging.error(f"Error getting connection or API key: {e}")
        raise ValueError("Error getting connection or API key")

    params = {
        'q': city,
        'cnt': 40,  # This will get the 5 forecast periods for 8-hour intervals
        'units': 'metric',
        'appid': api_key
    }
    
    try:
        response = http_hook.run(endpoint, data=params)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Error during API request: {e}")
        return []

    try:
        data = response.json()
        
        return [
            {
                'city': city,
                'date': datetime.utcfromtimestamp(forecast['dt']).strftime('%Y-%m-%d'),
                'hour': datetime.utcfromtimestamp(forecast['dt']).strftime('%H:%M:%S'),
                'temperature': forecast['main']['temp'],
                'feels_like': forecast['main']['feels_like'],
                'temp_min': forecast['main']['temp_min'],
                'temp_max': forecast['main']['temp_max'],
                'pressure': forecast['main']['pressure'],
                'sea_level': forecast['main']['sea_level'],
                'grnd_level': forecast['main']['grnd_level'],
                'humidity': forecast['main']['humidity'],
                'weathercode': forecast['weather'][0]['id'],
                'weather_main': forecast['weather'][0]['main'],
                'weather_description': forecast['weather'][0]['description'],
                'weather_icon': forecast['weather'][0]['icon'],
                'clouds_all': forecast['clouds']['all'],
                'windspeed': forecast['wind']['speed'],
                'winddirection': forecast['wind']['deg'],
                'windgust': forecast['wind']['gust'],
                'visibility': forecast['visibility'],
                'pop': forecast['pop'],
                'sys_pod': forecast['sys']['pod'],
                'latitude': data['city']['coord']['lat'],
                'longitude': data['city']['coord']['lon'],
                'country': data['city']['country'],
                'sunrise': datetime.utcfromtimestamp(data['city']['sunrise']).strftime('%Y-%m-%d %H:%M:%S'),
                'sunset': datetime.utcfromtimestamp(data['city']['sunset']).strftime('%Y-%m-%d %H:%M:%S'),
                'Continent': get_continent_by_country(data['city']['country'])
            }
            for forecast in data['list']
        ]
        
    except Exception as e:
        logging.error(f"Error parsing API response: {e}")
        return []
# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    dag_id='updated_weather_etl_pipeline_snowflake2.2',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # First Task: Scrape Countries and Cities
    @task()
    def scrape_countries_and_cities():
        """Scrape top countries and cities with populations."""
        countries = get_top_countries()
        all_data = []
        for country_name, country_slug in countries:
            cities = get_cities_for_country(country_name, country_slug)
            for city in cities:
                all_data.append({
                    "Country": country_name,
                    "City": city["City"],
                    "Population": city["Population"]
                })
            time.sleep(2)
        country_continent_df = bring_file1()
        df = pd.DataFrame(all_data)
        df = df.merge(country_continent_df[['country', 'Continent']], how='left', left_on='Country', right_on='country') 
        df=df[['Country','City','Population','Continent']]   

        cities_file = "/tmp/cities_with_populations.csv"
        df.to_csv(cities_file, index=False)
        return cities_file

    # Second Task: Parallel continent-based processing
    def fetch_weather_for_continent(continent, cities_file):
        """Fetch weather data for cities based on continent."""
        cities_df = pd.read_csv(cities_file)
        continent_df = cities_df[cities_df['Continent'] == continent]

        all_weather_data = []
        for city in continent_df['City']:
            city_weather_data = get_weather_forecast(city)
            all_weather_data.extend(city_weather_data)

        weather_file = f"/tmp/weather_forecast_{continent}.csv"
        pd.DataFrame(all_weather_data).to_csv(weather_file, index=False)
        return weather_file

   


    @task()
    def transform_and_merge_weather_data(continent_weather_files):
        """Transform weather data and merge all continent weather data."""
        all_data = []
        
        for file in continent_weather_files:
            try:
                # Try reading the CSV file for each continent
                df = pd.read_csv(file)
                all_data.append(df)
            except EmptyDataError:
                # If no data exists for a continent, log it or handle as required
                print(f"Warning: No data found for file {file}, skipping.")
              
        
        if all_data:
            # If there is data to merge
            merged_data = pd.concat(all_data, ignore_index=True)
            print(merged_data.columns)

            
            # Select relevant columns and add the continent to the final dataset
            # Ensure all necessary columns are included, like "feels_like", "temp_min", "temp_max", etc.
            """ merged_data = merged_data[[
                "city", "date", "hour", "temperature", "feels_like", "temp_min", "temp_max", "pressure",
                "sea_level", "grnd_level", "humidity", "weathercode", "weather_main", "weather_description", 
                "weather_icon", "clouds_all", "windspeed", "winddirection", "windgust", "visibility", "pop",
                "sys_pod", "latitude", "longitude", "country", "sunrise", "sunset", "Continent"
            ]]  """
            
            # Ensure the `Continent` column is present and filled
            if 'Continent' not in merged_data.columns:
                merged_data['Continent'] = "Unknown"  # or handle as per your logic
            
            # Save the transformed and merged data to a temporary CSV file
            transformed_file = "/tmp/transformed_weather_data_with_continent.csv"
            merged_data.to_csv(transformed_file, index=False)
            print(f"Data successfully transformed and saved to {transformed_file}")
        else:
            # Handle the case where all files were empty or no data was available
            print("Warning: No valid data available from the provided files.")
            transformed_file = None
        
        return transformed_file


    # Final task: Load transformed data to Snowflake
    @task()
    def load_weather_data_to_snowflake(transformed_file):
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Create the table with all the weather data attributes
            cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME} (
    city STRING,
    date STRING,
    hour STRING,
    temperature FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INT,
    sea_level INT,
    grnd_level INT,
    humidity INT,
    weathercode INT,
    weather_main STRING,
    weather_description STRING,
    weather_icon STRING,
    clouds_all INT,
    windspeed FLOAT,
    winddirection INT,
    windgust FLOAT,
    visibility INT,
    pop FLOAT,
    sys_pod STRING,
    latitude FLOAT,
    longitude FLOAT,
    country STRING,
    sunrise STRING,
    sunset STRING,
    Continent STRING
);
""")

            # Create or replace stage for file upload
            stage_name = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.weather_stage"
            cursor.execute(f"CREATE OR REPLACE STAGE {stage_name};")
            snowflake_hook.run(f"PUT file://{transformed_file} @{stage_name}")
            
            # Copy the transformed data into Snowflake
            cursor.execute(f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}
            FROM @{stage_name}/{transformed_file.split('/')[-1]}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE';
            """)
            conn.commit()

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

        return "Weather data loaded successfully using COPY INTO."

    # Task 1: Scrape Countries and Cities
    cities_file = scrape_countries_and_cities()

    # Task 2: Parallel continent processing (using PythonOperator)
    continents = ['Asia', 'Europe', 'Africa', 'North America', 'South America', 'Oceania']

    continent_weather_files = []
    for continent in continents:
        continent_weather_files.append(
            PythonOperator(
                task_id=f'fetch_weather_{continent.replace(" ", "_")}',
                python_callable=fetch_weather_for_continent,
                op_args=[continent, cities_file],
                dag=dag
            )
        )

    # Task 3: Transform and Merge Weather Data
    transformed_file = transform_and_merge_weather_data([task.output for task in continent_weather_files])

    # Task 4: Load Data to Snowflake
    load_weather_data_to_snowflake(transformed_file)

