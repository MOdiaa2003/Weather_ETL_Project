from airflow import DAG  # Import the DAG class from Airflow
from airflow.providers.http.hooks.http import HttpHook  # Import HttpHook for making HTTP requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # Import SnowflakeHook
from airflow.decorators import task  # Import the @task decorator to define Airflow tasks
from airflow.utils.dates import days_ago  # Import the days_ago function for setting the start date of the DAG
import pandas as pd  # Import pandas for handling data frames
import time  # Import time for adding delays between requests
from bs4 import BeautifulSoup  # Import BeautifulSoup for parsing HTML
from datetime import datetime  # Import datetime for manipulating date and time
import logging
from datetime import datetime
from airflow.models import Variable
# Constants defining the endpoints for the API
COUNTRIES_ENDPOINT = "/countries"  # Endpoint for fetching countries
CITIES_ENDPOINT_BASE = "/cities/"  # Base endpoint for fetching cities for a country
SNOWFLAKE_CONN_ID = 'snowflake_default'  # Connection ID for Snowflake
SNOWFLAKE_DATABASE = 'Snowflake_DATABASE'  # Specify your database name here
SNOWFLAKE_SCHEMA = 'Snowflake_SCHEMA'      # Specify your schema name here
TABLE_NAME = 'weather_forecast'
HTTP_CONN_ID = 'world_population_api'  # Connection ID for World Population 
OPENWEATHER_CONN_ID = 'openweather_api'  # Connection ID for OpenWeather API

def bring_file():
     file_path = '/usr/local/airflow/dags/country_continent.csv'
     return pd.read_csv(file_path)

# Helper function for getting a list of top countries
def get_top_countries():
    """Fetches a list of countries and their slugs using HttpHook."""
    http_hook = HttpHook(http_conn_id=HTTP_CONN_ID, method='GET')  # Create an HttpHook object for HTTP requests
    response = http_hook.run(COUNTRIES_ENDPOINT)  # Send a GET request to the countries endpoint
    if response.status_code != 200:  # Check if the response is successful
        raise Exception(f"Failed to fetch countries: {response.status_code}")  # Raise an error if the response is not OK
    soup = BeautifulSoup(response.text, "html.parser")  # Parse the response content using BeautifulSoup
    table = soup.find("table", class_="wpr-table")  # Find the table containing the countries' data
    if not table:  # If no table is found, return an empty list
        return []
    rows = table.find("tbody").find_all("tr") # Get the top 10 countries for demonstration
    return [
        (row.find_all("td")[0].text.strip(), row.find_all("td")[0].text.strip().lower().replace(" ", "-"))
        for row in rows  # Extract the country name and slug from each row in the table
    ]

# Helper function for getting a list of cities for a given country
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


# Helper function for getting weather forecast data for a city

def get_weather_forecast(city):
    """Fetches the 5-day weather forecast for a given city using HttpHook."""
    http_hook = HttpHook(http_conn_id=OPENWEATHER_CONN_ID, method='GET')  # Create an HttpHook object
    endpoint = '/data/2.5/forecast'  # Define the OpenWeather forecast endpoint
    
    try:
        connection = http_hook.get_connection(OPENWEATHER_CONN_ID)  # Get the connection details for OpenWeather API
        api_key = connection.extra_dejson.get('api_key')  # Retrieve the API key from connection extras
        if not api_key:
            raise ValueError("API key for OpenWeather is missing!")
    except Exception as e:
        logging.error(f"Error getting connection or API key: {e}")
        raise ValueError("Error getting connection or API key")

    params = {  # Define the parameters for the weather request
        'q': city,
        'cnt': 5,  # Number of forecast entries
        'units': 'metric',  # Get the temperature in Celsius
        'appid': api_key  # Provide the API key for authorization
    }
    
    try:
        response = http_hook.run(endpoint, data=params)  # Send the GET request to OpenWeather API
        response.raise_for_status()  # Will raise an HTTPError for bad responses (4xx, 5xx)
    except Exception as e:
        logging.error(f"Error during API request: {e}")
        return []  # Return an empty list if the response fails
    
    try:
        data = response.json()  # Parse the response as JSON
        return [
            {
                'city': city,
                'date': datetime.utcfromtimestamp(forecast['dt']).strftime('%Y-%m-%d'),  # Format the forecast date
                'hour': datetime.utcfromtimestamp(forecast['dt']).strftime('%H:%M:%S'),  # Format the forecast time
                'temperature': forecast['main']['temp'],  # Extract the temperature data
                'windspeed': forecast['wind']['speed'],  # Extract wind speed data
                'winddirection': forecast['wind']['deg'],  # Extract wind direction data
                'weathercode': forecast['weather'][0]['id'],  # Extract weather code (e.g., clear, rain)
                'latitude': data['city']['coord']['lat'],  # Extract latitude of the city
                'longitude': data['city']['coord']['lon'],  # Extract longitude of the city
                'country': data['city']['country'],  # Extract country of the city
            }
            for forecast in data['list']  # Extract the forecast data for each entry in the list
        ]
    except Exception as e:
        logging.error(f"Error parsing API response: {e}")
        return []  # Return an empty list if there was an error parsing the response

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'start_date': days_ago(1),  # Start date for the DAG, set to 1 day ago
}

# Define the DAG
with DAG(
    dag_id='updated_weather_etl_pipeline_snowflake2.1',  # The unique ID of the DAG
    default_args=default_args,  # Default arguments for the DAG
    schedule_interval='@daily',  # Schedule the DAG to run daily
    catchup=False,  # Do not backfill previous runs
) as dag:
    
    

    # Task to scrape top countries and cities
    @task()
    def scrape_cities():
        """Scrape top countries and cities with populations."""
        countries = get_top_countries()  # Get the top countries
        all_data = []  # List to store data for cities and populations
        for country_name, country_slug in countries:
            cities = get_cities_for_country(country_name, country_slug)  # Get cities for each country
            for city in cities:
                all_data.append({
                    "Country": country_name,
                    "City": city["City"],
                    "Population": city["Population"]
                })  # Append each city with its country and population to the data list
            time.sleep(2)  # Add a 2-second delay between requests to avoid overwhelming the server

        df = pd.DataFrame(all_data)  # Convert the data to a pandas DataFrame
        cities_file = "/tmp/cities_with_populations.csv"  # Define the file path for saving the cities data
        df.to_csv(cities_file, index=False)  # Save the DataFrame as a CSV file
        return cities_file  # Return the path to the saved file

    # Task to extract weather data for cities
    @task()
    def extract_weather_data(cities_file):
        """Extract weather data for cities."""
        cities_df = pd.read_csv(cities_file)  # Read the cities data from CSV into a DataFrame
        cities = cities_df['City'].tolist()  # Get a list of cities

        all_weather_data = []  # List to store weather data for each city
        for city in cities:
            city_weather_data = get_weather_forecast(city)  # Get weather data for each city
            all_weather_data.extend(city_weather_data)  # Add the weather data to the list

        weather_file = "/tmp/weather_forecast_all_cities.csv"  # Define the file path for saving weather data
        pd.DataFrame(all_weather_data).to_csv(weather_file, index=False)  # Save the weather data as a CSV file
        return weather_file  # Return the path to the saved file

    @task()
    def transform_weather_data(weather_file):
        """Transform weather data and add continent."""
        # Load the weather data
        df = pd.read_csv(weather_file)

        # Load the country-continent mapping file
        country_continent_df =bring_file()

        # Merge the weather data with the country-continent mapping based on the country
        df = df.merge(country_continent_df[['cca2', 'Continent']], how='left', left_on='country', right_on='cca2')

        # Select relevant columns and add the continent to the final dataset
        transformed_data = df[["city", "date", "hour", "temperature", "windspeed", "winddirection", "weathercode", "country", "Continent"]]
        
        # Save the transformed data
        transformed_file = "/tmp/transformed_weather_data_with_continent.csv"
        transformed_data.to_csv(transformed_file, index=False)

        return transformed_file  # Return the path to the transformed file


    @task()
    def load_weather_data_to_snowflake(transformed_file):
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME} (
                city STRING,
                date DATE,
                hour STRING,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                country STRING, 
                Continent STRING

            );
            """)

            stage_name = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.weather_stage"
            cursor.execute(f"CREATE OR REPLACE STAGE {stage_name};")
            snowflake_hook.run(f"PUT file://{transformed_file} @{stage_name}")
            cursor.execute(f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}
            FROM @{stage_name}/{transformed_file.split('/')[-1]}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'SKIP_FILE';
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
        return "Weather data loaded successfully using COPY INTO."
    # Define the DAG tasks and their dependencies
    cities_file = scrape_cities()  # Scrape cities and save to file
    weather_file = extract_weather_data(cities_file)  # Extract weather data from the cities file
    transformed_file = transform_weather_data(weather_file)  # Transform the weather data
    load_weather_data_to_snowflake(transformed_file)  # Load the transformed data into PostgreSQL
