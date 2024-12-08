# #Weather ETL Pipeline

## Overview

This project defines an **Airflow DAG** that automates the process of extracting, transforming, and loading (ETL) weather data for major cities around the world. The pipeline integrates data from multiple sources and loads it into a PostgreSQL database.

## Features

- **Country and City Scraping:** Fetches a list of top countries and their cities with population data.
- **Weather Data Extraction:** Retrieves a 5-day weather forecast for each city using the OpenWeather API.
- **Data Transformation:** Filters and organizes weather data into a structured format for storage.
- **Data Loading:** Saves the transformed data into a PostgreSQL database.

---

## Prerequisites

1. **Apache Airflow** installed and configured.
2. **PostgreSQL** database with a connection set up in Airflow.
3. **OpenWeather API** key for fetching weather data.
4. **BeautifulSoup4** and **pandas** Python libraries for data processing.
5. Configured Airflow connections:
   - `world_population_api` for fetching country and city data.
   - `openweather_api` with the OpenWeather API key.
   - `postgres_default1` for connecting to PostgreSQL.

---

## Pipeline Tasks

### 1. **`scrape_cities()`**
   - Scrapes the top countries and their cities with population data from the `world_population_api`.
   - Saves the city data to a CSV file: `/tmp/cities_with_populations.csv`.

### 2. **`extract_weather_data()`**
   - Extracts weather forecast data for each city using the OpenWeather API.
   - Saves the weather data to a CSV file: `/tmp/weather_forecast_all_cities.csv`.

### 3. **`transform_weather_data()`**
   - Transforms and filters relevant weather attributes (e.g., temperature, wind speed, weather code).
   - Saves the transformed data to a CSV file: `/tmp/transformed_weather_data.csv`.

### 4. **`load_weather_data_to_postgres()`**
   - Creates a `weather_forecast` table in PostgreSQL if it does not exist.
   - Loads the transformed weather data into the `weather_forecast` table.

---

## File Structure

```
project/
│
├── dags/
│   └── updated_weather_etl_pipeline.py  # The main DAG script
├── tmp/
│   ├── cities_with_populations.csv      # Temporary file for city data
│   ├── weather_forecast_all_cities.csv  # Temporary file for raw weather data
│   └── transformed_weather_data.csv     # Temporary file for transformed data
└── README.md                            # This file
```

---

## Database Table Structure

**Table Name:** `weather_forecast`

| Column         | Data Type   | Description                              |
|----------------|-------------|------------------------------------------|
| `city`         | VARCHAR(255)| Name of the city                        |
| `date`         | DATE        | Forecast date                           |
| `hour`         | TIME        | Forecast hour                           |
| `temperature`  | FLOAT       | Temperature in Celsius                  |
| `windspeed`    | FLOAT       | Wind speed in m/s                       |
| `winddirection`| FLOAT       | Wind direction in degrees               |
| `weathercode`  | INT         | Weather condition code (e.g., clear, rain) |

---

## How to Run

1. **Start Airflow**:
   ```bash
   airflow scheduler & airflow webserver
   ```
2. **Create Connections in Airflow**:
   - `world_population_api`: HTTP connection for country/city data.
   - `openweather_api`: HTTP connection with `api_key` for OpenWeather.
   - `postgres_default1`: PostgreSQL connection.
3. **Trigger the DAG**:
   - Log in to the Airflow web UI and trigger the `updated_weather_etl_pipeline` DAG.

---

## API References

- **World Population API**: Provides country and city data.
- **OpenWeather API**: Provides 5-day weather forecast data.

---

## Notes

- Ensure appropriate API limits are respected to avoid service disruption.
- Temporary CSV files are stored in `/tmp/`. These can be modified in the DAG script if needed.
- PostgreSQL table creation is idempotent; running the DAG multiple times will not duplicate the table.

---

## Author
Mohamed Saad
