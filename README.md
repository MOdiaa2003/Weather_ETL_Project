

# Weather ETL Pipeline

## Overview

This project defines an **Airflow DAG** that automates the process of extracting, transforming, and loading (ETL) weather data for major cities around the world. The pipeline integrates data from multiple sources and loads it into a PostgreSQL database.

## Features

- **Country and City Scraping:** Fetches a list of top countries and their cities with population data.
- **Weather Data Extraction:** Retrieves a 5-day weather forecast for each city using the OpenWeather API.
- **Data Transformation:** Filters and organizes weather data into a structured format for storage.
- **Data Loading:** Saves the transformed data into a PostgreSQL database.

---

## Prerequisites

1. **Astro CLI** installed ([Installation Guide](<https://www.astronomer.io/docs/astro/cli/install-cli/?tab=windowswithwinget#install-the-astro-cli>)).
2. **Docker** installed and running.
3. **PostgreSQL** database configured within Docker Compose.
4. **OpenWeather API** key for fetching weather data.
5. **BeautifulSoup4** and **pandas** Python libraries for data processing.
6. Configured Airflow connections:
   - `world_population_api` for fetching country and city data.
   - `openweather_api` with the OpenWeather API key.
   - `postgres_default1` for connecting to PostgreSQL.

---

## Starting Airflow with Astro CLI and Docker Compose

1. **Initialize the Astro Project**:
   ```bash
   astro dev init
   ```

2. **Configure Docker Compose**:

   Create a `docker-compose.yml` file with the following content:

   ```yaml
   version: '3'
   services:
     postgres:
       image: postgres:13
       container_name: postgres_db
       environment:
         POSTGRES_USER: postgres
         POSTGRES_PASSWORD: postgres
         POSTGRES_DB: postgres
       ports:
         - "5432:5432"
       volumes:
         - postgres_data:/var/lib/postgresql/data

   volumes:
     postgres_data:
   ```

3. **Start Airflow**:
   ```bash
   astro dev start
   ```

4. **Access Airflow UI**:
   - Open [http://localhost:8080](http://localhost:8080) in your browser.
   - Default credentials: `admin` / `admin`.

5. **Create Connections in Airflow UI**:
   - `world_population_api`: HTTP connection for country/city data.
   - `openweather_api`: HTTP connection with `api_key` for OpenWeather.
   - `postgres_default1`: PostgreSQL connection (`Host: postgres`, `Port: 5432`, `User: postgres`, `Password: postgres`).

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
├── docker-compose.yml                   # Docker Compose configuration for PostgreSQL
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

## How to Run the DAG

1. **Start Airflow** using Astro CLI:
   ```bash
   astro dev start
   ```

2. **Create Connections** in the Airflow UI:
   - `world_population_api`: HTTP connection for country/city data.
   - `openweather_api`: HTTP connection with `api_key` for OpenWeather.
   - `postgres_default1`: PostgreSQL connection (`Host: postgres`, `Port: 5432`).

3. **Trigger the DAG**:
   - Log in to the Airflow web UI and trigger the `updated_weather_etl_pipeline` DAG.

---

## API References

- **World Population API**: Provides country and city data.
- **OpenWeather API**: Provides 5-day weather forecast data.

---

## Data

The data for cities, countries, and weather forecasts are stored in the temporary files listed in the **File Structure** section.

---

## Notes

- Ensure appropriate API limits are respected to avoid service disruption.
- Temporary CSV files are stored in `/tmp/`. These can be modified in the DAG script if needed.
- PostgreSQL table creation is idempotent; running the DAG multiple times will not duplicate the table.
- For Dockerized PostgreSQL, use volumes to persist the database data between container restarts.

---

## My LinkedIn

[https://www.linkedin.com/in/mohameddiaa2003/](https://www.linkedin.com/in/mohameddiaa2003/)
