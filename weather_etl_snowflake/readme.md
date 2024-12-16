# 🌦️ Weather ETL Pipeline with 🌐 Airflow and ❄️ Snowflake 🌟🌟🌟

This project is an ETL (📤📦📥) pipeline built using Apache Airflow. It extracts 📊 population and 🌥️ weather data, transforms it, and loads the processed data into ❄️ Snowflake for 📈 analysis.

## 🗂️ Table of Contents

1. [📋 Overview](#overview)
2. [✨ Features](#features)
3. [💻 Technologies Used](#technologies-used)
4. [🛠️ Pipeline Steps](#pipeline-steps)
5. [✅ Prerequisites](#prerequisites)
6. [🚀 Setup and Execution](#setup-and-execution)
7. [📂 Folder Structure](#folder-structure)
8. [🔮 Future Enhancements](#future-enhancements)

---

## 📋 Overview 🌍🌥️📈

The ETL pipeline fetches top 🌍 countries and 🏙️ cities data from the **World Population API** and 🌤️ weather forecasts from the **OpenWeather API**. The data is then processed and stored in ❄️ Snowflake for further analysis.

---

## ✨ Features 🛠️📥🔄

- **📤 Data Extraction**: Retrieves 🌍 countries and 🏙️ cities data from a web API and 🌥️ weather forecasts from OpenWeather.
- **🔄 Data Transformation**: Filters and transforms data into 📑 structured formats.
- **📥 Data Storage**: Loads the transformed data into ❄️ Snowflake tables.
- **🧩 Modular DAG Design**: The pipeline is modular, enabling task-level control and easy 🛠️ maintenance.

---

## 💻 Technologies Used 🌐💾🌍

- **🌀 Apache Airflow**: Workflow orchestration and task management.
- **❄️ Snowflake**: Cloud-based data warehouse for storing processed data.
- **🌤️ OpenWeather API**: Source of weather forecast data.
- **🌍 World Population API**: Source of countries and 🏙️ cities data.
- **🐼 Pandas**: For 📊 data manipulation and transformation.
- **🍲 BeautifulSoup**: For 🧾 HTML parsing.

---

## 🛠️ Pipeline Steps 🌤️📊❄️

1. **📝 Scrape Cities**: Extracts a list of top 🌍 countries and their 🏙️ cities along with population data.
2. **📤 Extract Weather Data**: Fetches 5-day 🌤️ weather forecasts for the extracted 🏙️ cities.
3. **🔄 Transform Weather Data**: Filters and restructures 🌤️ weather data for 📈 analysis.
4. **📥 Load Data to ❄️ Snowflake**: Creates a `weather_forecast` table in ❄️ Snowflake and loads the transformed data into it.

---

## ✅ Prerequisites 🔧🌀🌍

### 🔧 Required Tools and Accounts:

- 🐍 Python 3.8+
- 🐳 Docker (for running 🌀 Airflow)
- ❄️ Snowflake account with a 📂 database and schema.
- API 🔑 keys for:
  - 🌤️ OpenWeather API
  - 🌍 World Population API

### 🌀 Airflow Connections:

1. **❄️ Snowflake Connection**:
   - Connection ID: `snowflake_default`
   - Details: Include ❄️ Snowflake credentials (account, 📂 database, schema, username, password).
2. **HTTP Connections**:
   - `world_population_api`: API endpoint for 🌍 country and 🏙️ city data.
   - `openweather_api`: API endpoint for 🌤️ weather data (API 🔑 required).

---

## 🚀 Setup and Execution 🐳⚙️▶️

### 1. 📂 Clone the Repository

```bash
git clone https://github.com/<your-repo-name>.git
cd <your-repo-name>
```

### 2. ⚙️ Configure Airflow

1. Set up your **🌀 Airflow environment** using 🐳 Docker:
   ```bash
   docker-compose up -d
   ```
2. Add the required connections in 🌀 Airflow's **Admin > Connections**:
   - **❄️ Snowflake**: Use `snowflake_default` for ❄️ Snowflake configurations.
   - **🌍 World Population API**: Use `world_population_api` with 🌐 base URL.
   - **🌤️ OpenWeather API**: Use `openweather_api` and provide the API 🔑 in the connection's `Extra` field.

### 3. ▶️ Run the DAG

1. Navigate to the 🌀 Airflow UI (default: `http://localhost:8080`).
2. Enable and trigger the `updated_weather_etl_pipeline_snowflake` DAG.

---

## 📂 Folder Structure 📄📚📁

```
.
├── dags
│   ├── weather_etl_snowflake.py  # Main 🌀 DAG script
├── docker-compose.yml            # 🐳 Airflow setup
├── README.md                     # 📚 Project documentation
└── requirements.txt              # 🐍 Python dependencies
```

---

## 🔮 Future Enhancements 🌠🧪📊

- Implement ⚠️ error handling for ❄️ Snowflake queries and API requests.
- Add ✅ unit tests for helper functions and tasks.
- Automate the creation of ❄️ Snowflake tables with predefined schemas.
- Visualize the 📊 data stored in ❄️ Snowflake using 📊 BI tools.

---

## 🙏 Acknowledgments 🌟🎉🌍

- **🌤️ OpenWeather API** for 🌥️ weather data.
- **🌍 World Population API** for 📊 population data.
- The 🌀 Airflow and ❄️ Snowflake communities for their excellent 📚 documentation.

---

