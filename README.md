# Weather Insights Data Pipeline | AWS Data Engineering Project 

## Introduction
This project is an automated pipeline designed to extract, transform, and load weather data from the WeatherAPI into a Snowflake Data Warehouse. Utilizing AWS services such as Lambda and S3, this project efficiently ingests real-time weather data, transforms it into structured formats, and loads it into Snowflake for further analysis and reporting. The pipeline ensures seamless integration between cloud services and data warehousing for weather data insights.

## Architecture
![Data Architecture](https://github.com/SrujanGowda-10/WeatherApi-Data-Engineering-project/blob/main/Weather%20ETL%20Architecture.png)

## Technology Used

1. **Programming Language**: Python
2. **Scripting Language**: SQL
3. **Amazon Web Services**
   - S3 Bucket
   - Lambda Function
   - Secrets Manager
   - EventBridge (CloudWatch Events)
4. **Snowflake Data Warehouse**
   - SnowPipe

  

## Dataset used
The source data for this project is taken from [WeatherAPI](https://www.weatherapi.com/), which provides real-time and forecasted weather data for cities. For this project, I focused on 10 major Indian cities, collecting data on their location, current weather conditions, and a two-day weather forecast. This data includes information such as temperature, humidity, wind speed, precipitation, and other key weather indicators for each city.

### More info about Dataset - https://www.weatherapi.com/docs/


## Data Model
![Data Model](https://github.com/SrujanGowda-10/WeatherApi-Data-Engineering-project/blob/main/Data%20Model.png)

## Python Scripts for project
- [Data Extraction](https://github.com/SrujanGowda-10/WeatherApi-Data-Engineering-project/blob/main/DataExtraction.py): Code to extract data from API and load it in S3 Bucket.
- [Data Transformation](https://github.com/SrujanGowda-10/WeatherApi-Data-Engineering-project/blob/main/DataTransformation.py): Code to perform transformation on extracted data and load it in S3 Bucket.

## Snowflake Scripts for project
1. [External Stage](https://github.com/SrujanGowda-10/WeatherApi-Data-Engineering-project/blob/main/SnowFlake-AWS%20connection.sql): Script for creating an external stage in Snowflake for data load from S3 Bucket.
2. [DIM_LOCATION](https://github.com/SrujanGowda-10/WeatherApi-Data-Engineering-project/blob/main/location.sql) | [DIM_CONDITION](https://github.com/SrujanGowda-10/WeatherApi-Data-Engineering-project/blob/main/condition.sql) | [FACT_CURRENT_DAY_WEATHER](https://github.com/SrujanGowda-10/WeatherApi-Data-Engineering-project/blob/main/current_weather.sql) | [FACT_FORECAST_DAY_WEATHER](https://github.com/SrujanGowda-10/WeatherApi-Data-Engineering-project/blob/main/forecast_day_weather.sql) | [FACT_FORECAST_HOUR_WEATHER](https://github.com/SrujanGowda-10/WeatherApi-Data-Engineering-project/blob/main/forecast_hour_weather.sql)
   - Table Creation: Each script defines SQL queries to create specific tables, where weather data is stored in a structured format.
   - Snowpipe Setup: The scripts include the creation of Snowpipes to automatically ingest data from AWS S3 buckets into Snowflake. Each Snowpipe is configured to load weather data into the appropriate table whenever new files arrive in the S3 bucket.
   - Task Scheduling: To maintain data freshness, the scripts implement Snowflake tasks that run at scheduled intervals, ensuring that the weather data is processed and loaded.
