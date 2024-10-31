CREATE OR REPLACE TABLE FORECAST_HOUR_STAGE(
    FORECAST_DATETIME VARCHAR(255),
    CONDITION_CODE VARCHAR(255),
    TEMP_C VARCHAR(255),
    IS_DAY VARCHAR(255),
    WIND_KPH VARCHAR(255),
    WIND_DIR VARCHAR(255),
    PRESSURE_MB VARCHAR(255),
    PRECIP_MM VARCHAR(255),
    HUMIDITY VARCHAR(255),
    CLOUD VARCHAR(255),
    DEWPOINT_C VARCHAR(255),
    GUST_KPH VARCHAR(255),
    WILL_IT_RAIN VARCHAR(255),
    CHANCE_OF_RAIN VARCHAR(255),
    WILL_IT_SNOW VARCHAR(255),
    CHANCE_OF_SNOW VARCHAR(255),
    SNOW_CM VARCHAR(255),
    UV VARCHAR(255),
    LOCATION_ID VARCHAR(255),
    FORECAST_DAY_WEATHER_ID VARCHAR(255),
    FORECAST_HOUR_WEATHER_ID VARCHAR(255)
);


CREATE OR REPLACE TABLE FACT_FORECAST_HOUR_WEATHER(
    FORECAST_HOUR_WEATHER_ID VARCHAR(255) PRIMARY KEY,
    FORECAST_DAY_WEATHER_ID VARCHAR(255),
    LOCATION_ID VARCHAR(255),
    CONDITION_CODE VARCHAR(255),
    FORECAST_DATETIME VARCHAR(255),
    TEMP_C VARCHAR(255),
    IS_DAY VARCHAR(255),
    WIND_KPH VARCHAR(255),
    WIND_DIR VARCHAR(255),
    PRESSURE_MB VARCHAR(255),
    PRECIP_MM VARCHAR(255),
    HUMIDITY VARCHAR(255),
    CLOUD VARCHAR(255),
    DEWPOINT_C VARCHAR(255),
    GUST_KPH VARCHAR(255),
    WILL_IT_RAIN VARCHAR(255),
    CHANCE_OF_RAIN VARCHAR(255),
    WILL_IT_SNOW VARCHAR(255),
    CHANCE_OF_SNOW VARCHAR(255),
    SNOW_CM VARCHAR(255),
    UV VARCHAR(255)
);



CREATE OR REPLACE PIPE FORECAST_HOUR_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO WEATHER_ETL_DB.PROJECT_SCHEMA.FORECAST_HOUR_STAGE
FROM @weather_etl_stage/forecast_hour_weather/;



CREATE OR REPLACE PROCEDURE forecast_hour_weather_procedure()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        // Step 1: Get the initial count of rows in stage_test
        var count_query = 'SELECT COUNT(DISTINCT FORECAST_HOUR_WEATHER_ID) AS count FROM FORECAST_HOUR_STAGE;';
        var count_result = snowflake.execute({ sqlText: count_query });
        var initial_stage_count = count_result.next() ? count_result.getColumnValue(1) : 0;

        // Step 2: Merge operation
        var merge_query = `
            MERGE INTO FACT_FORECAST_HOUR_WEATHER as target
            USING FORECAST_HOUR_STAGE AS source
            ON target.FORECAST_HOUR_WEATHER_ID = source.FORECAST_HOUR_WEATHER_ID
            WHEN MATCHED THEN
                UPDATE 
                    SET target.FORECAST_DAY_WEATHER_ID = source.FORECAST_DAY_WEATHER_ID,
                     target.LOCATION_ID = source.LOCATION_ID,
                     target.CONDITION_CODE = source.CONDITION_CODE,
                     target.FORECAST_DATETIME = source.FORECAST_DATETIME,
                     target.TEMP_C = source.TEMP_C,
                     target.IS_DAY = source.IS_DAY,
                     target.WIND_KPH = source.WIND_KPH,
                     target.WIND_DIR = source.WIND_DIR,
                     target.PRESSURE_MB = source.PRESSURE_MB,
                     target.PRECIP_MM = source.PRECIP_MM,
                     target.HUMIDITY = source.HUMIDITY,
                     target.CLOUD = source.CLOUD,
                     target.DEWPOINT_C = source.DEWPOINT_C,
                     target.GUST_KPH = source.GUST_KPH,
                     target.WILL_IT_RAIN = source.WILL_IT_RAIN,
                     target.CHANCE_OF_RAIN = source.CHANCE_OF_RAIN,
                     target.WILL_IT_SNOW = source.WILL_IT_SNOW,
                     target.CHANCE_OF_SNOW = source.CHANCE_OF_SNOW,
                     target.SNOW_CM = source.SNOW_CM,
                     target.UV = source.UV
                WHEN NOT MATCHED THEN
                    INSERT (target.FORECAST_HOUR_WEATHER_ID,target.FORECAST_DAY_WEATHER_ID,target.LOCATION_ID,target.CONDITION_CODE,target.FORECAST_DATETIME,target.TEMP_C,target.IS_DAY,target.WIND_KPH,target.WIND_DIR,target.PRESSURE_MB,target.PRECIP_MM,target.HUMIDITY,target.CLOUD,target.DEWPOINT_C,target.GUST_KPH,target.WILL_IT_RAIN,target.CHANCE_OF_RAIN,target.WILL_IT_SNOW,target.CHANCE_OF_SNOW,target.SNOW_CM,
target.UV)
                    VALUES (source.FORECAST_HOUR_WEATHER_ID,source.FORECAST_DAY_WEATHER_ID,source.LOCATION_ID,source.CONDITION_CODE,source.FORECAST_DATETIME,source.TEMP_C,source.IS_DAY,source.WIND_KPH,source.WIND_DIR,source.PRESSURE_MB,source.PRECIP_MM,source.HUMIDITY,source.CLOUD,source.DEWPOINT_C,source.GUST_KPH,source.WILL_IT_RAIN,source.CHANCE_OF_RAIN,source.WILL_IT_SNOW,source.CHANCE_OF_SNOW,source.SNOW_CM,
source.UV);
        `;
        // Execute the merge query
        snowflake.execute({ sqlText: merge_query });

        // Step 3: Get the number of rows inserted into target_test
        var inserted_query = `
            SELECT COUNT(DISTINCT FORECAST_HOUR_WEATHER_ID) AS count 
            FROM FACT_FORECAST_HOUR_WEATHER
            WHERE FORECAST_HOUR_WEATHER_ID IN (SELECT DISTINCT FORECAST_HOUR_WEATHER_ID FROM FORECAST_HOUR_STAGE);
        `;
        var inserted_result = snowflake.execute({ sqlText: inserted_query });
        var inserted_count = inserted_result.next() ? inserted_result.getColumnValue(1) : 0;

        // Step 4: Check if any new rows were successfully inserted
        if (inserted_count === initial_stage_count) {
            // All rows were inserted, truncate stage_test
            var truncate_query = 'TRUNCATE TABLE FORECAST_HOUR_STAGE;';
            snowflake.execute({ sqlText: truncate_query });
            return 'All rows inserted successfully, stage_test truncated.';
        } else {
            return `Merge completed, but not all rows were inserted. stage retained. 
                    Inserted rows: ${inserted_count}, Stage rows: ${initial_stage_count}`;
        }

    } catch (err) {
        return 'Error occurred: ' + err.message;
    }
$$;



CREATE OR REPLACE TASK forecast_hour_weather_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */4 * * * UTC'  -- This runs the task every 4hr
AS
CALL forecast_hour_weather_procedure();