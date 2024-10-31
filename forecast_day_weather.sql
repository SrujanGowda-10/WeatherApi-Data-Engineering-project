CREATE OR REPLACE TABLE FORECAST_DAY_STAGE(
    FORECAST_DATE VARCHAR(255),
    MAX_TEMP_C VARCHAR(255),
    AVG_TEMP_C VARCHAR(255),
    MIN_TEMP_C VARCHAR(255),
    MAX_WIND_KPH VARCHAR(255),
    TOTAL_PRECIP_MM VARCHAR(255),
    TOTAL_SNOW_CM VARCHAR(255),
    AVG_HUMIDITY VARCHAR(255),
    DAILY_WILL_IT_RAIN VARCHAR(255),
    DAILY_CHANCE_OF_RAIN VARCHAR(255),
    DAILY_WILL_IT_SNOW VARCHAR(255),
    DAILY_CHANCE_OF_SNOW VARCHAR(255),
    UV VARCHAR(255),
    SUNRISE_TIME VARCHAR(255),
    SUNSET_TIME VARCHAR(255),
    MOONRISE_TIME VARCHAR(255),
    MOONSET_TIME VARCHAR(255),
    CONDITION_CODE VARCHAR(255),
    LOCATION_ID VARCHAR(255),
    FORECAST_DAY_WEATHER_ID VARCHAR(255)
);


CREATE OR REPLACE TABLE FACT_FORECAST_DAY_WEATHER(
    FORECAST_DAY_WEATHER_ID VARCHAR(255) PRIMARY KEY,
    LOCATION_ID VARCHAR(255),
    CONDITION_CODE VARCHAR(255),
    FORECAST_DATE VARCHAR(255),
    MAX_TEMP_C VARCHAR(255),
    AVG_TEMP_C VARCHAR(255),
    MIN_TEMP_C VARCHAR(255),
    MAX_WIND_KPH VARCHAR(255),
    TOTAL_PRECIP_MM VARCHAR(255),
    TOTAL_SNOW_CM VARCHAR(255),
    AVG_HUMIDITY VARCHAR(255),
    DAILY_WILL_IT_RAIN VARCHAR(255),
    DAILY_CHANCE_OF_RAIN VARCHAR(255),
    DAILY_WILL_IT_SNOW VARCHAR(255),
    DAILY_CHANCE_OF_SNOW VARCHAR(255),
    UV VARCHAR(255),
    SUNRISE_TIME VARCHAR(255),
    SUNSET_TIME VARCHAR(255),
    MOONRISE_TIME VARCHAR(255),
    MOONSET_TIME VARCHAR(255)											
);

----Snowpipe------
CREATE OR REPLACE PIPE FORECAST_DAY_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO WEATHER_ETL_DB.PROJECT_SCHEMA.FORECAST_DAY_STAGE
FROM @weather_etl_stage/forecast_day_weather/;



CREATE OR REPLACE PROCEDURE forecast_day_weather_procedure()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        // Step 1: Get the initial count of rows in stage_test
        var count_query = 'SELECT COUNT(DISTINCT FORECAST_DAY_WEATHER_ID) AS count FROM FORECAST_DAY_STAGE;';
        var count_result = snowflake.execute({ sqlText: count_query });
        var initial_stage_count = count_result.next() ? count_result.getColumnValue(1) : 0;

        // Step 2: Merge operation
        var merge_query = `
            MERGE INTO FACT_FORECAST_DAY_WEATHER as target
            USING FORECAST_DAY_STAGE AS source
            ON target.FORECAST_DAY_WEATHER_ID = source.FORECAST_DAY_WEATHER_ID
            WHEN MATCHED THEN
                UPDATE 
                    SET target.LOCATION_ID = source.LOCATION_ID,
                        target.CONDITION_CODE= source.CONDITION_CODE,
                        target.FORECAST_DATE= source.FORECAST_DATE,
                        target.MAX_TEMP_C= source.MAX_TEMP_C,
                        target.AVG_TEMP_C= source.AVG_TEMP_C,
                        target.MIN_TEMP_C= source.MIN_TEMP_C,
                        target.MAX_WIND_KPH= source.MAX_WIND_KPH,
                        target.TOTAL_PRECIP_MM= source.TOTAL_PRECIP_MM,
                        target.TOTAL_SNOW_CM= source.TOTAL_SNOW_CM,
                        target.AVG_HUMIDITY= source.AVG_HUMIDITY,
                        target.DAILY_WILL_IT_RAIN= source.DAILY_WILL_IT_RAIN,
                        target.DAILY_CHANCE_OF_RAIN= source.DAILY_CHANCE_OF_RAIN,
                        target.DAILY_WILL_IT_SNOW= source.DAILY_WILL_IT_SNOW,
                        target.DAILY_CHANCE_OF_SNOW= source.DAILY_CHANCE_OF_SNOW,
                        target.UV = source.UV,
                        target.SUNRISE_TIME= source.SUNRISE_TIME,
                        target.SUNSET_TIME = source.SUNSET_TIME,
                        target.MOONRISE_TIME = source.MOONRISE_TIME,
                        target.MOONSET_TIME= source.MOONSET_TIME
                WHEN NOT MATCHED THEN
                    INSERT (target.FORECAST_DAY_WEATHER_ID,target.LOCATION_ID,target.CONDITION_CODE,target.FORECAST_DATE,target.MAX_TEMP_C,target.AVG_TEMP_C,target.MIN_TEMP_C,target.MAX_WIND_KPH,target.TOTAL_PRECIP_MM,target.TOTAL_SNOW_CM,target.AVG_HUMIDITY,target.DAILY_WILL_IT_RAIN,target.DAILY_CHANCE_OF_RAIN,target.DAILY_WILL_IT_SNOW,target.DAILY_CHANCE_OF_SNOW,target.UV,target.SUNRISE_TIME,target.SUNSET_TIME,target.MOONRISE_TIME,target.MOONSET_TIME)
                    VALUES (source.FORECAST_DAY_WEATHER_ID,source.LOCATION_ID,source.CONDITION_CODE,source.FORECAST_DATE,source.MAX_TEMP_C,source.AVG_TEMP_C,source.MIN_TEMP_C,source.MAX_WIND_KPH,source.TOTAL_PRECIP_MM,source.TOTAL_SNOW_CM,source.AVG_HUMIDITY,source.DAILY_WILL_IT_RAIN,source.DAILY_CHANCE_OF_RAIN,source.DAILY_WILL_IT_SNOW,source.DAILY_CHANCE_OF_SNOW,source.UV,source.SUNRISE_TIME,source.SUNSET_TIME,source.MOONRISE_TIME,source.MOONSET_TIME);
        `;
        // Execute the merge query
        snowflake.execute({ sqlText: merge_query });

        // Step 3: Get the number of rows inserted into target_test
        var inserted_query = `
            SELECT COUNT(DISTINCT FORECAST_DAY_WEATHER_ID) AS count 
            FROM FACT_FORECAST_DAY_WEATHER
            WHERE FORECAST_DAY_WEATHER_ID IN (SELECT DISTINCT FORECAST_DAY_WEATHER_ID FROM FORECAST_DAY_STAGE);
        `;
        var inserted_result = snowflake.execute({ sqlText: inserted_query });
        var inserted_count = inserted_result.next() ? inserted_result.getColumnValue(1) : 0;

        // Step 4: Check if any new rows were successfully inserted
        if (inserted_count === initial_stage_count) {
            // All rows were inserted, truncate stage_test
            var truncate_query = 'TRUNCATE TABLE FORECAST_DAY_STAGE;';
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



CREATE OR REPLACE TASK forecast_day_weather_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */4 * * * UTC'  -- This runs the task every 4hr
AS
CALL forecast_day_weather_procedure();