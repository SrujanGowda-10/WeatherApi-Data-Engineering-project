CREATE OR REPLACE TABLE LOCATION_STAGE(
    LOCATION_ID VARCHAR(255),
    NAME VARCHAR(255),
    REGION VARCHAR(255),
    COUNTRY VARCHAR(255),
    LATITUDE VARCHAR(255),
    LONGITUDE VARCHAR(255)
);


CREATE OR REPLACE TABLE DIM_LOCATION(
    LOCATION_ID VARCHAR(255) PRIMARY KEY,
    NAME VARCHAR(255),
    REGION VARCHAR(255),
    COUNTRY VARCHAR(255),
    LATITUDE VARCHAR(255),
    LONGITUDE VARCHAR(255)
);


--SNOWPIPE-----
CREATE OR REPLACE PIPE location_pipe
AUTO_INGEST = TRUE
AS
COPY INTO WEATHER_ETL_DB.PROJECT_SCHEMA.LOCATION_STAGE
FROM @weather_etl_stage/location/;


---PROCEDURE------------
CREATE OR REPLACE PROCEDURE location_procedure()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        // Step 1: Get the initial count of rows in stage
        var count_query = 'SELECT COUNT(DISTINCT LOCATION_ID) AS count FROM LOCATION_STAGE;';
        var count_result = snowflake.execute({ sqlText: count_query });
        var initial_stage_count = count_result.next() ? count_result.getColumnValue(1) : 0;

        // Step 2: Merge operation
        var merge_query = `
            MERGE INTO DIM_LOCATION as target
            USING LOCATION_STAGE AS source
            ON target.LOCATION_ID = source.LOCATION_ID
            WHEN MATCHED THEN
                UPDATE 
                    SET target.NAME = source.NAME,
                        target.REGION = source.REGION,
                        target.COUNTRY = source.COUNTRY,
                        target.LATITUDE = source.LATITUDE,
                        target.LONGITUDE = source.LONGITUDE
                WHEN NOT MATCHED THEN
                    INSERT (target.LOCATION_ID,target.NAME,target.REGION,target.COUNTRY,target.LATITUDE,target.LONGITUDE)
                    VALUES (source.LOCATION_ID,source.NAME, source.REGION,source.COUNTRY,source.LATITUDE,source.LONGITUDE);
        `;
        // Execute the merge query
        snowflake.execute({ sqlText: merge_query });

        // Step 3: Get the number of rows inserted into target
        var inserted_query = `
            SELECT COUNT(DISTINCT LOCATION_ID) AS count 
            FROM DIM_LOCATION
            WHERE LOCATION_ID IN (SELECT LOCATION_ID FROM LOCATION_STAGE);
        `;
        var inserted_result = snowflake.execute({ sqlText: inserted_query });
        var inserted_count = inserted_result.next() ? inserted_result.getColumnValue(1) : 0;

        // Step 4: Check if any new rows were successfully inserted
        if (inserted_count === initial_stage_count) {
            // All rows were inserted, truncate stage_test
            var truncate_query = 'TRUNCATE TABLE LOCATION_STAGE;';
            snowflake.execute({ sqlText: truncate_query });
            return 'All rows inserted successfully, stage truncated.';
        } else {
            return `Merge completed, but not all rows were inserted. stage_table retained. 
                    Inserted rows: ${inserted_count}, Stage rows: ${initial_stage_count}`;
        }

    } catch (err) {
        return 'Error occurred: ' + err.message;
    }
$$;


CREATE OR REPLACE TASK location_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */4 * * * UTC'  -- This runs the task every 4hr
AS
CALL location_procedure();