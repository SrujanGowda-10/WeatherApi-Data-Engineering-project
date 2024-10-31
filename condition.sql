CREATE OR REPLACE TABLE CONDITION_STAGE(
    CONDITION_CODE VARCHAR(255),
    CONDITION_NAME VARCHAR(255)
);

CREATE OR REPLACE TABLE DIM_CONDITION(
    CONDITION_CODE VARCHAR(255) PRIMARY KEY,
    CONDITION_NAME VARCHAR(255)
);


----SNOWPIPE---------
CREATE OR REPLACE PIPE condition_pipe
AUTO_INGEST = TRUE
AS
COPY INTO WEATHER_ETL_DB.PROJECT_SCHEMA.CONDITION_STAGE
FROM @weather_etl_stage/condition/;


--stored procedure for merge and truncate 
CREATE OR REPLACE PROCEDURE condition_procedure()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    try {
        // Step 1: Get the initial count of rows in stage_test
        var count_query = 'SELECT COUNT(DISTINCT CONDITION_CODE) AS count FROM CONDITION_STAGE;';
        var count_result = snowflake.execute({ sqlText: count_query });
        var initial_stage_count = count_result.next() ? count_result.getColumnValue(1) : 0;

        // Step 2: Create a temporary table with deduplicated data in stage
        var dedup_query = `
            CREATE OR REPLACE TEMPORARY TABLE DEDUPLICATED_STAGE AS 
            SELECT DISTINCT CONDITION_CODE, CONDITION_NAME
            FROM CONDITION_STAGE;
        `;
        snowflake.execute({ sqlText: dedup_query });

        // Step 2: Create a temporary table with duplicated data
        var temp_query = `
            CREATE OR REPLACE TEMPORARY TABLE TEMP_CONDITION (
            CONDITION_CODE VARCHAR(255),
            CONDITION_NAME VARCHAR(255)
            );
        `;
        snowflake.execute({ sqlText: temp_query });

        // Step 3: Merge operation
        var merge_query = `
            MERGE INTO TEMP_CONDITION as target
            USING DEDUPLICATED_STAGE AS source
            ON target.CONDITION_CODE = source.CONDITION_CODE
            WHEN MATCHED THEN
                UPDATE 
                    SET target.CONDITION_NAME = CASE 
                        WHEN source.CONDITION_CODE = '1000' THEN 'Sunny'
                        ELSE source.CONDITION_NAME 
                    END
                WHEN NOT MATCHED THEN
                    INSERT (target.CONDITION_CODE,target.CONDITION_NAME)
                    VALUES (source.CONDITION_CODE, CASE 
                    WHEN source.CONDITION_CODE = '1000' THEN 'Sunny'
                    ELSE source.CONDITION_NAME 
                END);
        `;
        // Execute the merge query
        snowflake.execute({ sqlText: merge_query });
        

        // Step 2: Insert unique records into dim table
        var insert_query = `
            INSERT INTO DIM_CONDITION (CONDITION_CODE, CONDITION_NAME)
            SELECT DISTINCT CONDITION_CODE, CONDITION_NAME
            FROM TEMP_CONDITION
            WHERE CONDITION_CODE NOT IN (SELECT DISTINCT CONDITION_CODE FROM DIM_CONDITION);
        `;
        snowflake.execute({ sqlText: insert_query });
        

        // Step 4: Get the number of rows inserted into target_test
        var inserted_query = `
            SELECT COUNT(DISTINCT CONDITION_CODE) AS count 
            FROM DIM_CONDITION
            WHERE CONDITION_CODE IN (SELECT DISTINCT CONDITION_CODE FROM CONDITION_STAGE);
        `;
        var inserted_result = snowflake.execute({ sqlText: inserted_query });
        var inserted_count = inserted_result.next() ? inserted_result.getColumnValue(1) : 0;

        // Step 5: Check if any new rows were successfully inserted
        if (inserted_count === initial_stage_count) {
            // All rows were inserted, truncate stage_test
            var truncate_query = 'TRUNCATE TABLE CONDITION_STAGE;';
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




CREATE OR REPLACE TASK condition_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */4 * * * UTC'  -- This runs the task every 4hr
AS
CALL condition_procedure();