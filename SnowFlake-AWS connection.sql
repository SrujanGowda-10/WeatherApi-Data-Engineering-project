create storage integration weather_etl_stg_integration
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'arn:aws:iam::396913696969:role/weather-etl-snowflake-connection'
    enabled = true
    storage_allowed_locations = ('s3://weather-etl-snowflake-stage-bucket')
    comment = 'Storage integration for weather-etl project';


DESC STORAGE INTEGRATION weather_etl_stg_integration; --- Take STORAGE_AWS_EXTERNAL_ID and STORAGE_AWS_ROLE_ARN(SF IAM USER) to edit ExternalId and AWS in TrustRelationship of AWS IAM Role

-- SHOW STORAGE INTEGRATIONS;

CREATE FILE FORMAT format_for_csv
    TYPE = CSV
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;

-- DESC FILE FORMAT format_for_csv

CREATE OR REPLACE STAGE weather_etl_stage
    URL = "s3://weather-etl-snowflake-stage-bucket"
    STORAGE_INTEGRATION = weather_etl_stg_integration
    FILE_FORMAT = format_for_csv;