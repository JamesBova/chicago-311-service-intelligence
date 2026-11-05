
-- Chicago 311 Service Intelligence
-- Create an external stage for raw JSONL files in S3.

USE ROLE ACCOUNTADMIN;

CREATE STAGE IF NOT EXISTS CHICAGO_311_DEV.BRONZE.S3_CHICAGO_311_STAGE
    URL = 's3://james-chicago-311-service-intelligence/dev/raw/'
    STORAGE_INTEGRATION = S3_CHICAGO_311_INT
    FILE_FORMAT = (
        FORMAT_NAME = 'CHICAGO_311_DEV.BRONZE.JSONL_FORMAT'
    );

-- Check which files Snowflake can see in S3.
LIST @CHICAGO_311_DEV.BRONZE.S3_CHICAGO_311_STAGE;
