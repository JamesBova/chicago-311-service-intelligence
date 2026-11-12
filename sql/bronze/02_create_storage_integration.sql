-- Chicago 311 Service Intelligence
-- Configure Snowflake access to AWS S3.

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS S3_CHICAGO_311_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN =
        'arn:aws:iam::690260957255:role/CHICAGO_311_SNOWFLAKE_ROLE'
    STORAGE_ALLOWED_LOCATIONS = (
        's3://james-chicago-311-service-intelligence/dev/raw/',
        's3://james-chicago-311-service-intelligence/dev/manifests/'
    );

-- Retrieve the Snowflake IAM identity and external ID.
DESC INTEGRATION S3_CHICAGO_311_INT;