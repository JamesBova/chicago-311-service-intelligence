-- Chicago 311 Service Intelligence: Silver schema
-- Run with a Snowflake role authorized to create schemas in CHICAGO_311_DEV.
-- Does not modify the existing BRONZE schema or objects.

CREATE SCHEMA IF NOT EXISTS CHICAGO_311_DEV.SILVER
    COMMENT = 'Cleaned and standardized current-state Chicago 311 service requests';
