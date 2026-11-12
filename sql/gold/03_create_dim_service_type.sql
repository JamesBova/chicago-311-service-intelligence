-- One row per distinct (SR_SHORT_CODE, SR_TYPE) combination.
-- Populate SERVICE_TYPE_KEY deterministically in dbt (e.g. a stable hash).
-- Use a documented Unknown member for missing service-type values.

CREATE TABLE IF NOT EXISTS CHICAGO_311_DEV.GOLD.DIM_SERVICE_TYPE (
    SERVICE_TYPE_KEY        VARCHAR(64) NOT NULL,
    SR_SHORT_CODE           VARCHAR(50),
    SR_TYPE                 VARCHAR(255),
    LAST_REFRESHED_AT       TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIM_SERVICE_TYPE PRIMARY KEY (SERVICE_TYPE_KEY)
)
COMMENT = 'Conformed Chicago 311 service request categories';
