-- One current version of each 311 service request (SR_NUMBER).
-- Future incremental transform: choose the most recent LAST_MODIFIED_DATE
-- for each SR_NUMBER, with ingestion metadata as a deterministic tie-breaker.
-- Source date/time strings have no explicit offset: retain as TIMESTAMP_NTZ.
-- Populate from a validated Bronze handoff; do not read an already-consumed stream.

CREATE TABLE IF NOT EXISTS CHICAGO_311_DEV.SILVER.SERVICE_REQUEST_CURRENT (
    SR_NUMBER               VARCHAR(50) NOT NULL,
    SR_TYPE                 VARCHAR(255),
    SR_SHORT_CODE           VARCHAR(50),
    CREATED_DEPARTMENT      VARCHAR(255),
    OWNER_DEPARTMENT        VARCHAR(255),
    STATUS                  VARCHAR(100),
    ORIGIN                  VARCHAR(100),
    CREATED_DATE            TIMESTAMP_NTZ(9),
    LAST_MODIFIED_DATE      TIMESTAMP_NTZ(9),
    CLOSED_DATE             TIMESTAMP_NTZ(9),
    STREET_ADDRESS          VARCHAR(500),
    CITY                    VARCHAR(100),
    STATE                   VARCHAR(20),
    ZIP_CODE                VARCHAR(20),
    WARD                    NUMBER(5, 0),
    COMMUNITY_AREA          NUMBER(5, 0),
    POLICE_DISTRICT         NUMBER(5, 0),
    LATITUDE                FLOAT,
    LONGITUDE               FLOAT,
    PARENT_SR_NUMBER        VARCHAR(50),
    IS_DUPLICATE            BOOLEAN,
    IS_LEGACY_RECORD        BOOLEAN,
    SOURCE_FILE             VARCHAR(1024),
    SOURCE_ROW_NUMBER       NUMBER(38, 0),
    SOURCE_INGESTED_AT      TIMESTAMP_LTZ(9),
    SILVER_UPDATED_AT       TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_SERVICE_REQUEST_CURRENT PRIMARY KEY (SR_NUMBER)
)
COMMENT = 'Latest standardized record per Chicago 311 service request';
