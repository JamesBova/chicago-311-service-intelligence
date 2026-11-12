-- Grain: ONE ROW PER CURRENT SERVICE REQUEST (SR_NUMBER).
-- This is a current-state fact, not a historical daily backlog snapshot.
-- RESOLUTION_HOURS derives from CLOSED_DATE minus CREATED_DATE when valid.
-- Status / closure classification will be defined in transformation logic.
-- Coordinates remain at request grain for future mapping.

CREATE TABLE IF NOT EXISTS CHICAGO_311_DEV.GOLD.FACT_SERVICE_REQUEST (
    SR_NUMBER               VARCHAR(50) NOT NULL,
    SERVICE_TYPE_KEY        VARCHAR(64),
    GEOGRAPHY_KEY           VARCHAR(64),
    CREATED_DATE_KEY        NUMBER(8, 0),
    CLOSED_DATE_KEY         NUMBER(8, 0),
    CREATED_DATE            TIMESTAMP_NTZ(9),
    LAST_MODIFIED_DATE      TIMESTAMP_NTZ(9),
    CLOSED_DATE             TIMESTAMP_NTZ(9),
    STATUS                  VARCHAR(100),
    OWNER_DEPARTMENT        VARCHAR(255),
    IS_CLOSED               BOOLEAN,
    RESOLUTION_HOURS        NUMBER(18, 2),
    LATITUDE                FLOAT,
    LONGITUDE               FLOAT,
    LAST_REFRESHED_AT       TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_FACT_SERVICE_REQUEST PRIMARY KEY (SR_NUMBER)
)
COMMENT = 'Current status and lifecycle measurements for each Chicago 311 request';
