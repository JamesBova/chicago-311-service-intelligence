-- Grain: METRIC_DATE + SERVICE_TYPE_KEY + GEOGRAPHY_KEY.
-- CREATED_REQUESTS counts requests created on METRIC_DATE.
-- CLOSED_REQUESTS counts requests closed on METRIC_DATE, regardless of creation date.
-- AVG_CLOSED_RESOLUTION_HOURS averages lifetimes of requests closed that date.
-- Do NOT treat this table as a historical open-backlog snapshot.

CREATE TABLE IF NOT EXISTS CHICAGO_311_DEV.GOLD.DAILY_REQUEST_METRICS (
    METRIC_DATE                  DATE NOT NULL,
    SERVICE_TYPE_KEY             VARCHAR(64) NOT NULL,
    GEOGRAPHY_KEY                VARCHAR(64) NOT NULL,
    CREATED_REQUESTS             NUMBER(38, 0) DEFAULT 0 NOT NULL,
    CLOSED_REQUESTS              NUMBER(38, 0) DEFAULT 0 NOT NULL,
    AVG_CLOSED_RESOLUTION_HOURS  NUMBER(18, 2),
    LAST_REFRESHED_AT            TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DAILY_REQUEST_METRICS PRIMARY KEY
        (METRIC_DATE, SERVICE_TYPE_KEY, GEOGRAPHY_KEY)
)
COMMENT = 'Daily created/closed 311 activity by category and geography';
