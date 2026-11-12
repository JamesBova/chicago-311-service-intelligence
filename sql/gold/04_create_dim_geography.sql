-- One row per distinct combination of area, ward, police district, ZIP.
-- Populate GEOGRAPHY_KEY deterministically in dbt; Unknown for absent data.
-- Geography attributes can be absent or inconsistent in 311 source records.
-- Avoid placing street addresses or coordinates in this low-cardinality dimension.

CREATE TABLE IF NOT EXISTS CHICAGO_311_DEV.GOLD.DIM_GEOGRAPHY (
    GEOGRAPHY_KEY           VARCHAR(64) NOT NULL,
    COMMUNITY_AREA          NUMBER(5, 0),
    WARD                    NUMBER(5, 0),
    POLICE_DISTRICT         NUMBER(5, 0),
    ZIP_CODE                VARCHAR(20),
    LAST_REFRESHED_AT       TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DIM_GEOGRAPHY PRIMARY KEY (GEOGRAPHY_KEY)
)
COMMENT = 'Chicago 311 reporting geography, by area/ward/district/ZIP combination';
