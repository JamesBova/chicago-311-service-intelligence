{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='GEOGRAPHY_KEY',
        on_schema_change='fail',
        tags=['chicago-311', 'gold']
    )
}}

WITH geography AS (
    SELECT DISTINCT
        TRY_CAST(COMMUNITY_AREA AS NUMBER(5,0)) AS COMMUNITY_AREA,
        TRY_CAST(WARD AS NUMBER(5,0)) AS WARD,
        TRY_CAST(POLICE_DISTRICT AS NUMBER(5,0)) AS POLICE_DISTRICT,
        CAST(ZIP_CODE AS VARCHAR(20)) AS ZIP_CODE
    FROM {{ ref('service_request_current') }}
)

SELECT
    CAST(
        MD5(
            COALESCE(TO_VARCHAR(COMMUNITY_AREA), '<NULL>') || '|' ||
            COALESCE(TO_VARCHAR(WARD), '<NULL>') || '|' ||
            COALESCE(TO_VARCHAR(POLICE_DISTRICT), '<NULL>') || '|' ||
            COALESCE(ZIP_CODE, '<NULL>')
        ) AS VARCHAR(64)
    ) AS GEOGRAPHY_KEY,
    COMMUNITY_AREA,
    WARD,
    POLICE_DISTRICT,
    ZIP_CODE,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_LTZ) AS LAST_REFRESHED_AT
FROM geography