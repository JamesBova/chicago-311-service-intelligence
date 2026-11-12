{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='SR_NUMBER',
        on_schema_change='fail',
        tags=['chicago-311', 'gold']
    )
}}

WITH requests AS (
    SELECT
        SR_NUMBER,
        SR_SHORT_CODE,
        SR_TYPE,
        COMMUNITY_AREA,
        WARD,
        POLICE_DISTRICT,
        CAST(ZIP_CODE AS VARCHAR(20)) AS ZIP_CODE,
        CREATED_DATE,
        LAST_MODIFIED_DATE,
        CLOSED_DATE,
        STATUS,
        OWNER_DEPARTMENT,
        LATITUDE,
        LONGITUDE
    FROM {{ ref('service_request_current') }}
    WHERE SR_NUMBER IS NOT NULL
)

SELECT
    CAST(SR_NUMBER AS VARCHAR(50)) AS SR_NUMBER,

    CASE
        WHEN SR_SHORT_CODE IS NULL AND SR_TYPE IS NULL
            THEN NULL
        ELSE CAST(
            MD5(
                COALESCE(SR_SHORT_CODE, '<NULL>') || '|' ||
                COALESCE(SR_TYPE, '<NULL>')
            ) AS VARCHAR(64)
        )
    END AS SERVICE_TYPE_KEY,

    CAST(
        MD5(
            COALESCE(TO_VARCHAR(COMMUNITY_AREA), '<NULL>') || '|' ||
            COALESCE(TO_VARCHAR(WARD), '<NULL>') || '|' ||
            COALESCE(TO_VARCHAR(POLICE_DISTRICT), '<NULL>') || '|' ||
            COALESCE(ZIP_CODE, '<NULL>')
        ) AS VARCHAR(64)
    ) AS GEOGRAPHY_KEY,

    CAST(
        TO_CHAR(CREATED_DATE, 'YYYYMMDD') AS NUMBER(8,0)
    ) AS CREATED_DATE_KEY,

    CAST(
        TO_CHAR(CLOSED_DATE, 'YYYYMMDD') AS NUMBER(8,0)
    ) AS CLOSED_DATE_KEY,

    CREATED_DATE,
    LAST_MODIFIED_DATE,
    CLOSED_DATE,

    CAST(STATUS AS VARCHAR(100)) AS STATUS,
    CAST(OWNER_DEPARTMENT AS VARCHAR(255)) AS OWNER_DEPARTMENT,

    CLOSED_DATE IS NOT NULL AS IS_CLOSED,

    CAST(
        CASE
            WHEN CLOSED_DATE >= CREATED_DATE
            THEN ROUND(
                DATEDIFF('second', CREATED_DATE, CLOSED_DATE) / 3600.0,
                2
            )
            ELSE NULL
        END AS NUMBER(18,2)
    ) AS RESOLUTION_HOURS,

    CAST(LATITUDE AS FLOAT) AS LATITUDE,
    CAST(LONGITUDE AS FLOAT) AS LONGITUDE,

    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_LTZ) AS LAST_REFRESHED_AT

FROM requests