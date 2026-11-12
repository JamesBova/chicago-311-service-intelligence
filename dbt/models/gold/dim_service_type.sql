{{ 
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='SERVICE_TYPE_KEY',
        on_schema_change='fail',
        tags=['chicago-311', 'gold']
    )
}}

WITH service_types AS (
    SELECT DISTINCT
        SR_SHORT_CODE,
        SR_TYPE
    FROM {{ ref('service_request_current') }}
    WHERE SR_SHORT_CODE IS NOT NULL
       OR SR_TYPE IS NOT NULL
)

SELECT
    CAST(
        MD5(
            COALESCE(SR_SHORT_CODE, '<NULL>') || '|' ||
            COALESCE(SR_TYPE, '<NULL>')
        ) AS VARCHAR(64)
    ) AS SERVICE_TYPE_KEY,
    CAST(SR_SHORT_CODE AS VARCHAR(50)) AS SR_SHORT_CODE,
    CAST(SR_TYPE AS VARCHAR(255)) AS SR_TYPE,
    CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_LTZ) AS LAST_REFRESHED_AT
FROM service_types