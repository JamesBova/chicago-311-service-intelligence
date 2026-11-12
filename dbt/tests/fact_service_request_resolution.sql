-- dbt test: returns rows that violate resolution rules.
-- Zero returned rows means the test passes.

SELECT
    SR_NUMBER,
    CREATED_DATE,
    CLOSED_DATE,
    RESOLUTION_HOURS
FROM {{ ref('fact_service_request') }}
WHERE
    (CLOSED_DATE IS NOT NULL AND RESOLUTION_HOURS IS NULL)
    OR RESOLUTION_HOURS < 0
    OR (CLOSED_DATE IS NULL AND RESOLUTION_HOURS IS NOT NULL)