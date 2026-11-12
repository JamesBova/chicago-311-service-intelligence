-- dbt singular test: zero rows = pass

WITH fact_counts AS (
    SELECT
        COUNT_IF(CREATED_DATE IS NOT NULL) AS FACT_CREATED,
        COUNT_IF(CLOSED_DATE IS NOT NULL) AS FACT_CLOSED
    FROM {{ ref('fact_service_request') }}
),

metric_counts AS (
    SELECT
        COALESCE(SUM(CREATED_REQUESTS), 0) AS METRIC_CREATED,
        COALESCE(SUM(CLOSED_REQUESTS), 0) AS METRIC_CLOSED
    FROM {{ ref('daily_request_metrics') }}
)

SELECT
    f.FACT_CREATED,
    m.METRIC_CREATED,
    f.FACT_CLOSED,
    m.METRIC_CLOSED
FROM fact_counts f
CROSS JOIN metric_counts m
WHERE f.FACT_CREATED <> m.METRIC_CREATED
   OR f.FACT_CLOSED <> m.METRIC_CLOSED