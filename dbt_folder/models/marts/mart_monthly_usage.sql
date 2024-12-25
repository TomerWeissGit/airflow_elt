{{ config(
    materialized='incremental',
    unique_key='MONTH'
) }}

WITH extracted_day AS (
    SELECT
        CURRENT_PLAN,
        USAGE / LIMIT AS UTILIZATION,
        USAGE,
        TOTAL_USAGE AS TOTAL_USAGE,
        TO_CHAR(DATE_TRUNC('MONTH', CURRENT_DATE), 'YYYY-MM') AS MONTH
    FROM {{ref('stg_usage')}}
)

SELECT
    CURRENT_PLAN,
    MONTH,
    SUM(USAGE) AS MONTHLY_USAGE,
    SUM(TOTAL_USAGE) AS TOTAL_USAGE_LAST_EXCLUDED,
    SUM(USAGE) + SUM(TOTAL_USAGE) AS TOTAL_USAGE,
    AVG(UTILIZATION) AS AVERAGE_PLAN_UTILIZATION


FROM extracted_day
{% if is_incremental() %}
WHERE MONTH > (SELECT MAX(MONTH) FROM {{ this }})
{% endif %}
GROUP BY CURRENT_PLAN, MONTH
ORDER BY MONTH, CURRENT_PLAN