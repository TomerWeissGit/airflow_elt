{{ config(
    materialized='incremental',
    unique_key='MONTH'
) }}

WITH extracted_day AS (
    SELECT
        CURRENT_PLAN,
        TO_CHAR(DATE_TRUNC('MONTH', CURRENT_DATE), 'YYYY-MM') AS MONTH,
        PAYMENT_STATUS
    FROM {{source('SNOWFLAKE_AIRFLOW_DBT', 'RAW_USERS')}}
),

aggregated_data AS (
    SELECT
        CURRENT_PLAN,
        MONTH,
        COUNT(*) AS OVERALL_SUBSCRIPTIONS,
        SUM(CASE WHEN PAYMENT_STATUS = 'paid' THEN 1 ELSE 0 END) AS PAID_SUB,
        SUM(CASE WHEN PAYMENT_STATUS = 'canceled' THEN 1 ELSE 0 END) AS CANCELED_SUB
    FROM extracted_day
    {% if is_incremental() %}
    WHERE MONTH > (SELECT MAX(MONTH) FROM {{ this }})
    {% endif %}
    GROUP BY CURRENT_PLAN, MONTH
)

SELECT
    CURRENT_PLAN,
    MONTH,
    OVERALL_SUBSCRIPTIONS,
    PAID_SUB,
    CANCELED_SUB
FROM aggregated_data
ORDER BY MONTH, CURRENT_PLAN
