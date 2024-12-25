{{ config(
    materialized='incremental',
    unique_key='MONTH'
) }}

SELECT
    UID,
    MANUAL_LIMIT,
    TO_CHAR(DATE_TRUNC('MONTH', CURRENT_DATE), 'YYYY-MM') AS MONTH
FROM {{ source('SNOWFLAKE_AIRFLOW_DBT', 'RAW_USERS') }}
WHERE MANUAL_LIMIT > 0
