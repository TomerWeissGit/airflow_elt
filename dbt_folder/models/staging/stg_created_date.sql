{{ config(
    materialized='table'
) }}

 SELECT
    UID,
    CURRENT_PLAN,
    TO_CHAR(DATE_TRUNC('DAY', TO_TIMESTAMP(CREATED)), 'YYYY-MM-DD') AS CREATED
FROM {{ source('SNOWFLAKE_AIRFLOW_DBT', 'RAW_USERS') }}