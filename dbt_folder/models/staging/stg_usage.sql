{{ config(
    materialized='table'
) }}

WITH limit_modes AS (
    SELECT
        CURRENT_PLAN,
        LIMIT,
        COUNT(*) AS limit_count,
        ROW_NUMBER() OVER (PARTITION BY CURRENT_PLAN ORDER BY COUNT(*) DESC, LIMIT) AS rn
    FROM {{ source('SNOWFLAKE_AIRFLOW_DBT', 'RAW_USERS') }}
    WHERE LIMIT IS NOT NULL AND LIMIT != 0
    GROUP BY CURRENT_PLAN, LIMIT
),
most_common_limit AS (
    SELECT
        CURRENT_PLAN,
        LIMIT AS most_common_limit
    FROM limit_modes
    WHERE rn = 1
),
replaced_limits AS (
    SELECT
        ru.*,
        COALESCE(NULLIF(ru.LIMIT, 0), mcl.most_common_limit, ru.USAGE) AS updated_limit
    FROM {{ source('SNOWFLAKE_AIRFLOW_DBT', 'RAW_USERS') }} ru
    LEFT JOIN most_common_limit mcl
    ON ru.CURRENT_PLAN = mcl.CURRENT_PLAN
)

SELECT
    UID,
    NAME,
    EMAIL,
    CURRENT_PLAN,
    USAGE,
    TOTAL_USAGE,
    updated_limit AS LIMIT,
FROM replaced_limits
