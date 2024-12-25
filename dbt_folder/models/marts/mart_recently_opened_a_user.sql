{{ config(
    materialized='table'
) }}

SELECT
    UID,
    NAME,
    EMAIL,
    CURRENT_PLAN,
    USAGE,
    LIMIT,
    USAGE / LIMIT AS PLAN_UTILIZATION,
FROM {{ref('stg_usage')}}
WHERE PLAN_UTILIZATION > 0.9 AND CURRENT_PLAN != 'Enterprise'

ORDER BY PLAN_UTILIZATION