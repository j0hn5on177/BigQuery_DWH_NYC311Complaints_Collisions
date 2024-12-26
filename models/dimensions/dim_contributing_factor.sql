{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER () AS factor_key,
    contributing_factor_vehicle1,
    contributing_factor_vehicle2,
    contributing_factor_vehicle3,
    contributing_factor_vehicle4,
    contributing_factor_vehicle5
FROM {{ ref('stg_motor') }}
GROUP BY
    contributing_factor_vehicle1,
    contributing_factor_vehicle2,
    contributing_factor_vehicle3,
    contributing_factor_vehicle4,
    contributing_factor_vehicle5