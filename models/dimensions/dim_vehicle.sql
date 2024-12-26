{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER () AS vehicle_key,
    vehicle_type_1,
    vehicle_type_2,
    vehicle_type_3,
    vehicle_type_4,
    vehicle_type_5
FROM {{ ref('stg_motor') }}
GROUP BY
    vehicle_type_1, vehicle_type_2, vehicle_type_3, vehicle_type_4, vehicle_type_5