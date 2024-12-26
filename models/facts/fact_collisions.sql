{{ config(materialized='table') }}

WITH fact_collision AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY m.crash_datetime) AS factcollision_key,
        dt.datetime_key,
        l.location_key,
        cf.factor_key,
        v.vehicle_key,
        m.persons_injured,
        m.persons_killed,
        m.pedestrians_injured,
        m.pedestrians_killed,
        m.cyclists_injured,
        m.cyclists_killed,
        m.motorists_injured,
        m.motorists_killed
    FROM {{ ref('stg_motor') }} m

    LEFT JOIN {{ ref('dim_datetime') }} dt
        ON m.crash_datetime = dt.datetime
    LEFT JOIN {{ ref('dim_location') }} l
        ON m.borough = l.borough
        AND m.zip = l.zip
        AND m.street_name = l.street_name
    LEFT JOIN {{ ref('dim_contributing_factor') }} cf
        ON m.contributing_factor_vehicle1 = cf.contributing_factor_vehicle1
        AND m.contributing_factor_vehicle2 = cf.contributing_factor_vehicle2
        AND m.contributing_factor_vehicle3 = cf.contributing_factor_vehicle3
        AND m.contributing_factor_vehicle4 = cf.contributing_factor_vehicle4
        AND m.contributing_factor_vehicle5 = cf.contributing_factor_vehicle5
    LEFT JOIN {{ ref('dim_vehicle') }} v
        ON m.vehicle_type_1 = v.vehicle_type_1
        AND m.vehicle_type_2 = v.vehicle_type_2
        AND m.vehicle_type_3 = v.vehicle_type_3
        AND m.vehicle_type_4 = v.vehicle_type_4
        AND m.vehicle_type_5 = v.vehicle_type_5
)

SELECT
    factcollision_key,
    datetime_key,
    location_key,
    factor_key,
    vehicle_key,
    persons_injured,
    persons_killed,
    pedestrians_injured,
    pedestrians_killed,
    cyclists_injured,
    cyclists_killed,
    motorists_injured,
    motorists_killed
FROM fact_collision
ORDER BY factcollision_key