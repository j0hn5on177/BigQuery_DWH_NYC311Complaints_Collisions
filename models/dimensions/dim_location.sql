{{ config(materialized='table') }}

WITH distinct_locations AS (
    SELECT DISTINCT
        borough,
        zip,
        address,
        street_name,
        cross_street1,
        cross_street2,
        intersection_street1,
        intersection_street2,
        latitude,
        longitude
    FROM (
        SELECT
            BOROUGH AS borough,
            zip,
            NULL AS address,
            street_name,
            cross_street1,
            NULL AS cross_street2,
            NULL AS intersection_street1,
            NULL AS intersection_street2,
            latitude,
            longitude
        FROM {{ ref('stg_motor') }}
        UNION ALL
        SELECT
            Borough AS borough,
            zip,
            address,
            street_name,
            cross_street1,
            cross_street2,
            intersection1 AS intersection_street1,
            intersection2 AS intersection_street2,
            latitude,
            longitude
        FROM {{ ref('stg_new_311_SR') }}
    ) combined_locations
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY borough, zip, address, street_name, cross_street1, cross_street2, intersection_street1, intersection_street2, latitude, longitude
    ) AS location_key,
    borough,
    zip,
    address,
    street_name,
    cross_street1,
    cross_street2,
    intersection_street1,
    intersection_street2,
    latitude,
    longitude
FROM distinct_locations