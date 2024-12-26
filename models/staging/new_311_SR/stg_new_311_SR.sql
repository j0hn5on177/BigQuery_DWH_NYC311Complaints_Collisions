{{ config(materialized='view') }}

SELECT
    CAST(zip AS INT) AS zip,
    address,
    street_name,
    Borough,
    cross_street1,
    cross_street2,
    intersection1,
    intersection2,
    resolution_description,
    address_type,
    location_type,
    facility_type,
    open_channel_type,
    vehicle_type,
    CAST(created_date AS DATETIME) AS created_date,
    CAST(closed_date AS DATETIME) AS closed_date,
    PARSE_DATETIME('%m/%d/%Y %H:%M', due_date) AS due_date,
    CAST(resolution_action_updated_date AS DATETIME) AS resolution_action_updated_date,
    City,
    descriptor AS description,
    Latitude AS latitude,
    Longitude AS longitude,
    Location AS location
FROM {{ source('311_SR', 'new_311_SR') }}