{{ config(materialized='table') }}

WITH combined_datetimes AS (
    SELECT
        CAST(crash_datetime AS DATETIME) AS event_datetime
    FROM {{ ref('stg_motor') }}
    
    UNION ALL

    SELECT
        CAST(created_date AS DATETIME) AS event_datetime
    FROM {{ ref('stg_new_311_SR') }}

    UNION ALL

    SELECT
        CAST(closed_date AS DATETIME) AS event_datetime
    FROM {{ ref('stg_new_311_SR') }}

    UNION ALL

    SELECT
        CAST(due_date AS DATETIME) AS event_datetime
    FROM {{ ref('stg_new_311_SR') }}

    UNION ALL

    SELECT
        CAST(resolution_action_updated_date AS DATETIME) AS event_datetime
    FROM {{ ref('stg_new_311_SR') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY event_datetime) AS datetime_key,
    event_datetime AS datetime,
    EXTRACT(HOUR FROM event_datetime) AS hour,
    EXTRACT(DAY FROM event_datetime) AS day,
    EXTRACT(MONTH FROM event_datetime) AS month,
    EXTRACT(YEAR FROM event_datetime) AS year,
    EXTRACT(QUARTER FROM event_datetime) AS year_quarter
FROM combined_datetimes