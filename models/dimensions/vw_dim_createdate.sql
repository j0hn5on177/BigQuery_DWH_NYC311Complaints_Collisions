{{ config(materialized='view') }}

WITH created_dates AS (
    SELECT DISTINCT created_date
    FROM {{ ref('stg_new_311_SR') }}
    WHERE created_date IS NOT NULL
),
numbered_dates AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY created_date) AS createdate_key,
        created_date
    FROM created_dates
),
unique_dim_datetime AS (
    SELECT DISTINCT datetime, hour, day, month, year, year_quarter
    FROM {{ ref('dim_datetime') }}
)

SELECT
    nd.createdate_key,
    nd.created_date AS createdate,
    dt.hour AS create_hour,
    dt.day AS create_day,
    dt.month AS create_month,
    dt.year AS create_year,
    dt.year_quarter AS create_year_quarter
FROM numbered_dates nd
JOIN unique_dim_datetime dt
    ON nd.created_date = dt.datetime
ORDER BY createdate_key