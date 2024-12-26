{{ config(materialized='view') }}

WITH closed_dates AS (
    SELECT DISTINCT closed_date
    FROM {{ ref('stg_new_311_SR') }}
    WHERE closed_date IS NOT NULL
),
unique_closed_dates AS (
    SELECT DISTINCT closed_date
    FROM closed_dates
),
numbered_dates AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY closed_date) AS closedate_key,
        closed_date
    FROM unique_closed_dates
)

SELECT
    nd.closedate_key,
    nd.closed_date AS closedate,
    dt.hour AS close_hour,
    dt.day AS close_day,
    dt.month AS close_month,
    dt.year AS close_year,
    dt.year_quarter AS close_year_quarter
FROM numbered_dates nd
JOIN (
    SELECT DISTINCT datetime, hour, day, month, year, year_quarter
    FROM {{ ref('dim_datetime') }}
) dt
    ON nd.closed_date = dt.datetime 
ORDER BY closedate_key