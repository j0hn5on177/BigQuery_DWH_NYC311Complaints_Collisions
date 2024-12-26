{{ config(materialized='view') }}

WITH due_dates AS (
    SELECT DISTINCT due_date
    FROM {{ ref('stg_new_311_SR') }}
    WHERE due_date IS NOT NULL
),
numbered_dates AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY due_date) AS duedate_key,
        due_date
    FROM due_dates
),
unique_dim_datetime AS (
    SELECT DISTINCT datetime, hour, day, month, year, year_quarter
    FROM {{ ref('dim_datetime') }}
)

SELECT
    nd.duedate_key,
    nd.due_date AS duedate,
    dt.hour AS due_hour,
    dt.day AS due_day,
    dt.month AS due_month,
    dt.year AS due_year,
    dt.year_quarter AS due_year_quarter
FROM numbered_dates nd
JOIN unique_dim_datetime dt
    ON nd.due_date = dt.datetime
ORDER BY duedate_key