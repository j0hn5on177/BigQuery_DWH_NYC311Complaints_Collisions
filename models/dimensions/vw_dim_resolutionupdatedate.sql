{{ config(materialized='view') }}

WITH resolution_update_dates AS (
    SELECT DISTINCT resolution_action_updated_date
    FROM {{ ref('stg_new_311_SR') }}
    WHERE resolution_action_updated_date IS NOT NULL
),
numbered_dates AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY resolution_action_updated_date) AS updatedate_key,
        resolution_action_updated_date
    FROM resolution_update_dates
),
unique_dim_datetime AS (
    SELECT DISTINCT datetime, hour, day, month, year, year_quarter
    FROM {{ ref('dim_datetime') }}
)

SELECT
    nd.updatedate_key,
    nd.resolution_action_updated_date AS resolutionupdatedate,
    dt.hour AS resolution_update_hour,
    dt.day AS resolution_update_day,
    dt.month AS resolution_update_month,
    dt.year AS resolution_update_year,
    dt.year_quarter AS resolution_update_year_quarter
FROM numbered_dates nd
JOIN unique_dim_datetime dt
    ON nd.resolution_action_updated_date = dt.datetime
ORDER BY updatedate_key