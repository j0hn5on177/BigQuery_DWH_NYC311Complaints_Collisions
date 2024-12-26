{{ config(materialized='table') }}

WITH distinct_descriptors AS (
    SELECT DISTINCT description,
        open_channel_type
    FROM {{ ref('stg_new_311_SR') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY description, open_channel_type) AS descriptor_key,
    description,
    open_channel_type
FROM distinct_descriptors