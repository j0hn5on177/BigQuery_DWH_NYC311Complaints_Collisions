{{ config(materialized='table') }}

WITH complaint_source AS (
    SELECT
        s.created_date,
        s.due_date,
        s.closed_date,
        s.resolution_action_updated_date,
        s.zip,
        s.address,
        s.street_name,
        s.cross_street1,
        s.cross_street2,
        s.intersection1,
        s.intersection2,
        s.latitude,
        s.longitude,
        s.description,
        s.open_channel_type
    FROM {{ ref('stg_new_311_SR') }} s
),
fact_complaints AS (
    SELECT
        GENERATE_UUID() AS complaint_key,
        dt.datetime_key,
        loc.location_key,
        dsc.descriptor_key,
        cd.createdate_key,
        dd.duedate_key,
        cld.closedate_key,
        upd.updatedate_key
    FROM complaint_source cs
    LEFT JOIN {{ ref('dim_datetime') }} dt ON cs.created_date = dt.datetime
    LEFT JOIN {{ ref('dim_location') }} loc
        ON cs.zip = loc.zip
        AND cs.street_name = loc.street_name
        AND cs.cross_street1 = loc.cross_street1
    LEFT JOIN {{ ref('dim_complaint_description') }} dsc
        ON cs.description = dsc.description
        AND cs.open_channel_type = dsc.open_channel_type
    LEFT JOIN {{ ref('vw_dim_createdate') }} cd ON cs.created_date = cd.createdate
    LEFT JOIN {{ ref('vw_dim_duedate') }} dd ON cs.due_date = dd.duedate
    LEFT JOIN {{ ref('vw_dim_closedate') }} cld ON cs.closed_date = cld.closedate
    LEFT JOIN {{ ref('vw_dim_resolutionupdatedate') }} upd ON cs.resolution_action_updated_date = upd.resolutionupdatedate
)

SELECT *
FROM fact_complaints
ORDER BY complaint_key