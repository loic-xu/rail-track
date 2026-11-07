{{ config(materialized='table') }}

SELECT
    md5(journey_id) AS train_key,
    journey_id,
    line_ref,
    line_name,
    mission_code,
    direction,
    vehicle_mode,
    origin_name,
    destination_name,
    train_length
FROM {{ ref('stg_train_stops') }}
WHERE journey_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY journey_id
    ORDER BY recorded_at DESC
) = 1
