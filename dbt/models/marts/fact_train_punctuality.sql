{{ config(materialized='table') }}

SELECT
    -- Primary Surrogate Key
    punctuality_key,

    -- Dimensional Foreign Keys
    md5(journey_id) AS train_key,
    md5(stop_point_ref) AS station_key,
    date_key,
    journey_id,
    stop_point_ref,

    -- Stop Sequence & Platform
    stop_sequence,
    platform,

    -- Scheduled & Real-Time Timestamps (UTC)
    aimed_arrival_time,
    expected_arrival_time,
    aimed_departure_time,
    expected_departure_time,

    -- Punctuality & Delay Metrics (minutes)
    arrival_delay_min,
    departure_delay_min,
    arrival_status,
    departure_status,

    -- Contractual & Operational Flags
    is_cancelled,
    is_late_5min,

    -- Audit Metadata
    recorded_at

FROM {{ ref('stg_train_stops') }}
