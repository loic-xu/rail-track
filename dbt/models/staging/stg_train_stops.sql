{{ config(materialized='view') }}

WITH raw_silver AS (
    SELECT *
    FROM read_parquet({{ get_silver_path() }})
),

deduplicated AS (
    -- Retain the latest snapshot for each unique train journey and stop sequence
    SELECT *
    FROM raw_silver
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY journey_id, stop_sequence
        ORDER BY recorded_at DESC
    ) = 1
)

SELECT
    -- Surrogate Primary Key
    md5(concat_ws('||', journey_id, CAST(stop_sequence AS VARCHAR))) AS punctuality_key,

    -- Date Key for dimensional joins (YYYYMMDD integer)
    CAST(strftime(COALESCE(aimed_departure_time, aimed_arrival_time), '%Y%m%d') AS INTEGER) AS date_key,

    -- Train & Journey Identifiers
    journey_id,
    line_ref,
    line_name,
    mission_code,
    direction,
    vehicle_mode,
    origin_name,
    destination_name,
    train_length,

    -- Station & Stop Call Technical References
    stop_point_ref,
    platform,
    stop_sequence,

    -- Timestamps (Strict UTC)
    aimed_arrival_time,
    expected_arrival_time,
    aimed_departure_time,
    expected_departure_time,

    -- Delay Deltas & Indicators
    arrival_delay_min,
    departure_delay_min,
    arrival_status,
    departure_status,
    is_cancelled,
    is_late_5min,

    -- Audit Metadata
    recorded_at

FROM deduplicated
