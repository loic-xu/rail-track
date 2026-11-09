{{ config(materialized='view') }}

WITH raw_silver AS (
    SELECT *
    FROM read_parquet({{ get_silver_path() }})
),

deduplicated_stops AS (
    -- 1. Deduplicate by physical station stop per train journey, retaining the latest snapshot state
    SELECT *
    FROM raw_silver
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY journey_id, stop_point_ref
        ORDER BY recorded_at DESC
    ) = 1
),

sequenced_stops AS (
    -- 2. Reconstruct the complete global stop sequence across all observed calls of the journey
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY journey_id
            ORDER BY COALESCE(
                aimed_arrival_time,
                aimed_departure_time,
                expected_arrival_time,
                expected_departure_time
            )
        ) AS rebuilt_stop_sequence
    FROM deduplicated_stops
)

SELECT
    -- Surrogate Primary Key based on physical train journey and station stop
    md5(concat_ws('||', journey_id, stop_point_ref)) AS punctuality_key,

    -- Date Key for dimensional joins (YYYYMMDD integer based on local Paris time of journey start)
    FIRST_VALUE(
        CAST(strftime(timezone('Europe/Paris', COALESCE(aimed_departure_time, aimed_arrival_time, expected_departure_time, expected_arrival_time)), '%Y%m%d') AS INTEGER)
    ) OVER (
        PARTITION BY journey_id
        ORDER BY rebuilt_stop_sequence
    ) AS date_key,

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
    rebuilt_stop_sequence AS stop_sequence,

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

FROM sequenced_stops
