{{ config(materialized='table') }}

WITH stations_in_stream AS (
    SELECT DISTINCT stop_point_ref
    FROM {{ ref('stg_train_stops') }}
    WHERE stop_point_ref IS NOT NULL
),

referential AS (
    SELECT *
    FROM {{ ref('dim_stations_rer') }}
),

-- Combine stations observed in traffic and stations from the referential
combined_keys AS (
    SELECT stop_point_ref FROM stations_in_stream
    UNION
    SELECT stop_point_ref FROM referential
),

enriched AS (
    SELECT
        md5(c.stop_point_ref) AS station_key,
        c.stop_point_ref,
        COALESCE(r.station_name, 'Station non répertoriée') AS station_name,
        COALESCE(r.line_name, 'Non spécifié') AS lines_served,
        COALESCE(r.exploitant, 'Non spécifié') AS exploitant,
        r.latitude,
        r.longitude
    FROM combined_keys c
    LEFT JOIN referential r ON c.stop_point_ref = r.stop_point_ref
),

unknown_row AS (
    SELECT
        md5('UNKNOWN') AS station_key,
        'UNKNOWN' AS stop_point_ref,
        'Station inconnue' AS station_name,
        'Non applicable' AS lines_served,
        'Non applicable' AS exploitant,
        CAST(NULL AS DOUBLE) AS latitude,
        CAST(NULL AS DOUBLE) AS longitude
)

SELECT * FROM enriched
UNION ALL
SELECT * FROM unknown_row
