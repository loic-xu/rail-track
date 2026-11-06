from pathlib import Path

import duckdb
import polars as pl

from rail_track.utils.logger import get_logger

logger = get_logger(__name__)


class SilverTransformer:
    def __init__(self, duckdb_conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = duckdb_conn if duckdb_conn else duckdb.connect(":memory:")
        self.conn.execute("SET TimeZone = 'UTC';")

    def extract_raw_calls(self, bronze_path_pattern: str | Path) -> pl.DataFrame:
        """Extract and unnest nested SIRI JSON into a tabular schema.

        Leverages DuckDB's streaming JSON engine to flatten nested
        arrays without materializing bloated intermediate Python
        dictionaries in memory. Enriches the result with a calculated
        ``stop_sequence`` representing the sequential stop order of each train.

        Args:
            bronze_path_pattern: Filepath or glob pattern pointing
                to raw Bronze JSON dumps.

        Returns:
            A Polars DataFrame where each row corresponds to an
            individual train stop call with calculated stop sequence.
        """
        path_str = str(bronze_path_pattern)
        logger.info(f"Starting raw call extraction from Bronze pattern: {path_str}")

        query = f"""
        WITH raw_json AS (
            SELECT *
            FROM read_json_auto(
                '{path_str}',
                records := true,
                ignore_errors := true
            )
        ),
        deliveries AS (
            SELECT
                UNNEST(
                    Siri.ServiceDelivery.EstimatedTimetableDelivery
                ) AS delivery
            FROM raw_json
        ),
        frames AS (
            SELECT
                UNNEST(
                    delivery.EstimatedJourneyVersionFrame
                ) AS frame
            FROM deliveries
        ),
        journeys AS (
            SELECT
                UNNEST(frame.EstimatedVehicleJourney) AS journey,
                to_json(journey) AS journey_json
            FROM frames
        ),
        calls AS (
            SELECT
                TRY_CAST(
                    json_extract_string(
                        journey_json,
                        '$.RecordedAtTime'
                    ) AS TIMESTAMPTZ
                ) AS recorded_at,
                json_extract_string(
                    journey_json,
                    '$.DatedVehicleJourneyRef.value'
                ) AS journey_id,
                json_extract_string(
                    journey_json,
                    '$.LineRef.value'
                ) AS line_ref,
                json_extract_string(
                    journey_json,
                    '$.PublishedLineName[0].value'
                ) AS line_name,
                json_extract_string(
                    journey_json,
                    '$.JourneyNote[0].value'
                ) AS mission_code,
                json_extract_string(
                    journey_json,
                    '$.DirectionRef.value'
                ) AS direction,
                json_extract_string(
                    journey_json,
                    '$.VehicleMode[0]'
                ) AS vehicle_mode,
                json_extract_string(
                    journey_json,
                    '$.OriginName[0].value'
                ) AS origin_name,
                json_extract_string(
                    journey_json,
                    '$.DestinationName[0].value'
                ) AS destination_name,
                json_extract_string(
                    journey_json,
                    '$.VehicleFeatureRef[0]'
                ) AS train_length,
                COALESCE(
                    CAST(
                        json_extract(
                            journey_json,
                            '$.Cancellation'
                        ) AS BOOLEAN
                    ),
                    false
                ) AS is_cancelled,
                to_json(
                    UNNEST(
                        journey.EstimatedCalls.EstimatedCall
                    )
                ) AS call_json
            FROM journeys
            WHERE
                journey.EstimatedCalls.EstimatedCall
                    IS NOT NULL
        ),
        parsed_calls AS (
            SELECT
                recorded_at,
                journey_id,
                line_ref,
                line_name,
                mission_code,
                direction,
                vehicle_mode,
                origin_name,
                destination_name,
                train_length,
                is_cancelled,
                json_extract_string(
                    call_json,
                    '$.StopPointRef.value'
                ) AS stop_point_ref,
                COALESCE(
                    json_extract_string(
                        call_json,
                        '$.DeparturePlatformName.value'
                    ),
                    json_extract_string(
                        call_json,
                        '$.ArrivalPlatformName.value'
                    )
                ) AS platform,
                json_extract_string(
                    call_json,
                    '$.DepartureStatus'
                ) AS departure_status,
                json_extract_string(
                    call_json,
                    '$.ArrivalStatus'
                ) AS arrival_status,
                TRY_CAST(
                    json_extract_string(
                        call_json,
                        '$.AimedArrivalTime'
                    ) AS TIMESTAMPTZ
                ) AS aimed_arrival_time,
                TRY_CAST(
                    json_extract_string(
                        call_json,
                        '$.ExpectedArrivalTime'
                    ) AS TIMESTAMPTZ
                ) AS expected_arrival_time,
                TRY_CAST(
                    json_extract_string(
                        call_json,
                        '$.AimedDepartureTime'
                    ) AS TIMESTAMPTZ
                ) AS aimed_departure_time,
                TRY_CAST(
                    json_extract_string(
                        call_json,
                        '$.ExpectedDepartureTime'
                    ) AS TIMESTAMPTZ
                ) AS expected_departure_time
            FROM calls
            WHERE journey_id IS NOT NULL
        )
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY journey_id, recorded_at
                ORDER BY COALESCE(
                    aimed_departure_time,
                    aimed_arrival_time,
                    expected_departure_time,
                    expected_arrival_time
                )
            ) AS stop_sequence
        FROM parsed_calls
        WHERE stop_point_ref IS NOT NULL;
        """

        df: pl.DataFrame = self.conn.sql(query).pl()

        logger.info(
            f"Extraction completed successfully: {df.height} stop calls ingested."
        )
        return df
