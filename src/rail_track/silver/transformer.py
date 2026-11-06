from datetime import datetime, timezone
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

    def compute_delays(self, df: pl.DataFrame) -> pl.DataFrame:
        """Compute atomic delay deltas in minutes and contractual flags.

        Calculates arrival and departure delays against aimed schedules,
        handling origin and terminus boundaries gracefully (preserving nulls),
        and adds the official IDFM ``is_late_5min`` boolean flag.

        Args:
            df: Polars DataFrame output from ``extract_raw_calls()``.

        Returns:
            Polars DataFrame enriched with ``arrival_delay_min``,
            ``departure_delay_min``, and ``is_late_5min``.
        """
        logger.info(f"Computing delay deltas for {df.height} stop calls.")

        return df.with_columns(
            [
                (
                    (
                        pl.col("expected_arrival_time") - pl.col("aimed_arrival_time")
                    ).dt.total_seconds()
                    / 60.0
                )
                .round(2)
                .alias("arrival_delay_min"),
                (
                    (
                        pl.col("expected_departure_time")
                        - pl.col("aimed_departure_time")
                    ).dt.total_seconds()
                    / 60.0
                )
                .round(2)
                .alias("departure_delay_min"),
            ]
        ).with_columns(
            [
                (
                    (pl.col("arrival_delay_min") >= 5.0)
                    | (pl.col("departure_delay_min") >= 5.0)
                )
                .fill_null(False)
                .alias("is_late_5min")
            ]
        )

    def save_to_silver(
        self,
        df: pl.DataFrame,
        base_dir: Path | str = "data/silver",
        filename: str | None = None,
    ) -> list[Path]:
        """Save cleaned Silver data as partitioned Parquet files.

        Partitions the DataFrame by date into ``year=YYYY/month=MM/`` directories
        using ZSTD compression for optimal query performance and compact storage.

        Args:
            df: Enriched Polars DataFrame from ``compute_delays()``.
            base_dir: Root directory for the Silver storage layer.
            filename: Optional custom filename for the Parquet file. Defaults to
                ``train_stops_{timestamp}.parquet``.

        Returns:
            List of Path objects pointing to the newly written Parquet files.
        """
        if df.is_empty():
            logger.warning(
                "Empty DataFrame provided to save_to_silver; skipping write."
            )
            return []

        base_path = Path(base_dir)
        now = datetime.now(timezone.utc)
        file_stem = filename or f"train_stops_{now:%Y%m%d_%H%M%S}.parquet"

        df_partitioned = df.with_columns(
            [
                pl.coalesce(
                    [
                        pl.col("recorded_at").dt.year(),
                        pl.lit(now.year),
                    ]
                ).alias("_part_year"),
                pl.coalesce(
                    [
                        pl.col("recorded_at").dt.strftime("%m"),
                        pl.lit(f"{now.month:02d}"),
                    ]
                ).alias("_part_month"),
            ]
        )

        saved_files: list[Path] = []
        for (year, month), partition_df in df_partitioned.group_by(
            ["_part_year", "_part_month"]
        ):
            target_dir = base_path / f"year={year}" / f"month={month}"
            target_dir.mkdir(parents=True, exist_ok=True)
            target_path = target_dir / file_stem

            cleaned_partition = partition_df.drop(["_part_year", "_part_month"])
            cleaned_partition.write_parquet(
                target_path,
                compression="zstd",
            )
            logger.info(
                f"Saved {cleaned_partition.height} rows to Silver Parquet: "
                f"{target_path}"
            )
            saved_files.append(target_path)

        return saved_files

    def transform(
        self,
        bronze_path_pattern: str | Path,
        silver_dir: Path | str = "data/silver",
        filename: str | None = None,
    ) -> list[Path]:
        """Execute end-to-end Silver pipeline: extract, enrich, and persist.

        Args:
            bronze_path_pattern: Glob pattern or path to raw Bronze files.
            silver_dir: Output base directory for Silver Parquet files.
            filename: Optional custom filename for the Parquet file.

        Returns:
            List of generated Parquet file paths.
        """
        raw_df = self.extract_raw_calls(bronze_path_pattern)
        enriched_df = self.compute_delays(raw_df)
        return self.save_to_silver(enriched_df, base_dir=silver_dir, filename=filename)
