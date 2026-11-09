import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from types import UnionType
from typing import Any, Union, get_args, get_origin

import duckdb
import polars as pl

from rail_track.ingestion.models import StopCall, TrainJourney
from rail_track.utils.logger import get_logger

logger = get_logger(__name__)


def _pydantic_to_polars_type(py_type: Any) -> pl.DataType:
    """Map a Python / Pydantic field annotation to its equivalent Polars DataType."""
    origin = get_origin(py_type)
    if origin in (Union, UnionType):
        args = [a for a in get_args(py_type) if a is not type(None)]
        if args:
            py_type = args[0]
    if py_type is str:
        return pl.String()
    if py_type is bool:
        return pl.Boolean()
    if py_type is int:
        return pl.Int64()
    if py_type is float:
        return pl.Float64()
    if py_type is datetime:
        return pl.Datetime("us", "UTC")
    return pl.String()


def build_raw_calls_schema() -> dict[str, pl.DataType]:
    """Dynamically derive the tabular Polars schema from Pydantic contract models.

    Preserves the Pydantic Data Contract (Control Plane) as the Single
    Source of Truth (SSOT), strictly preventing schema drift between
    contract models and empty dataframe fallbacks in the Data Plane.
    """
    schema: dict[str, pl.DataType] = {"recorded_at": pl.Datetime("us", "UTC")}
    for name, field in TrainJourney.model_fields.items():
        if name != "calls":
            schema[name] = _pydantic_to_polars_type(field.annotation)
    for name, field in StopCall.model_fields.items():
        schema[name] = _pydantic_to_polars_type(field.annotation)
    schema["stop_sequence"] = pl.Int64()
    return schema


RAW_CALLS_SCHEMA: dict[str, pl.DataType] = build_raw_calls_schema()


class SilverTransformer:
    def __init__(self, duckdb_conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self.conn = duckdb_conn if duckdb_conn else duckdb.connect(":memory:")
        self.conn.execute("SET TimeZone = 'UTC';")

    @staticmethod
    def _file_has_journeys(file_path: str | Path) -> bool:
        """Inspect a Bronze JSON file to check if it contains any vehicle journeys."""
        path = Path(file_path)
        if not path.is_file():
            return False
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            deliveries = (
                data.get("Siri", {})
                .get("ServiceDelivery", {})
                .get("EstimatedTimetableDelivery", [])
            )
            return any(
                bool(frame.get("EstimatedVehicleJourney"))
                for d in deliveries
                for frame in d.get("EstimatedJourneyVersionFrame", [])
            )
        except Exception:
            return False

    def extract_raw_calls(
        self, bronze_path_pattern: str | Path | list[str] | list[Path]
    ) -> pl.DataFrame:
        """Extract and unnest nested SIRI JSON into a tabular schema.

        Leverages DuckDB's streaming JSON engine to flatten nested
        arrays without materializing bloated intermediate Python
        dictionaries in memory. Enriches the result with a calculated
        ``stop_sequence`` representing the sequential stop order of each train.

        Args:
            bronze_path_pattern: Filepath, glob pattern, or list of file paths
                pointing to raw Bronze JSON dumps.

        Returns:
            A Polars DataFrame where each row corresponds to an
            individual train stop call with calculated stop sequence.
        """
        if isinstance(bronze_path_pattern, (list, tuple)):
            candidate_files = [Path(p) for p in bronze_path_pattern]
        elif "*" in str(bronze_path_pattern) or "?" in str(bronze_path_pattern):
            candidate_files = [
                Path(p) for p in glob.glob(str(bronze_path_pattern), recursive=True)
            ]
        else:
            candidate_files = [Path(bronze_path_pattern)]

        # Filter out nocturnal off-peak files where no trains circulate
        valid_files = [f for f in candidate_files if self._file_has_journeys(f)]
        if not valid_files:
            logger.info(
                "No active train journeys found in Bronze files "
                "(nocturnal service shutdown). Returning empty schema."
            )
            return pl.DataFrame(schema=RAW_CALLS_SCHEMA)

        path_literal = "[" + ", ".join(f"'{p}'" for p in valid_files) + "]"

        logger.info(f"Starting raw call extraction from Bronze source: {path_literal}")

        query = f"""
        WITH raw_json AS (
            SELECT *
            FROM read_json_auto(
                {path_literal},
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
                -- Isolate stop sequence (1..N) per train journey and temporal snapshot
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
        bronze_path_pattern: str | Path | list[str] | list[Path],
        silver_dir: Path | str = "data/silver",
        filename: str | None = None,
    ) -> list[Path]:
        """Execute end-to-end Silver pipeline: extract, enrich, and persist.

        Args:
            bronze_path_pattern: Glob pattern, path, or list of paths to Bronze files.
            silver_dir: Output base directory for Silver Parquet files.
            filename: Optional custom filename for the Parquet file.

        Returns:
            List of generated Parquet file paths.
        """
        raw_df = self.extract_raw_calls(bronze_path_pattern)
        enriched_df = self.compute_delays(raw_df)
        return self.save_to_silver(enriched_df, base_dir=silver_dir, filename=filename)

    def compact_month(
        self,
        silver_dir: Path | str,
        year: int,
        month: int,
        target_filename: str | None = None,
    ) -> Path | None:
        """Compact Silver delta Parquet files of a given month into one file.

        Consolidates hundreds or thousands of 15-minute micro-batch Parquet
        files into an atomic, ZSTD-compressed, consolidated monthly file.
        Mitigates the Small Files Problem without continuous write amplification.

        Args:
            silver_dir: Base directory for the Silver layer (e.g. ``data/silver``).
            year: Target partition year (e.g. 2026).
            month: Target partition month (e.g. 11).
            target_filename: Optional custom filename. Defaults to
                ``train_stops_{year}{month:02d}.parquet``.

        Returns:
            Path to the newly compacted Parquet file, or existing path if already
            compacted, or None if the partition directory is missing or empty.
        """
        partition_dir = Path(silver_dir) / f"year={year}" / f"month={month:02d}"
        if not partition_dir.exists():
            logger.warning(f"Partition directory does not exist: {partition_dir}")
            return None

        parquet_files = sorted(partition_dir.glob("*.parquet"))
        if not parquet_files:
            logger.warning(f"No Parquet files found in {partition_dir} to compact.")
            return None

        final_name = target_filename or f"train_stops_{year}{month:02d}.parquet"
        final_path = partition_dir / final_name

        if len(parquet_files) == 1 and parquet_files[0] == final_path:
            logger.info(f"Partition {partition_dir} is already compacted: {final_path}")
            return final_path

        logger.info(
            f"Compacting {len(parquet_files)} Parquet files in "
            f"{partition_dir} into {final_path}"
        )

        tmp_path = partition_dir / f"{final_name}.tmp"
        file_literals = (
            "[" + ", ".join(f"'{p.as_posix()}'" for p in parquet_files) + "]"
        )

        compact_query = f"""
        COPY (
            SELECT *
            FROM read_parquet({file_literals})
        ) TO '{tmp_path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
        self.conn.execute(compact_query)
        tmp_path.replace(final_path)

        removed_count = 0
        for f in parquet_files:
            if f != final_path and f.exists():
                f.unlink()
                removed_count += 1

        logger.info(
            f"Compaction completed: consolidated {len(parquet_files)} files "
            f"into {final_path} ({final_path.stat().st_size} bytes). "
            f"Removed {removed_count} obsolete delta files."
        )
        return final_path
