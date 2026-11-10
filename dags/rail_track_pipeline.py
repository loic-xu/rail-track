"""RailTrack Pipeline — End-to-End Orchestration DAG.

Orchestrates the complete data journey from PRIM API to Gold marts:
1. task_ingest_bronze   : Fetch real-time SIRI snapshots (RER A-E) into Bronze.
2. task_quality_gate    : Validate contract compliance of new Bronze payloads.
3. task_transform_silver : Flatten, compute delays & partition into Silver Parquet.
4. task_dbt_run_gold    : Seed referentials and materialize dimensional Gold marts.
5. task_dbt_test_gold   : Execute comprehensive dbt data quality test suite.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from rail_track.ingestion.client import PrimApiClient
from rail_track.ingestion.models import TrainJourney
from rail_track.silver.transformer import SilverTransformer
from rail_track.utils.logger import get_logger

logger = get_logger("rail_track.airflow_dag")

TARGET_LINES = [
    "STIF:Line::C01742:",  # RER A
    "STIF:Line::C01743:",  # RER B
    "STIF:Line::C01727:",  # RER C
    "STIF:Line::C01728:",  # RER D
    "STIF:Line::C01729:",  # RER E
]

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt"


def ingest_bronze(**context: Any) -> list[str]:
    """Ingest real-time SIRI line data from PRIM API into Bronze layer."""
    client = PrimApiClient()
    saved_files: list[str] = []
    logical_date = (
        context.get("data_interval_start")
        or context.get("logical_date")
        or datetime.now(timezone.utc)
    )

    for line_ref in TARGET_LINES:
        logger.info(f"Fetching raw transit payload for line: {line_ref}")
        payload = client.fetch_line_data(line_ref=line_ref)
        saved_path = client.save_to_bronze(
            data=payload,
            line_ref=line_ref,
            base_dir="/opt/airflow/data/bronze",
            snapshot_time=logical_date,
        )
        logger.info(f"Saved Bronze payload to {saved_path}")
        saved_files.append(str(saved_path))

    return saved_files


def quality_gate(**context: Any) -> int:
    """Validate data contract compliance on newly ingested Bronze JSON payloads."""
    ti = context["ti"]
    bronze_files: list[str] = ti.xcom_pull(task_ids="task_ingest_bronze") or []

    if not bronze_files:
        today = datetime.now(timezone.utc)
        today_dir = Path(f"/opt/airflow/data/bronze/{today:%Y}/{today:%m}/{today:%d}")
        if today_dir.exists():
            bronze_files = [str(p) for p in today_dir.glob("*.json")]

    if not bronze_files:
        raise AirflowException(
            "Quality Gate failed: No Bronze files found to validate."
        )

    validated_count = 0

    for file_path in bronze_files:
        path = Path(file_path)
        if not path.exists():
            logger.warning(f"File {file_path} not found; skipping validation.")
            continue

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        deliveries = (
            data.get("Siri", {})
            .get("ServiceDelivery", {})
            .get("EstimatedTimetableDelivery", [])
        )

        if not deliveries:
            raise AirflowException(
                f"Quality Gate violation in {path.name}: "
                f"Missing Siri.ServiceDelivery.EstimatedTimetableDelivery."
            )

        # Validate a sample of vehicle journeys against Pydantic contract
        for delivery in deliveries:
            frames = delivery.get("EstimatedJourneyVersionFrame", [])
            for frame in frames:
                journeys = frame.get("EstimatedVehicleJourney", [])
                for journey in journeys[:5]:
                    TrainJourney.model_validate(journey)
                    validated_count += 1

    logger.info(
        f"Quality Gate passed successfully! {validated_count} journeys checked."
    )
    return validated_count


def transform_silver(**context: Any) -> list[str]:
    """Execute Silver pipeline: DuckDB unnesting, delays computation & Parquet."""
    ti = context["ti"]
    bronze_files: list[str] = ti.xcom_pull(task_ids="task_ingest_bronze") or []

    if not bronze_files:
        today = datetime.now(timezone.utc)
        today_dir = Path(f"/opt/airflow/data/bronze/{today:%Y}/{today:%m}/{today:%d}")
        if today_dir.exists():
            bronze_files = [str(p) for p in today_dir.glob("*.json")]

    if not bronze_files:
        logger.warning("No incremental Bronze files found; skipping Silver task.")
        return []

    transformer = SilverTransformer()
    silver_dir = "/opt/airflow/data/silver"

    logical_date = (
        context.get("data_interval_start")
        or context.get("logical_date")
        or datetime.now(timezone.utc)
    )
    target_filename = f"train_stops_{logical_date:%Y%m%d_%H%M%S}.parquet"

    logger.info(f"Transforming incremental Bronze files: {bronze_files}")
    saved_paths = transformer.transform(
        bronze_path_pattern=bronze_files,
        silver_dir=silver_dir,
        filename=target_filename,
    )

    result_files = [str(p) for p in saved_paths]
    logger.info(
        f"Silver transformation complete. {len(result_files)} partition(s) written."
    )
    return result_files


with DAG(
    dag_id="rail_track_pipeline",
    default_args=DEFAULT_ARGS,
    description="RailTrack ELT Pipeline: Ingestion, Quality Gate, Silver & Gold",
    schedule="*/15 0,5-23 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=ZoneInfo("Europe/Paris")),
    catchup=False,
    max_active_runs=1,
    tags=["rail-track", "production", "rer", "duckdb", "dbt"],
) as dag:
    # 1. Ingestion: Fetch real-time snapshots from PRIM API
    task_ingest_bronze = PythonOperator(
        task_id="task_ingest_bronze",
        python_callable=ingest_bronze,
    )

    # 2. Quality Gate: Validate contract schemas before expensive processing
    task_quality_gate = PythonOperator(
        task_id="task_quality_gate",
        python_callable=quality_gate,
    )

    # 3. Silver Transformation: Vectorized DuckDB JSON parsing to partitioned Parquet
    task_transform_silver = PythonOperator(
        task_id="task_transform_silver",
        python_callable=transform_silver,
    )

    # 4. Gold Transformation: dbt seed & run to materialize Kimball star schema
    task_dbt_run_gold = BashOperator(
        task_id="task_dbt_run_gold",
        bash_command=(
            f"dbt seed --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} && "
            f"dbt run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}"
        ),
    )

    # 5. Gold Data Quality: Run all 28 dbt data quality assertion tests
    task_dbt_test_gold = BashOperator(
        task_id="task_dbt_test_gold",
        bash_command=f"dbt test --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}",
    )

    # Define linear execution topology
    (
        task_ingest_bronze
        >> task_quality_gate
        >> task_transform_silver
        >> task_dbt_run_gold
        >> task_dbt_test_gold
    )
