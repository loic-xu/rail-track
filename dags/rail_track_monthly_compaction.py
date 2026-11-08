"""RailTrack Monthly Compaction — Silver Layer Maintenance DAG.

Consolidates all individual 15-minute micro-batch Parquet files of the
previous month into a single ZSTD-compressed, consolidated monthly file.

Executes on the 1st of every month at 03:00 UTC (during the RER night break).
Solves the Small Files Problem without continuous write amplification.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from rail_track.silver.transformer import SilverTransformer
from rail_track.utils.logger import get_logger

logger = get_logger("rail_track.airflow_compaction")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def compact_previous_month(**context: Any) -> str:
    """Identify and compact all delta files from the recently concluded month."""
    # Airflow provides logical_date in context
    logical_date: datetime = context.get("logical_date") or datetime.now(timezone.utc)

    # Determine previous month and year
    first_of_month = logical_date.replace(day=1)
    last_day_prev_month = first_of_month - timedelta(days=1)
    target_year = last_day_prev_month.year
    target_month = last_day_prev_month.month

    logger.info(
        f"Triggering monthly compaction for year={target_year}/month={target_month:02d}"
    )

    transformer = SilverTransformer()
    silver_dir = "/opt/airflow/data/silver"

    compacted_path = transformer.compact_month(
        silver_dir=silver_dir,
        year=target_year,
        month=target_month,
    )

    if compacted_path:
        logger.info(f"Monthly compaction completed successfully: {compacted_path}")
        return str(compacted_path)

    logger.info(
        f"No partition found to compact for {target_year}-{target_month:02d}; skipped."
    )
    return "SKIPPED_NO_PARTITION"


with DAG(
    dag_id="rail_track_monthly_compaction",
    default_args=DEFAULT_ARGS,
    description="Monthly compaction of Silver Parquet files (Hot -> Cold)",
    schedule="0 3 1 * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rail-track", "maintenance", "compaction", "silver", "duckdb"],
) as dag:
    task_compact_silver = PythonOperator(
        task_id="task_compact_previous_month",
        python_callable=compact_previous_month,
    )
