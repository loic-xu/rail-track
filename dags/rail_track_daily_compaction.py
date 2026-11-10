"""RailTrack Daily Compaction — Silver Layer Maintenance DAG.

Consolidates all individual 15-minute micro-batch Parquet files of the
previous day into a single ZSTD-compressed, consolidated daily file.

Executes daily at 02:00 UTC (during the RER night break).
Solves the Small Files Problem while keeping optimal daily partition pruning.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from rail_track.silver.transformer import SilverTransformer
from rail_track.utils.logger import get_logger

logger = get_logger("rail_track.airflow_daily_compaction")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def compact_previous_day(**context: Any) -> str:
    """Identify and compact all delta files from the concluded day."""
    dag_run = context.get("dag_run")
    conf_date = dag_run.conf.get("date") if dag_run and dag_run.conf else None

    if conf_date:
        # Manual trigger with custom date payload: {"date": "YYYY-MM-DD"}
        target_dt = datetime.strptime(conf_date, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
        logger.info(f"Targeting custom date from DAG run conf: {conf_date}")
    else:
        interval_start = context.get("data_interval_start")
        interval_end = context.get("data_interval_end")

        # In Airflow scheduled runs with '0 2 * * *', interval_start is the previous day
        if interval_start and interval_end and interval_start != interval_end:
            target_dt = interval_start
        else:
            # Fallback for manual trigger without conf: target yesterday
            base_date: datetime = context.get("logical_date") or datetime.now(
                timezone.utc
            )
            target_dt = base_date - timedelta(days=1)

    target_year = target_dt.year
    target_month = target_dt.month
    target_day = target_dt.day
    date_str = f"{target_year}-{target_month:02d}-{target_day:02d}"

    logger.info(f"Triggering daily compaction for {date_str}")

    transformer = SilverTransformer()
    silver_dir = "/opt/airflow/data/silver"

    compacted_path = transformer.compact_day(
        silver_dir=silver_dir,
        year=target_year,
        month=target_month,
        day=target_day,
    )

    if compacted_path:
        logger.info(f"Daily compaction completed successfully: {compacted_path}")
        return str(compacted_path)

    logger.info(f"No delta files found to compact for {date_str}; skipped.")
    return "SKIPPED_NO_FILES"


with DAG(
    dag_id="rail_track_daily_compaction",
    default_args=DEFAULT_ARGS,
    description="Daily compaction of Silver Parquet files (Deltas -> Daily)",
    schedule="0 2 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    tags=["rail-track", "maintenance", "compaction", "silver", "duckdb"],
) as dag:
    task_compact_silver_day = PythonOperator(
        task_id="task_compact_previous_day",
        python_callable=compact_previous_day,
    )
