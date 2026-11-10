"""Replay and regenerate Silver layer 1:1 from Bronze JSON snapshots."""

import shutil
from pathlib import Path
from typing import List, Tuple

from rail_track.silver.transformer import SilverTransformer
from rail_track.utils.logger import get_logger

logger = get_logger("rail_track.regenerate_silver")


def file_sort_key(p: Path) -> Tuple[str, str, str, str]:
    parts = p.parts
    year, month, day = parts[-4], parts[-3], parts[-2]
    hhmmss = p.stem.split("_")[-1]
    return (year, month, day, hhmmss)


def cluster_bronze_runs(raw_files: List[Path]) -> List[Tuple[str, List[Path]]]:
    sorted_files = sorted(raw_files, key=file_sort_key)
    runs: List[Tuple[str, List[Path]]] = []
    current_run: List[Path] = []
    last_key: Tuple[str, str, str, str] | None = None

    for p in sorted_files:
        key = file_sort_key(p)
        if not current_run or last_key is None:
            current_run.append(p)
            last_key = key
        else:
            if key[:3] == last_key[:3]:
                t_curr = (
                    int(key[3][:2]) * 3600 + int(key[3][2:4]) * 60 + int(key[3][4:6])
                )
                t_last = (
                    int(last_key[3][:2]) * 3600
                    + int(last_key[3][2:4]) * 60
                    + int(last_key[3][4:6])
                )
                if abs(t_curr - t_last) <= 60:
                    current_run.append(p)
                else:
                    runs.append(
                        (
                            f"{last_key[0]}{last_key[1]}{last_key[2]}_{last_key[3]}",
                            current_run,
                        )
                    )
                    current_run = [p]
                    last_key = key
            else:
                runs.append(
                    (
                        f"{last_key[0]}{last_key[1]}{last_key[2]}_{last_key[3]}",
                        current_run,
                    )
                )
                current_run = [p]
                last_key = key

    if current_run and last_key is not None:
        runs.append(
            (f"{last_key[0]}{last_key[1]}{last_key[2]}_{last_key[3]}", current_run)
        )

    return runs


def main() -> None:
    silver_base = Path("data/silver")
    nov_dir = silver_base / "year=2026" / "month=11"
    backup_dir = silver_base / "year=2026" / "month=11_backup"

    bronze_files = list(Path("data/bronze/2026/11").glob("*/*.json"))
    logger.info(f"Discovered {len(bronze_files)} Bronze JSON files for November 2026.")

    runs = cluster_bronze_runs(bronze_files)
    logger.info(f"Clustered into {len(runs)} distinct micro-batch pipeline runs.")

    # 1. Backup current month=11
    if nov_dir.exists():
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        logger.info(f"Backing up {nov_dir} to {backup_dir}...")
        shutil.copytree(nov_dir, backup_dir)
        # Purge existing parquet files
        for p in nov_dir.glob("*.parquet"):
            p.unlink()
        logger.info("Purged existing November Parquet files.")
    else:
        nov_dir.mkdir(parents=True, exist_ok=True)

    # 2. Transform each run 1:1
    transformer = SilverTransformer()
    total_runs = len(runs)
    for idx, (ts, flist) in enumerate(runs, 1):
        target_filename = f"train_stops_{ts}.parquet"
        logger.info(f"[{idx}/{total_runs}] Transforming run {ts} ({len(flist)} lines)")
        transformer.transform(
            bronze_path_pattern=flist,
            silver_dir=silver_base,
            filename=target_filename,
        )

    # 3. Clean up backup
    if backup_dir.exists():
        shutil.rmtree(backup_dir)
        logger.info("Removed temporary backup.")

    logger.info("Silver regeneration complete 1:1 without compaction!")


if __name__ == "__main__":
    main()
