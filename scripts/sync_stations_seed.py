"""Synchronize official IDFM station referential data into dbt seeds.

Fetches the latest official station master data from the Île-de-France Mobilités
Open Data API, extracts stations for RER A, B, C, D and E, formats technical identifiers
to match SIRI NeTEx standard (``STIF:StopArea:SP:XXXXX:``), deduplicates multimodal
hubs, and writes ``dbt/seeds/dim_stations_rer.csv``.
"""

import csv
import sys
from pathlib import Path
from typing import Any

import httpx

API_URL = (
    "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/"
    "emplacement-des-gares-idf/records"
)
DEFAULT_OUTPUT_PATH = Path("dbt/seeds/dim_stations_rer.csv")


def fetch_stations_referential(
    output_path: Path = DEFAULT_OUTPUT_PATH, timeout_s: float = 15.0
) -> int:
    """Fetch RER stations from IDFM Open Data API and write dbt seed CSV.

    Args:
        output_path: Target path for the output CSV file.
        timeout_s: HTTP request timeout in seconds.

    Returns:
        Number of unique stations successfully written.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    limit = 100
    offset = 0
    records: list[dict[str, Any]] = []

    print(f"Fetching station referential from IDFM Open Data: {API_URL}...")
    while True:
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "where": "res_com IN ('RER A', 'RER B', 'RER C', 'RER D', 'RER E')",
        }
        try:
            response = httpx.get(API_URL, params=params, timeout=timeout_s)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as exc:
            print(f"Error fetching IDFM Open Data: {exc}", file=sys.stderr)
            raise

        batch = data.get("results", [])
        records.extend(batch)
        total_count = data.get("total_count", len(records))
        if len(records) >= total_count or not batch:
            break
        offset += limit

    print(f"Retrieved {len(records)} raw records from IDFM API.")

    # Deduplicate by stop_point_ref while aggregating served lines
    stations: dict[str, dict[str, Any]] = {}

    for record in records:
        zda_id = record.get("id_ref_zda")
        if not zda_id:
            continue

        stop_point_ref = f"STIF:StopArea:SP:{zda_id}:"
        line_name = (record.get("res_com") or "").strip()
        station_name = (record.get("nom_gares") or "").strip()
        exploitant = (record.get("exploitant") or "").strip()
        geo = record.get("geo_point_2d") or {}

        if stop_point_ref not in stations:
            stations[stop_point_ref] = {
                "stop_point_ref": stop_point_ref,
                "station_name": station_name,
                "lines": {line_name} if line_name else set(),
                "exploitant": exploitant,
                "latitude": geo.get("lat", ""),
                "longitude": geo.get("lon", ""),
            }
        else:
            if line_name:
                stations[stop_point_ref]["lines"].add(line_name)

    # Write clean CSV
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "stop_point_ref",
                "station_name",
                "line_name",
                "exploitant",
                "latitude",
                "longitude",
            ]
        )

        for s in sorted(stations.values(), key=lambda x: x["station_name"]):
            lines_str = " / ".join(sorted(s["lines"]))
            writer.writerow(
                [
                    s["stop_point_ref"],
                    s["station_name"],
                    lines_str,
                    s["exploitant"],
                    s["latitude"],
                    s["longitude"],
                ]
            )

    print(f"Successfully generated {output_path} with {len(stations)} unique stations.")
    return len(stations)


if __name__ == "__main__":
    fetch_stations_referential()
