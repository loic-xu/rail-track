import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import httpx
from dotenv import load_dotenv

load_dotenv()


class PrimApiClient:
    """Client dedicated to fetching real-time transit data from PRIM."""

    BASE_URL = "https://prim.iledefrance-mobilites.fr/marketplace"

    def __init__(self, api_key: str | None = None, timeout: float = 10.0) -> None:
        key = api_key or os.getenv("PRIM_API_KEY")

        if not key:
            raise ValueError(
                "PRIM API key is required. "
                "Set PRIM_API_KEY in .env or pass it explicitly."
            )
        self.api_key: str = key
        self.timeout = timeout

    def fetch_line_data(self, line_ref: str = "STIF:Line::C01742:") -> dict[str, Any]:
        """Fetch real-time departure schedules for all stops on a given line.

        Args:
            line_ref: STIF line technical identifier.

        Returns:
            RAW SIRI Lite JSON response payload.

        Raises:
            httpx.HTTPStatusError: If the API returns a 4xx/5xx status code.
            httpx.RequestError: If a network or timeout error occurs.
        """
        endpoint = f"{self.BASE_URL}/requete-ligne"
        headers = {
            "Accept": "application/json",
            "apiKey": self.api_key,
        }
        params = {"LineRef": line_ref}

        with httpx.Client(timeout=self.timeout) as client:
            response = client.get(endpoint, headers=headers, params=params)
            response.raise_for_status()
            return cast(dict[str, Any], response.json())

    def save_to_bronze(
        self,
        data: dict[str, Any],
        line_ref: str,
        base_dir: Path | str = "data/bronze",
        snapshot_time: datetime | None = None,
    ) -> Path:
        now = snapshot_time or datetime.now(timezone.utc)
        partition_dir = Path(base_dir) / f"{now:%Y}" / f"{now:%m}" / f"{now:%d}"
        partition_dir.mkdir(parents=True, exist_ok=True)

        safe_line = line_ref.strip(":").split(":")[-1]
        file_name = f"run_{safe_line}_{now:%H%M%S}.json"
        target_file = partition_dir / file_name

        with open(target_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return target_file
