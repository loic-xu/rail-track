import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from rail_track.ingestion.client import PrimApiClient


def test_client_init_missing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure client instantiation fails immediately when no API key is supplied."""
    monkeypatch.delenv("PRIM_API_KEY", raising=False)

    with pytest.raises(ValueError, match="PRIM API key is required"):
        PrimApiClient(api_key=None)


def test_client_init_with_explicit_key() -> None:
    """Validate explicit credentials override environment configurations."""
    client = PrimApiClient(api_key="explicit_secret_key", timeout=5.0)

    assert client.api_key == "explicit_secret_key"
    assert client.timeout == 5.0


def test_fetch_line_data_success() -> None:
    """Validate nominal API response ingestion, headers, and query parameters."""
    client = PrimApiClient(api_key="test_api_key", timeout=10.0)
    mock_payload: dict[str, Any] = {
        "Siri": {"ServiceDelivery": {"EstimatedTimetableDelivery": []}}
    }

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = mock_payload
    mock_response.raise_for_status.return_value = None

    with patch("httpx.Client.get", return_value=mock_response) as mock_get:
        data = client.fetch_line_data(line_ref="STIF:Line::C01742:")

        assert data == mock_payload
        mock_get.assert_called_once_with(
            "https://prim.iledefrance-mobilites.fr/marketplace/requete-ligne",
            headers={
                "Accept": "application/json",
                "apiKey": "test_api_key",
            },
            params={"LineRef": "STIF:Line::C01742:"},
        )


def test_fetch_line_data_http_error() -> None:
    """Ensure HTTP errors (e.g. 401 Unauthorized, 500) raise an HTTPStatusError."""
    client = PrimApiClient(api_key="invalid_key")

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 401
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        message="401 Unauthorized",
        request=MagicMock(spec=httpx.Request),
        response=mock_response,
    )

    with patch("httpx.Client.get", return_value=mock_response):
        with pytest.raises(httpx.HTTPStatusError):
            client.fetch_line_data()


def test_fetch_line_data_timeout_network_error() -> None:
    """Ensure connectivity drops and timeouts bubble up as RequestErrors."""
    client = PrimApiClient(api_key="test_api_key")

    with patch(
        "httpx.Client.get",
        side_effect=httpx.TimeoutException("Read operation timed out"),
    ):
        with pytest.raises(httpx.RequestError):
            client.fetch_line_data()


def test_save_to_bronze_structure_and_persistence(tmp_path: Path) -> None:
    """Verify raw JSON persistence, folder partitioning, and naming sanitization.

    Validates that:
    - Folders follow the partitioned layout: {base_dir}/YYYY/MM/DD/.
    - Technical LineRef colons are stripped to avoid illegal path characters.
    - Serialized contents strictly match source raw payloads.
    """
    client = PrimApiClient(api_key="test_api_key")
    sample_payload: dict[str, Any] = {"status": "ok", "records": [1, 2, 3]}
    line_ref = "STIF:Line::C01742:"

    saved_file_path = client.save_to_bronze(
        data=sample_payload,
        line_ref=line_ref,
        base_dir=tmp_path,
    )
    assert saved_file_path.exists()
    assert saved_file_path.is_file()

    relative_parts = saved_file_path.relative_to(tmp_path).parts
    assert len(relative_parts) == 4
    assert relative_parts[0].isdigit() and len(relative_parts[0]) == 4
    assert relative_parts[1].isdigit() and len(relative_parts[1]) == 2
    assert relative_parts[2].isdigit() and len(relative_parts[2]) == 2

    filename = relative_parts[3]
    assert filename.startswith("run_C01742_")
    assert filename.endswith(".json")

    with open(saved_file_path, encoding="utf-8") as f:
        loaded_content = json.load(f)
    assert loaded_content == sample_payload
