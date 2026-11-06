"""Contract parity tests for the Silver layer.

Ensures the DuckDB SQL extraction in ``SilverTransformer`` produces
identical results to the Pydantic SIRI contract model (``TrainJourney``),
preventing silent schema drift between the two code paths.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from rail_track.ingestion.models import TrainJourney
from rail_track.silver.transformer import SilverTransformer


def _wrap_siri_envelope(
    journeys: list[dict[str, Any]],
) -> dict[str, Any]:
    """Wrap raw journeys into a full SIRI envelope."""
    return {
        "Siri": {
            "ServiceDelivery": {
                "EstimatedTimetableDelivery": [
                    {
                        "EstimatedJourneyVersionFrame": [
                            {
                                "EstimatedVehicleJourney": journeys,
                            }
                        ],
                    }
                ],
            }
        }
    }


@pytest.fixture
def raw_journey() -> dict[str, Any]:
    """A production-grade SIRI journey with nominal fields populated."""
    return {
        "RecordedAtTime": "2026-10-12T02:31:46.975Z",
        "DatedVehicleJourneyRef": {"value": "SNCF:Journey::001:LOC"},
        "LineRef": {"value": "STIF:Line::C01742:"},
        "PublishedLineName": [{"value": "A"}],
        "JourneyNote": [{"value": "QIKI"}],
        "DirectionRef": {"value": "Retour"},
        "VehicleMode": ["RAIL"],
        "OriginName": [{"value": "Saint-Germain-en-Laye"}],
        "DestinationName": [{"value": "Marne-la-Vallée Chessy"}],
        "VehicleFeatureRef": ["longTrain"],
        "Cancellation": False,
        "EstimatedCalls": {
            "EstimatedCall": [
                {
                    "StopPointRef": {"value": "STIF:StopArea:SP:43224:"},
                    "Order": 3,
                    "DeparturePlatformName": {"value": "1"},
                    "ArrivalPlatformName": {"value": "1"},
                    "DepartureStatus": "ON_TIME",
                    "ArrivalStatus": "DELAYED",
                    "AimedArrivalTime": ("2026-10-12T14:04:24.000Z"),
                    "ExpectedArrivalTime": ("2026-10-12T14:06:00.000Z"),
                    "AimedDepartureTime": ("2026-10-12T14:04:54.000Z"),
                    "ExpectedDepartureTime": ("2026-10-12T14:05:44.000Z"),
                }
            ]
        },
    }


def test_pydantic_duckdb_contract_parity(
    tmp_path: Path,
    raw_journey: dict[str, Any],
) -> None:
    """DuckDB SQL extraction must match Pydantic model field-for-field."""
    payload = _wrap_siri_envelope([raw_journey])
    sample_file = tmp_path / "sample_siri.json"
    sample_file.write_text(json.dumps(payload), encoding="utf-8")

    # Pydantic (control plane / schema contract)
    pydantic_j = TrainJourney.model_validate(raw_journey)
    pydantic_call = pydantic_j.calls[0]

    # DuckDB (data plane / bulk extraction)
    transformer = SilverTransformer()
    df = transformer.extract_raw_calls(sample_file)
    row = df.row(0, named=True)

    # Journey-level parity
    assert row["journey_id"] == pydantic_j.journey_id
    assert row["line_ref"] == pydantic_j.line_ref
    assert row["line_name"] == pydantic_j.line_name
    assert row["mission_code"] == pydantic_j.mission_code
    assert row["direction"] == pydantic_j.direction
    assert row["vehicle_mode"] == pydantic_j.vehicle_mode
    assert row["origin_name"] == pydantic_j.origin_name
    assert row["destination_name"] == pydantic_j.destination_name
    assert row["train_length"] == pydantic_j.train_length
    assert row["is_cancelled"] == pydantic_j.is_cancelled

    # Call-level parity
    assert row["stop_point_ref"] == pydantic_call.stop_point_ref
    assert row["platform"] == pydantic_call.platform
    assert row["departure_status"] == pydantic_call.departure_status
    assert row["arrival_status"] == pydantic_call.arrival_status


def test_cancelled_journey_flag(
    tmp_path: Path,
    raw_journey: dict[str, Any],
) -> None:
    """Cancelled journeys propagate is_cancelled=True across both paths."""
    journey = raw_journey.copy()
    journey["Cancellation"] = True
    payload = _wrap_siri_envelope([journey])

    sample_file = tmp_path / "cancelled.json"
    sample_file.write_text(json.dumps(payload), encoding="utf-8")

    pydantic_j = TrainJourney.model_validate(journey)
    assert pydantic_j.is_cancelled is True

    transformer = SilverTransformer()
    df = transformer.extract_raw_calls(sample_file)
    row = df.row(0, named=True)
    assert row["is_cancelled"] is True


def test_missing_optional_fields(tmp_path: Path) -> None:
    """Journeys without optional fields must not crash DuckDB or Pydantic."""
    minimal_journey = {
        "DatedVehicleJourneyRef": {"value": "SNCF:Journey::002:LOC"},
        "PublishedLineName": [{"value": "B"}],
        "DirectionRef": {"value": "Aller"},
        "VehicleMode": ["RAIL"],
        "EstimatedCalls": {
            "EstimatedCall": [
                {
                    "StopPointRef": {"value": "STIF:StopArea:SP:99999:"},
                    "DepartureStatus": "EARLY",
                }
            ]
        },
    }
    payload = _wrap_siri_envelope([minimal_journey])
    sample_file = tmp_path / "minimal.json"
    sample_file.write_text(json.dumps(payload), encoding="utf-8")

    # Pydantic validation
    pydantic_j = TrainJourney.model_validate(minimal_journey)
    assert pydantic_j.line_name == "B"
    assert pydantic_j.mission_code is None
    assert pydantic_j.is_cancelled is False

    # DuckDB extraction
    transformer = SilverTransformer()
    df = transformer.extract_raw_calls(sample_file)
    assert df.height == 1

    row = df.row(0, named=True)
    assert row["line_name"] == "B"
    assert row["mission_code"] is None
    assert row["stop_point_ref"] == "STIF:StopArea:SP:99999:"
    assert row["platform"] is None
    assert row["arrival_status"] is None
    assert row["aimed_arrival_time"] is None
    assert row["is_cancelled"] is False


def test_stop_sequence_calculation(tmp_path: Path) -> None:
    """Validate that stop_sequence increments chronologically along the route."""
    multi_stop_journey = {
        "DatedVehicleJourneyRef": {"value": "SNCF:Journey::100"},
        "PublishedLineName": [{"value": "A"}],
        "DirectionRef": {"value": "Aller"},
        "VehicleMode": ["RAIL"],
        "EstimatedCalls": {
            "EstimatedCall": [
                {
                    "StopPointRef": {"value": "SP:Gare2"},
                    "AimedArrivalTime": "2026-10-12T10:10:00.000Z",
                    "AimedDepartureTime": "2026-10-12T10:11:00.000Z",
                },
                {
                    "StopPointRef": {"value": "SP:Gare1"},
                    "AimedDepartureTime": "2026-10-12T10:00:00.000Z",
                },
                {
                    "StopPointRef": {"value": "SP:Gare3"},
                    "AimedArrivalTime": "2026-10-12T10:20:00.000Z",
                },
            ]
        },
    }
    payload = _wrap_siri_envelope([multi_stop_journey])
    sample_file = tmp_path / "multi_stop.json"
    sample_file.write_text(json.dumps(payload), encoding="utf-8")

    transformer = SilverTransformer()
    df = transformer.extract_raw_calls(sample_file)

    assert df.height == 3
    # Check stop_sequence is strictly 1, 2, 3 in chronological order
    sequences = df.sort("stop_sequence")["stop_point_ref"].to_list()
    assert sequences == ["SP:Gare1", "SP:Gare2", "SP:Gare3"]
    assert df.sort("stop_sequence")["stop_sequence"].to_list() == [1, 2, 3]


def test_multi_snapshot_partitioning(tmp_path: Path) -> None:
    """Ensure stop_sequence resets per snapshot (recorded_at) for the same train."""
    journey_snap1 = {
        "RecordedAtTime": "2026-10-12T08:00:00.000Z",
        "DatedVehicleJourneyRef": {"value": "SNCF:Journey::999"},
        "PublishedLineName": [{"value": "A"}],
        "DirectionRef": {"value": "Aller"},
        "VehicleMode": ["RAIL"],
        "EstimatedCalls": {
            "EstimatedCall": [
                {
                    "StopPointRef": {"value": "SP:GareA"},
                    "AimedDepartureTime": "2026-10-12T08:05:00.000Z",
                },
                {
                    "StopPointRef": {"value": "SP:GareB"},
                    "AimedArrivalTime": "2026-10-12T08:15:00.000Z",
                },
            ]
        },
    }
    journey_snap2 = {
        "RecordedAtTime": "2026-10-12T08:05:00.000Z",
        "DatedVehicleJourneyRef": {"value": "SNCF:Journey::999"},
        "PublishedLineName": [{"value": "A"}],
        "DirectionRef": {"value": "Aller"},
        "VehicleMode": ["RAIL"],
        "EstimatedCalls": {
            "EstimatedCall": [
                {
                    "StopPointRef": {"value": "SP:GareA"},
                    "AimedDepartureTime": "2026-10-12T08:05:00.000Z",
                },
                {
                    "StopPointRef": {"value": "SP:GareB"},
                    "AimedArrivalTime": "2026-10-12T08:15:00.000Z",
                },
            ]
        },
    }
    file1 = tmp_path / "snap1.json"
    file2 = tmp_path / "snap2.json"
    file1.write_text(json.dumps(_wrap_siri_envelope([journey_snap1])), encoding="utf-8")
    file2.write_text(json.dumps(_wrap_siri_envelope([journey_snap2])), encoding="utf-8")

    transformer = SilverTransformer()
    # Read both snapshots using glob pattern
    df = transformer.extract_raw_calls(tmp_path / "snap*.json")

    assert df.height == 4
    # Each snapshot must independently have sequence [1, 2], not [1, 2, 3, 4]
    snap1_seqs = (
        df.filter(df["recorded_at"].dt.hour() == 8)
        .sort(["recorded_at", "stop_sequence"])["stop_sequence"]
        .to_list()
    )
    assert snap1_seqs == [1, 2, 1, 2]
