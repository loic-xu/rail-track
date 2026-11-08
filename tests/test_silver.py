"""Contract parity tests for the Silver layer.

Ensures the DuckDB SQL extraction in ``SilverTransformer`` produces
identical results to the Pydantic SIRI contract model (``TrainJourney``),
preventing silent schema drift between the two code paths.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
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


def test_compute_delays_nominal_and_boundaries() -> None:
    """Validate arrival/departure delay deltas and is_late_5min flag."""
    transformer = SilverTransformer()
    test_df = pl.DataFrame(
        {
            "journey_id": ["J1", "J1", "J2"],
            "stop_sequence": [1, 2, 1],
            "aimed_arrival_time": [
                None,
                datetime(2026, 10, 12, 10, 10, tzinfo=timezone.utc),
                datetime(2026, 10, 12, 11, 0, tzinfo=timezone.utc),
            ],
            "expected_arrival_time": [
                None,
                datetime(2026, 10, 12, 10, 16, tzinfo=timezone.utc),  # +6 min
                datetime(2026, 10, 12, 11, 2, tzinfo=timezone.utc),  # +2 min
            ],
            "aimed_departure_time": [
                datetime(2026, 10, 12, 10, 0, tzinfo=timezone.utc),
                None,  # Terminus
                datetime(2026, 10, 12, 11, 5, tzinfo=timezone.utc),
            ],
            "expected_departure_time": [
                datetime(
                    2026, 10, 12, 9, 59, 30, tzinfo=timezone.utc
                ),  # -0.5 min early
                None,
                datetime(2026, 10, 12, 11, 10, 30, tzinfo=timezone.utc),  # +5.5 min
            ],
        }
    )

    enriched = transformer.compute_delays(test_df)

    # Row 1 (Origin): arrival delay null, departure delay -0.5 min, not late 5min
    assert enriched["arrival_delay_min"][0] is None
    assert enriched["departure_delay_min"][0] == -0.5
    assert enriched["is_late_5min"][0] is False

    # Row 2 (Terminus): arrival delay +6.0 min, departure delay null, late 5min
    assert enriched["arrival_delay_min"][1] == 6.0
    assert enriched["departure_delay_min"][1] is None
    assert enriched["is_late_5min"][1] is True

    # Row 3 (Intermediate): arrival +2.0 min, departure +5.5 min -> late 5min True
    assert enriched["arrival_delay_min"][2] == 2.0
    assert enriched["departure_delay_min"][2] == 5.5
    assert enriched["is_late_5min"][2] is True


def test_save_to_silver_and_transform_pipeline(tmp_path: Path) -> None:
    """Validate Parquet partitioning, write integrity, and roundtrip reading."""
    transformer = SilverTransformer()

    test_df = pl.DataFrame(
        {
            "recorded_at": [
                datetime(2026, 10, 12, 14, 0, tzinfo=timezone.utc),
                datetime(2026, 11, 5, 8, 30, tzinfo=timezone.utc),
            ],
            "journey_id": ["TRAIN_A", "TRAIN_B"],
            "stop_point_ref": ["SP:1", "SP:2"],
            "arrival_delay_min": [0.0, 6.2],
            "departure_delay_min": [1.5, 7.0],
            "is_late_5min": [False, True],
        }
    )

    silver_dir = tmp_path / "silver"
    saved_files = transformer.save_to_silver(
        test_df, base_dir=silver_dir, filename="test_run.parquet"
    )

    assert len(saved_files) == 2

    # Check partition directories structure
    oct_part = silver_dir / "year=2026" / "month=10" / "test_run.parquet"
    nov_part = silver_dir / "year=2026" / "month=11" / "test_run.parquet"
    assert oct_part.exists()
    assert nov_part.exists()

    # Read back and verify roundtrip
    oct_df = pl.read_parquet(oct_part)
    assert oct_df.height == 1
    assert oct_df["journey_id"][0] == "TRAIN_A"
    assert oct_df["is_late_5min"][0] is False

    nov_df = pl.read_parquet(nov_part)
    assert nov_df.height == 1
    assert nov_df["journey_id"][0] == "TRAIN_B"
    assert nov_df["is_late_5min"][0] is True


def test_save_to_silver_empty_dataframe(tmp_path: Path) -> None:
    """Empty dataframe must return an empty list without writing files."""
    transformer = SilverTransformer()
    empty_df = pl.DataFrame()
    saved = transformer.save_to_silver(empty_df, base_dir=tmp_path / "silver")
    assert saved == []


def test_extract_raw_calls_with_file_list(
    tmp_path: Path, raw_journey: dict[str, Any]
) -> None:
    """Extracting from a list of Path / str objects must succeed and combine rows."""
    file1 = tmp_path / "f1.json"
    file2 = tmp_path / "f2.json"

    j1 = raw_journey.copy()
    j1["DatedVehicleJourneyRef"] = {"value": "TRAIN_01"}
    file1.write_text(json.dumps(_wrap_siri_envelope([j1])), encoding="utf-8")

    j2 = raw_journey.copy()
    j2["DatedVehicleJourneyRef"] = {"value": "TRAIN_02"}
    file2.write_text(json.dumps(_wrap_siri_envelope([j2])), encoding="utf-8")

    transformer = SilverTransformer()
    # Test with list of Path
    df = transformer.extract_raw_calls([file1, file2])
    assert df.height == 2
    assert set(df["journey_id"].to_list()) == {"TRAIN_01", "TRAIN_02"}

    # Test with list of str
    df_str = transformer.extract_raw_calls([str(file1), str(file2)])
    assert df_str.height == 2
    assert set(df_str["journey_id"].to_list()) == {"TRAIN_01", "TRAIN_02"}
