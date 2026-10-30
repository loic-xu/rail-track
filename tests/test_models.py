from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError

from rail_track.ingestion.models import StopCall, TrainJourney


@pytest.fixture
def raw_nominal_call() -> dict[str, Any]:
    """Return a nominal SIRI-SX StopCall payload from production feeds.

    Represents an intermediate station call with fully populated arrival/departure
    schedules, platform indicators, and nested value dictionaries.
    """
    return {
        "StopPointRef": {"value": "STIF:StopArea:SP:43224:"},
        "ExpectedDepartureTime": "2026-10-12T14:04:44.000Z",
        "DestinationDisplay": [{"value": "Marne-la-Vallée Chessy"}],
        "ArrivalStopAssignment": {
            "ExpectedQuayRef": {"value": "STIF:StopPoint:Q:474000:"}
        },
        "DepartureStopAssignment": {
            "ExpectedQuayRef": {"value": "STIF:StopPoint:Q:474000:"}
        },
        "DeparturePlatformName": {"value": "1"},
        "ArrivalPlatformName": {"value": "1"},
        "DepartureStatus": "ON_TIME",
        "ExpectedArrivalTime": "2026-10-12T14:04:24.000Z",
        "AimedArrivalTime": "2026-10-12T14:04:24.000Z",
        "ArrivalStatus": "ON_TIME",
        "AimedDepartureTime": "2026-10-12T14:04:54.000Z",
    }


@pytest.fixture
def raw_nominal_train() -> dict[str, Any]:
    """Return a production-grade SIRI-SX EstimatedVehicleJourney payload.

    Simulates an active transit run (RER A) containing top-level route metadata,
    journey identifiers, vehicle composition, and sequential call points.
    """
    return {
        "RecordedAtTime": "2026-10-12T02:31:46.975Z",
        "LineRef": {"value": "STIF:Line::C01742:"},
        "DirectionRef": {"value": "Retour"},
        "DatedVehicleJourneyRef": {
            "value": (
                "SNCF_MAGENTA_PRD:VehicleJourney::"
                "f9f8d9ee-636b-4b0e-a365-d0cb796f0662:LOC"
            )
        },
        "VehicleMode": ["RAIL"],
        "VehicleFeatureRef": ["longTrain"],
        "RouteRef": {},
        "PublishedLineName": [{"value": "A"}],
        "DirectionName": [],
        "OriginRef": {"value": "STIF:StopArea:SP:43198:"},
        "OriginName": [{"value": "Saint-Germain-en-Laye"}],
        "DestinationRef": {"value": "STIF:StopArea:SP:43239:"},
        "DestinationName": [{"value": "Marne-la-Vallée Chessy"}],
        "OperatorRef": {},
        "ProductCategoryRef": {},
        "JourneyNote": [{"value": "QIKI"}],
        "EstimatedCalls": {
            "EstimatedCall": [
                {
                    "StopPointRef": {"value": "STIF:StopArea:SP:43224:"},
                    "ExpectedDepartureTime": "2026-10-12T14:04:44.000Z",
                    "DestinationDisplay": [{"value": "Marne-la-Vallée Chessy"}],
                    "ArrivalStopAssignment": {
                        "ExpectedQuayRef": {"value": "STIF:StopPoint:Q:474000:"}
                    },
                    "DepartureStopAssignment": {
                        "ExpectedQuayRef": {"value": "STIF:StopPoint:Q:474000:"}
                    },
                    "DeparturePlatformName": {"value": "1"},
                    "ArrivalPlatformName": {"value": "1"},
                    "DepartureStatus": "ON_TIME",
                    "ExpectedArrivalTime": "2026-10-12T14:04:24.000Z",
                    "AimedArrivalTime": "2026-10-12T14:04:24.000Z",
                    "ArrivalStatus": "ON_TIME",
                    "AimedDepartureTime": "2026-10-12T14:04:54.000Z",
                },
                {
                    "StopPointRef": {"value": "STIF:StopArea:SP:43213:"},
                    "ExpectedDepartureTime": "2026-10-12T14:34:31.000Z",
                    "DestinationDisplay": [{"value": "Marne-la-Vallée Chessy"}],
                    "ArrivalStopAssignment": {
                        "ExpectedQuayRef": {"value": "STIF:StopPoint:Q:474003:"}
                    },
                    "DepartureStopAssignment": {
                        "ExpectedQuayRef": {"value": "STIF:StopPoint:Q:474003:"}
                    },
                    "DeparturePlatformName": {"value": "1"},
                    "ArrivalPlatformName": {"value": "1"},
                    "DepartureStatus": "ON_TIME",
                    "ExpectedArrivalTime": "2026-10-12T14:34:11.000Z",
                    "AimedArrivalTime": "2026-10-12T14:34:11.000Z",
                    "ArrivalStatus": "ON_TIME",
                    "AimedDepartureTime": "2026-10-12T14:34:51.000Z",
                },
            ]
        },
    }


def test_stop_call_nominal(raw_nominal_call: dict[str, Any]) -> None:
    """Validate nominal StopCall deserialization, key extraction, and type coercion.

    Asserts that:
    - Nested SIRI structures ({'value': ...}) are unnested correctly.
    - Platform assignment captures the departure track.
    - ISO 8601 timestamp strings are coerced into timezone-aware datetime objects.
    """
    stop_call = StopCall.model_validate(raw_nominal_call)
    assert stop_call.stop_point_ref == "STIF:StopArea:SP:43224:"
    assert stop_call.platform == "1"
    assert stop_call.departure_status == "ON_TIME"
    assert isinstance(stop_call.aimed_arrival_time, datetime)
    assert isinstance(stop_call.expected_departure_time, datetime)


def test_stop_call_origin_boundary(raw_nominal_call: dict[str, Any]) -> None:
    """Verify schema resilience on trip origin boundaries.

    Trip origins omit arrival schedules by definition. Ensures that missing
    arrival metrics safely evaluate to None without breaking the ingestion contract.
    """
    excluded_keys = {"AimedArrivalTime", "ExpectedArrivalTime", "ArrivalStatus"}
    origin_payload = {
        k: v for k, v in raw_nominal_call.items() if k not in excluded_keys
    }

    call = StopCall.model_validate(origin_payload)

    assert call.aimed_arrival_time is None
    assert call.expected_arrival_time is None
    assert call.arrival_status is None

    assert call.expected_departure_time is not None
    assert call.platform == "1"


def test_stop_call_terminus_boundary(raw_nominal_call: dict[str, Any]) -> None:
    """Verify schema resilience on trip terminus boundaries.

    Trip terminations omit departure schedules and departure platform refs. Ensures
    the model gracefully handles null departure timestamps while preserving arrival data
    and platform fallback logic.
    """
    excluded_keys = {
        "AimedDepartureTime",
        "ExpectedDepartureTime",
        "DepartureStatus",
    }
    terminus_payload = {
        k: v for k, v in raw_nominal_call.items() if k not in excluded_keys
    }

    call = StopCall.model_validate(terminus_payload)

    assert call.aimed_departure_time is None
    assert call.expected_departure_time is None
    assert call.departure_status is None

    assert call.expected_arrival_time is not None
    assert call.platform == "1"


def test_stop_call_missing_mandatory_field(raw_nominal_call: dict[str, Any]) -> None:
    """Ensure validation fails when mandatory StopPointRef is absent."""
    invalid_payload = {k: v for k, v in raw_nominal_call.items() if k != "StopPointRef"}

    with pytest.raises(ValidationError):
        StopCall.model_validate(invalid_payload)


def test_train_journey_nominal(raw_nominal_train: dict[str, Any]) -> None:
    """Validate top-level metadata extraction and nested StopCall instantiation.

    Asserts that:
    - Scalar journey identifiers and operational tags are parsed.
    - Missing or default attributes resolve cleanly (e.g. vehicle_mode).
    - Child calls cascade into fully instantiated StopCall domain models.
    """
    train = TrainJourney.model_validate(raw_nominal_train)

    assert (
        train.journey_id
        == "SNCF_MAGENTA_PRD:VehicleJourney::f9f8d9ee-636b-4b0e-a365-d0cb796f0662:LOC"
    )
    assert train.line_name == "A"
    assert train.mission_code == "QIKI"
    assert train.direction == "Retour"
    assert train.origin_name == "Saint-Germain-en-Laye"
    assert train.destination_name == "Marne-la-Vallée Chessy"
    assert train.vehicle_mode == "RAIL"
    assert train.train_length == "longTrain"

    assert len(train.calls) == 2
    assert all(isinstance(call, StopCall) for call in train.calls)
    assert train.calls[0].stop_point_ref == "STIF:StopArea:SP:43224:"


def test_train_journey_missing_mandatory_field(
    raw_nominal_train: dict[str, Any],
) -> None:
    """Verify that omitting mandatory journey identifiers raises a ValidationError."""
    payload = {
        k: v for k, v in raw_nominal_train.items() if k != "DatedVehicleJourneyRef"
    }

    with pytest.raises(ValidationError):
        TrainJourney.model_validate(payload)


@pytest.mark.parametrize(
    "raw_calls_payload, expected_length",
    [
        # Standard array of multiple stop points
        (
            [
                {"StopPointRef": {"value": "STIF:StopArea:SP:1:"}},
                {"StopPointRef": {"value": "STIF:StopArea:SP:2:"}},
            ],
            2,
        ),
        # Single stop point provided as an unnested dict
        ({"StopPointRef": {"value": "STIF:StopArea:SP:1:"}}, 1),
        # Empty array
        ([], 0),
    ],
)
def test_train_journey_calls_polymorphism(
    raw_nominal_train: dict[str, Any],
    raw_calls_payload: Any,
    expected_length: int,
) -> None:
    """Verify handling of polymorphic SIRI EstimatedCall payloads.

    SIRI-SX feeds oscillate between lists, single dicts, or empty payloads
    depending on upstream serialization. Ensures calls are normalized into a list.
    """
    payload = raw_nominal_train.copy()
    payload["EstimatedCalls"] = {"EstimatedCall": raw_calls_payload}

    train = TrainJourney.model_validate(payload)

    assert isinstance(train.calls, list)
    assert len(train.calls) == expected_length
