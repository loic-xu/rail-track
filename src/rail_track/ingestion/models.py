from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, model_validator


def _extract_val(raw: Any) -> Any:
    if isinstance(raw, dict):
        return raw.get("value")
    if isinstance(raw, list):
        return _extract_val(raw[0]) if raw else None
    return raw


class StopCall(BaseModel):
    stop_point_ref: str
    platform: str | None = None
    aimed_arrival_time: datetime | None = None
    expected_arrival_time: datetime | None = None
    arrival_status: str | None = None
    aimed_departure_time: datetime | None = None
    expected_departure_time: datetime | None = None
    departure_status: str | None = None

    @model_validator(mode="before")
    @classmethod
    def flatten_siri_call(cls, data: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(data, dict):
            return data

        return {
            "stop_point_ref": _extract_val(data.get("StopPointRef")),
            "platform": _extract_val(data.get("DeparturePlatformName"))
            or _extract_val(data.get("ArrivalPlatformName")),
            "aimed_arrival_time": _extract_val(data.get("AimedArrivalTime")),
            "expected_arrival_time": _extract_val(data.get("ExpectedArrivalTime")),
            "arrival_status": _extract_val(data.get("ArrivalStatus")),
            "aimed_departure_time": _extract_val(data.get("AimedDepartureTime")),
            "expected_departure_time": _extract_val(data.get("ExpectedDepartureTime")),
            "departure_status": _extract_val(data.get("DepartureStatus")),
        }


class TrainJourney(BaseModel):
    journey_id: str
    line_name: str
    mission_code: str | None = None
    direction: str
    origin_name: str | None = None
    destination_name: str | None = None
    vehicle_mode: str = "RAIL"
    train_length: str | None = None
    calls: list[StopCall] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def flatten_siri_train(cls, data: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(data, dict):
            return data

        raw_calls = (data.get("EstimatedCalls") or {}).get("EstimatedCall", [])
        if isinstance(raw_calls, dict):
            raw_calls = [raw_calls]

        return {
            "journey_id": _extract_val(data.get("DatedVehicleJourneyRef")),
            "line_name": _extract_val(data.get("PublishedLineName")),
            "mission_code": _extract_val(data.get("JourneyNote")),
            "direction": _extract_val(data.get("DirectionRef")),
            "origin_name": _extract_val(data.get("OriginName")),
            "destination_name": _extract_val(data.get("DestinationName")),
            "vehicle_mode": _extract_val(data.get("VehicleMode")) or "RAIL",
            "train_length": _extract_val(data.get("VehicleFeatureRef")),
            "calls": raw_calls,
        }
