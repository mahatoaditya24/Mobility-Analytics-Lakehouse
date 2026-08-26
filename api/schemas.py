"""
Pydantic Schemas for FastAPI Endpoints.
Includes standard Pydantic models with graceful dataclass fallback for minimal test environments.
"""

from typing import Any, Dict, List, Optional

try:
    from pydantic import BaseModel, Field
except ImportError:
    # Graceful fallback for minimal environments without pydantic installed
    class BaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
        def dict(self):
            return self.__dict__

    def Field(default=..., **kwargs):
        return default


class TelemetryIngestRequest(BaseModel):
    vehicle_id: str
    road_id: str
    city_zone: str
    speed: int
    congestion_level: int
    weather: str = "CLEAR"
    vehicle_type: str = "SEDAN"
    latitude: Optional[float] = 12.9716
    longitude: Optional[float] = 77.5946


class TelemetryIngestResponse(BaseModel):
    status: str
    message: str
    vehicle_id: str
    ingested_at: str


class CongestionPredictionRequest(BaseModel):
    city_zone: str = "CBD"
    road_id: str = "R300"
    weather: str = "RAIN"
    hour: int = 9
    is_weekend: bool = False
    vehicle_density_factor: float = 1.2


class CongestionPredictionResponse(BaseModel):
    city_zone: str
    road_id: str
    predicted_congestion_level: float
    gridlock_probability_pct: float
    risk_category: str
    recommended_action: str
    feature_contributions: Dict[str, float]


class ZoneStatusResponse(BaseModel):
    city_zone: str
    zone_type: str
    traffic_risk_tier: str
    active_vehicles: int
    avg_speed_kmh: float
    current_congestion_level: float
    status: str


class SystemHealthResponse(BaseModel):
    status: str
    lakehouse_connected: bool
    kafka_broker_status: str
    active_streams: List[str]
    uptime_seconds: float
