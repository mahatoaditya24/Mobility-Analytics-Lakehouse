"""
Unit Tests for FastAPI Serving Microservice & Pydantic Schemas.
Compatible with both pytest and unittest.
"""

import unittest
from api.schemas import (
    CongestionPredictionRequest,
    TelemetryIngestRequest,
    ZoneStatusResponse
)
from api.main import (
    root,
    health_check,
    list_zone_telemetry,
    get_zone_details,
    predict_traffic_congestion
)


class TestMobilityAPI(unittest.TestCase):

    def test_root_endpoint(self):
        """Verify API root metadata response."""
        res = root()
        self.assertIn("platform", res)
        self.assertEqual(res["version"], "2.0.0")

    def test_health_endpoint(self):
        """Verify system health endpoint returns HEALTHY status."""
        health = health_check()
        self.assertEqual(health.status, "HEALTHY")
        self.assertTrue(health.lakehouse_connected)
        self.assertIn("BronzeIngestionStream", health.active_streams)

    def test_list_zones_endpoint(self):
        """Verify zone telemetry list contains all sectors."""
        zones = list_zone_telemetry()
        self.assertGreaterEqual(len(zones), 5)
        zone_names = [z.city_zone for z in zones]
        self.assertIn("CBD", zone_names)
        self.assertIn("TECHPARK", zone_names)

    def test_predict_congestion_endpoint(self):
        """Verify AI prediction endpoint produces valid response."""
        req = CongestionPredictionRequest(
            city_zone="CBD",
            road_id="R100",
            weather="RAIN",
            hour=9,
            is_weekend=False,
            vehicle_density_factor=1.1
        )
        res = predict_traffic_congestion(req)
        self.assertEqual(res.city_zone, "CBD")
        self.assertTrue(1.0 <= res.predicted_congestion_level <= 5.0)
        self.assertTrue(0.0 <= res.gridlock_probability_pct <= 100.0)

    def test_telemetry_schema_validation(self):
        """Verify Telemetry Ingestion Pydantic model enforces field constraints."""
        valid_payload = {
            "vehicle_id": "550e8400-e29b-41d4-a716-446655440000",
            "road_id": "R100",
            "city_zone": "CBD",
            "speed": 65,
            "congestion_level": 3
        }
        model = TelemetryIngestRequest(**valid_payload)
        self.assertEqual(model.speed, 65)
        self.assertEqual(model.city_zone, "CBD")


if __name__ == "__main__":
    unittest.main()
