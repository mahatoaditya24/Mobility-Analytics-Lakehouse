"""
Unit Tests for IoT Traffic Event Producer.
Verifies payload schemas, value ranges, and dirty anomaly generation mechanics.
Compatible with both pytest and unittest.
"""

import unittest
from producer.traffic_producer import (
    generate_clean_event,
    generate_dirty_event,
    ROADS,
    ZONES,
    WEATHER_CONDITIONS,
    VEHICLE_TYPES
)


class TestTrafficProducer(unittest.TestCase):

    def test_clean_event_schema(self):
        """Verify that clean events contain all mandatory fields with correct data types."""
        event = generate_clean_event()

        required_keys = [
            "vehicle_id", "road_id", "city_zone", "speed",
            "congestion_level", "weather", "vehicle_type",
            "sensor_id", "latitude", "longitude", "event_time"
        ]

        for key in required_keys:
            self.assertIn(key, event, f"Missing required key: {key}")

        self.assertIsInstance(event["vehicle_id"], str)
        self.assertTrue(len(event["vehicle_id"]) > 0)
        self.assertIn(event["road_id"], [r["road_id"] for r in ROADS])
        self.assertIn(event["city_zone"], ZONES)
        self.assertIn(event["weather"], WEATHER_CONDITIONS)
        self.assertIn(event["vehicle_type"], VEHICLE_TYPES)
        self.assertTrue(0 <= event["speed"] <= 180)
        self.assertTrue(1 <= event["congestion_level"] <= 5)
        self.assertTrue(-90.0 <= event["latitude"] <= 90.0)
        self.assertTrue(-180.0 <= event["longitude"] <= 180.0)

    def test_clean_event_speed_boundaries(self):
        """Ensure clean generated speeds are strictly positive and realistic."""
        for _ in range(50):
            event = generate_clean_event()
            self.assertGreaterEqual(event["speed"], 10)
            self.assertLessEqual(event["speed"], 160)

    def test_dirty_event_anomalies(self):
        """Verify that dirty generator outputs known anomaly types."""
        anomalies_detected = set()

        for _ in range(150):
            event = generate_dirty_event()
            if isinstance(event, str):
                anomalies_detected.add("corrupt_string")
            elif event.get("speed") is None:
                anomalies_detected.add("null_speed")
            elif isinstance(event.get("speed"), int) and event["speed"] < 0:
                anomalies_detected.add("negative_speed")
            elif isinstance(event.get("speed"), int) and event["speed"] > 200:
                anomalies_detected.add("extreme_speed")
            elif isinstance(event.get("speed"), str):
                anomalies_detected.add("wrong_datatype")
            elif event.get("vehicle_id") is None:
                anomalies_detected.add("null_vehicle_id")
            elif "road_surface_temp_c" in event:
                anomalies_detected.add("schema_drift")

        # Ensure a rich variety of anomalies are generated
        self.assertGreaterEqual(len(anomalies_detected), 4)


if __name__ == "__main__":
    unittest.main()
