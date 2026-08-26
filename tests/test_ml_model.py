"""
Unit Tests for Machine Learning Congestion Model & Anomaly Engine.
Compatible with both pytest and unittest.
"""

import unittest
from ml.congestion_model import MobilityAIEngine, AI_ENGINE


class TestMobilityAIEngine(unittest.TestCase):

    def setUp(self):
        self.engine = MobilityAIEngine()

    def test_predict_congestion_boundaries(self):
        """Ensure congestion predictions stay within valid 1.0 to 5.0 scale."""
        for zone in ["CBD", "TECHPARK", "AIRPORT", "SUBURB"]:
            for weather in ["CLEAR", "RAIN", "STORM"]:
                pred = self.engine.predict_congestion(
                    city_zone=zone,
                    road_id="R300",
                    weather=weather,
                    hour=9,
                    is_weekend=False
                )
                self.assertGreaterEqual(pred.predicted_congestion_level, 1.0)
                self.assertLessEqual(pred.predicted_congestion_level, 5.0)
                self.assertTrue(0.0 <= pred.gridlock_probability_pct <= 100.0)
                self.assertIn(pred.risk_category, ["LOW", "MODERATE", "SEVERE", "CRITICAL"])
                self.assertIsInstance(pred.recommended_action, str)

    def test_weather_increases_congestion(self):
        """Verify adverse weather (Storm) generates higher congestion than Clear weather."""
        pred_clear = self.engine.predict_congestion("CBD", "R300", "CLEAR", 9, False)
        pred_storm = self.engine.predict_congestion("CBD", "R300", "STORM", 9, False)
        self.assertGreater(pred_storm.predicted_congestion_level, pred_clear.predicted_congestion_level)

    def test_speed_anomaly_detection(self):
        """Verify detection of reckless speeding and stalled vehicle conditions."""
        # Normal driving
        normal = self.engine.detect_velocity_anomaly(speed_kmh=60, speed_limit_kmh=60, congestion_level=2)
        self.assertFalse(normal.is_anomaly)

        # Extreme Speeding
        speeding = self.engine.detect_velocity_anomaly(speed_kmh=110, speed_limit_kmh=60, congestion_level=2)
        self.assertTrue(speeding.is_anomaly)
        self.assertEqual(speeding.anomaly_type, "EXCESSIVE_SPEED_VIOLATION")

        # Stalled vehicle in clear traffic
        stalled = self.engine.detect_velocity_anomaly(speed_kmh=4, speed_limit_kmh=60, congestion_level=1)
        self.assertTrue(stalled.is_anomaly)
        self.assertEqual(stalled.anomaly_type, "UNEXPECTED_STOP_OR_BREAKDOWN")


if __name__ == "__main__":
    unittest.main()
