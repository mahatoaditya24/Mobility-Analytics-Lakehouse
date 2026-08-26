"""
Unit Tests for Silver & Gold Layer Feature Engineering & Dimensional Transformations.
Compatible with both pytest and unittest.
"""

import unittest


def calculate_peak_flag(hour: int) -> int:
    """Calculates peak rush hour flag."""
    return 1 if (8 <= hour <= 11 or 17 <= hour <= 20) else 0


def calculate_speed_band(speed_kmh: int) -> str:
    """Calculates categorical speed band."""
    if speed_kmh < 30:
        return "LOW_SPEED"
    elif speed_kmh < 70:
        return "MEDIUM_SPEED"
    return "HIGH_SPEED"


def calculate_congestion_risk(congestion_level: int) -> int:
    """Calculates congestion risk tier score."""
    if congestion_level >= 4:
        return 3
    elif congestion_level == 3:
        return 2
    return 1


def get_zone_type(city_zone: str) -> str:
    """Maps zone identifier to descriptive classification."""
    if city_zone == "CBD":
        return "Commercial / Financial"
    elif city_zone == "TECHPARK":
        return "IT & Technology Hub"
    elif city_zone in ["AIRPORT", "TRAINSTATION", "HARBOR"]:
        return "Multimodal Transit Hub"
    return "Residential & Suburban"


class TestTransformations(unittest.TestCase):

    def test_peak_hour_flagging(self):
        """Verify peak rush hour identification for morning and evening peaks."""
        # Morning Peak (8 to 11)
        self.assertEqual(calculate_peak_flag(8), 1)
        self.assertEqual(calculate_peak_flag(9), 1)
        self.assertEqual(calculate_peak_flag(11), 1)

        # Evening Peak (17 to 20)
        self.assertEqual(calculate_peak_flag(17), 1)
        self.assertEqual(calculate_peak_flag(19), 1)
        self.assertEqual(calculate_peak_flag(20), 1)

        # Off Peak
        self.assertEqual(calculate_peak_flag(2), 0)
        self.assertEqual(calculate_peak_flag(14), 0)
        self.assertEqual(calculate_peak_flag(23), 0)

    def test_speed_band_categorization(self):
        """Verify velocity categorization into Low, Medium, and High bands."""
        self.assertEqual(calculate_speed_band(15), "LOW_SPEED")
        self.assertEqual(calculate_speed_band(29), "LOW_SPEED")
        self.assertEqual(calculate_speed_band(30), "MEDIUM_SPEED")
        self.assertEqual(calculate_speed_band(65), "MEDIUM_SPEED")
        self.assertEqual(calculate_speed_band(70), "HIGH_SPEED")
        self.assertEqual(calculate_speed_band(120), "HIGH_SPEED")

    def test_congestion_risk_scoring(self):
        """Verify risk scoring mapping from congestion level (1-5)."""
        self.assertEqual(calculate_congestion_risk(1), 1)
        self.assertEqual(calculate_congestion_risk(2), 1)
        self.assertEqual(calculate_congestion_risk(3), 2)
        self.assertEqual(calculate_congestion_risk(4), 3)
        self.assertEqual(calculate_congestion_risk(5), 3)

    def test_zone_type_mapping(self):
        """Verify dimensional zone categorization."""
        self.assertEqual(get_zone_type("CBD"), "Commercial / Financial")
        self.assertEqual(get_zone_type("TECHPARK"), "IT & Technology Hub")
        self.assertEqual(get_zone_type("AIRPORT"), "Multimodal Transit Hub")
        self.assertEqual(get_zone_type("SUBURB"), "Residential & Suburban")


if __name__ == "__main__":
    unittest.main()
