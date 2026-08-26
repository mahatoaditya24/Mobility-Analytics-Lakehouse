"""
Unit Tests for Data Quality (DQ) Rules Engine & Quarantine (DLQ) Classification Logic.
Compatible with both pytest and unittest.
"""

from datetime import datetime, timedelta
import unittest


def evaluate_dq_rule(
    raw_payload: str,
    vehicle_id: str,
    speed: any,
    event_time_str: str
) -> str:
    """Python reference implementation of the Silver Layer DQ Rules Engine."""
    if "CORRUPT" in str(raw_payload) or str(raw_payload).startswith("###"):
        return "CORRUPT_PAYLOAD"

    if vehicle_id is None or str(vehicle_id).strip() == "":
        return "MISSING_VEHICLE_ID"

    if event_time_str is None:
        return "MISSING_OR_INVALID_TIMESTAMP"

    try:
        event_dt = datetime.fromisoformat(str(event_time_str))
    except (ValueError, TypeError):
        return "MISSING_OR_INVALID_TIMESTAMP"

    # Type & Range validation for speed
    try:
        speed_int = int(speed)
    except (ValueError, TypeError):
        return "SPEED_OUT_OF_BOUNDS"

    if speed_int < 0 or speed_int > 180:
        return "SPEED_OUT_OF_BOUNDS"

    now = datetime.utcnow()
    event_dt_naive = event_dt.replace(tzinfo=None) if event_dt.tzinfo else event_dt

    if event_dt_naive > now + timedelta(minutes=10):
        return "FUTURE_TIMESTAMP_ANOMALY"

    if event_dt_naive < now - timedelta(hours=3):
        return "EXCESSIVE_LATENCY"

    return "VALID"


class TestDataQualityRules(unittest.TestCase):

    def test_valid_record_passes_dq(self):
        """Verify that a standard valid record passes with status 'VALID'."""
        now_iso = datetime.utcnow().isoformat()
        status = evaluate_dq_rule(
            raw_payload='{"vehicle_id": "V123", "speed": 65}',
            vehicle_id="V123",
            speed=65,
            event_time_str=now_iso
        )
        self.assertEqual(status, "VALID")

    def test_missing_vehicle_id_quarantine(self):
        """Verify null or empty vehicle IDs are flagged for quarantine."""
        now_iso = datetime.utcnow().isoformat()
        self.assertEqual(evaluate_dq_rule("{}", None, 50, now_iso), "MISSING_VEHICLE_ID")
        self.assertEqual(evaluate_dq_rule("{}", "", 50, now_iso), "MISSING_VEHICLE_ID")

    def test_negative_speed_quarantine(self):
        """Verify negative speeds are quarantined."""
        now_iso = datetime.utcnow().isoformat()
        status = evaluate_dq_rule("{}", "V1", -30, now_iso)
        self.assertEqual(status, "SPEED_OUT_OF_BOUNDS")

    def test_excessive_speed_quarantine(self):
        """Verify extreme unrealistic speeds (>180 km/h) are quarantined."""
        now_iso = datetime.utcnow().isoformat()
        status = evaluate_dq_rule("{}", "V1", 250, now_iso)
        self.assertEqual(status, "SPEED_OUT_OF_BOUNDS")

    def test_corrupt_payload_quarantine(self):
        """Verify corrupt hex/binary payloads are quarantined."""
        now_iso = datetime.utcnow().isoformat()
        status = evaluate_dq_rule("###CORRUPT_PAYLOAD###", "V1", 50, now_iso)
        self.assertEqual(status, "CORRUPT_PAYLOAD")

    def test_future_timestamp_anomaly(self):
        """Verify events timestamped in the distant future are flagged."""
        future_iso = (datetime.utcnow() + timedelta(hours=2)).isoformat()
        status = evaluate_dq_rule("{}", "V1", 50, future_iso)
        self.assertEqual(status, "FUTURE_TIMESTAMP_ANOMALY")


if __name__ == "__main__":
    unittest.main()
