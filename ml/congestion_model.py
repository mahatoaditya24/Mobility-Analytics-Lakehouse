"""
Real-Time Congestion Prediction & Speed Anomaly Detection Engine.
Provides heuristic and ML-based inference for gridlock forecasting and traffic hazard identification.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import math
import random


@dataclass
class CongestionPrediction:
    predicted_congestion_level: float
    gridlock_probability_pct: float
    risk_category: str  # LOW, MODERATE, SEVERE, CRITICAL
    recommended_action: str
    feature_contributions: Dict[str, float]


@dataclass
class AnomalyResult:
    is_anomaly: bool
    anomaly_score: float  # 0.0 (Normal) to 1.0 (Extreme Outlier)
    anomaly_type: Optional[str]
    severity: str


class MobilityAIEngine:
    """Intelligent inference engine for smart city traffic networks."""

    WEATHER_WEIGHTS = {
        "CLEAR": 1.0,
        "RAIN": 1.4,
        "FOG": 1.6,
        "STORM": 2.1,
        "SNOW": 2.4
    }

    ZONE_BASE_LOAD = {
        "CBD": 3.2,
        "TECHPARK": 2.8,
        "TRAINSTATION": 2.9,
        "AIRPORT": 2.4,
        "HARBOR": 1.8,
        "SUBURB": 1.4
    }

    ROAD_CAPACITY_FACTOR = {
        "R100": 0.7,   # Highway
        "R200": 0.75,  # Highway
        "R300": 1.1,   # Arterial
        "R400": 1.3,   # City Road
        "R500": 1.5    # Local Street
    }

    def predict_congestion(
        self,
        city_zone: str,
        road_id: str,
        weather: str,
        hour: int,
        is_weekend: bool = False,
        vehicle_density_factor: float = 1.0
    ) -> CongestionPrediction:
        """
        Calculates predicted congestion level (1.0 to 5.0) and gridlock probability
        based on multi-factor telemetry features.
        """
        base = self.ZONE_BASE_LOAD.get(city_zone.upper(), 2.0)
        road_mult = self.ROAD_CAPACITY_FACTOR.get(road_id.upper(), 1.0)
        weather_mult = self.WEATHER_WEIGHTS.get(weather.upper(), 1.0)

        # Peak hour surge multiplier
        is_peak = (8 <= hour <= 11) or (17 <= hour <= 20)
        peak_mult = 1.45 if (is_peak and not is_weekend) else (0.85 if is_weekend else 1.0)

        # Compute raw index
        raw_score = (base * 0.45 + (hour % 12) * 0.08) * road_mult * (weather_mult ** 0.5) * peak_mult * vehicle_density_factor
        predicted_level = round(max(1.0, min(5.0, raw_score)), 2)

        # Sigmoidal Gridlock Probability
        gridlock_prob = round(1.0 / (1.0 + math.exp(-2.2 * (predicted_level - 3.4))) * 100, 1)

        if predicted_level >= 4.2:
            category = "CRITICAL"
            action = "Activate dynamic signal retiming & divert arterial traffic"
        elif predicted_level >= 3.4:
            category = "SEVERE"
            action = "Dispatch traffic marshals & trigger variable message signs (VMS)"
        elif predicted_level >= 2.4:
            category = "MODERATE"
            action = "Monitor ramp meters and maintain standard signal cycles"
        else:
            category = "LOW"
            action = "Optimal network flow. No intervention required"

        contributions = {
            "Zone Base Load": round(base * 0.3, 2),
            "Rush Hour Surge": round((1.45 if is_peak else 1.0) * 0.4, 2),
            "Weather Severity": round((weather_mult - 1.0) * 0.5, 2),
            "Road Capacity Impact": round((road_mult - 1.0) * 0.4, 2)
        }

        return CongestionPrediction(
            predicted_congestion_level=predicted_level,
            gridlock_probability_pct=gridlock_prob,
            risk_category=category,
            recommended_action=action,
            feature_contributions=contributions
        )

    def detect_velocity_anomaly(
        self,
        speed_kmh: float,
        speed_limit_kmh: float,
        congestion_level: int
    ) -> AnomalyResult:
        """Evaluates whether an individual vehicle observation is a behavioral anomaly."""
        speed_ratio = speed_kmh / max(10, speed_limit_kmh)

        # High Speed Anomaly (Reckless Speeding)
        if speed_kmh > speed_limit_kmh * 1.35:
            severity = "CRITICAL" if speed_kmh > speed_limit_kmh * 1.6 else "HIGH"
            score = min(1.0, (speed_kmh - speed_limit_kmh) / 50.0)
            return AnomalyResult(
                is_anomaly=True,
                anomaly_score=round(score, 2),
                anomaly_type="EXCESSIVE_SPEED_VIOLATION",
                severity=severity
            )

        # Unexpected Deadlock / Stalled Vehicle Anomaly
        if speed_kmh < 10 and congestion_level <= 2:
            return AnomalyResult(
                is_anomaly=True,
                anomaly_score=0.82,
                anomaly_type="UNEXPECTED_STOP_OR_BREAKDOWN",
                severity="HIGH"
            )

        return AnomalyResult(
            is_anomaly=False,
            anomaly_score=0.05,
            anomaly_type=None,
            severity="NORMAL"
        )


# Global AI Singleton
AI_ENGINE = MobilityAIEngine()
