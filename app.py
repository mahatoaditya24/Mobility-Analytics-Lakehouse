"""
Mobility Lakehouse Real-Time Analytics & Observability Dashboard.
Built with Streamlit, Plotly & PyDeck. Provides executive KPIs, 3D geospatial GPS fleet tracking,
AI predictive congestion forecasting, real-time data quality monitoring, and DLQ quarantine error audit logs.
"""

import os
import random
import time
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

# =============================================================================
# SELF-CONTAINED AI CONGESTION & ANOMALY PREDICTIVE ENGINE
# =============================================================================
@dataclass
class CongestionPrediction:
    predicted_congestion_level: float
    gridlock_probability_pct: float
    risk_category: str
    recommended_action: str
    feature_contributions: Dict[str, float]

class MobilityAIEngine:
    WEATHER_WEIGHTS = {"CLEAR": 1.0, "RAIN": 1.4, "FOG": 1.6, "STORM": 2.1, "SNOW": 2.4}
    ZONE_BASE_LOAD = {"CBD": 3.2, "TECHPARK": 2.8, "TRAINSTATION": 2.9, "AIRPORT": 2.4, "HARBOR": 1.8, "SUBURB": 1.4}
    ROAD_CAPACITY_FACTOR = {"R100": 0.7, "R200": 0.75, "R300": 1.1, "R400": 1.3, "R500": 1.5}

    def predict_congestion(self, city_zone: str, road_id: str, weather: str, hour: int, is_weekend: bool = False, vehicle_density_factor: float = 1.0) -> CongestionPrediction:
        base = self.ZONE_BASE_LOAD.get(city_zone.upper(), 2.0)
        road_mult = self.ROAD_CAPACITY_FACTOR.get(road_id.upper(), 1.0)
        weather_mult = self.WEATHER_WEIGHTS.get(weather.upper(), 1.0)
        is_peak = (8 <= hour <= 11) or (17 <= hour <= 20)
        peak_mult = 1.45 if (is_peak and not is_weekend) else (0.85 if is_weekend else 1.0)

        raw_score = (base * 0.45 + (hour % 12) * 0.08) * road_mult * (weather_mult ** 0.5) * peak_mult * vehicle_density_factor
        predicted_level = round(max(1.0, min(5.0, raw_score)), 2)
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

AI_ENGINE = MobilityAIEngine()

# Page Configuration
st.set_page_config(
    page_title="Mobility Lakehouse | Real-Time Traffic, AI & DQ Ops",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern dark-mode aesthetic
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 18px 24px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    }
    .metric-title {
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #94a3b8;
        margin-bottom: 6px;
    }
    .metric-value {
        font-size: 1.9rem;
        font-weight: 700;
        color: #f8fafc;
    }
    .metric-delta {
        font-size: 0.85rem;
        font-weight: 500;
        margin-top: 4px;
    }
    .delta-green { color: #10b981; }
    .delta-red { color: #ef4444; }
    .delta-blue { color: #38bdf8; }
    .delta-yellow { color: #f59e0b; }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# DATA GENERATION / MOCK DATA ENGINE (FOR ZERO-DEPENDENCY DEMOS)
# =============================================================================
@st.cache_data(ttl=5)
def generate_sample_lakehouse_data(n_events: int = 600):
    """Generates synthetic mobility data matching the Gold, Silver, and Quarantine schemas with GPS coords."""
    roads = ["R100", "R200", "R300", "R400", "R500"]
    road_types = {"R100": "Highway", "R200": "Highway", "R300": "Arterial", "R400": "City Road", "R500": "Local Street"}
    speed_limits = {"R100": 100, "R200": 100, "R300": 60, "R400": 50, "R500": 40}
    zones = ["CBD", "AIRPORT", "TECHPARK", "SUBURB", "TRAINSTATION", "HARBOR"]
    weather_list = ["CLEAR", "RAIN", "FOG", "STORM", "SNOW"]
    vehicle_types = ["SEDAN", "SUV", "TRUCK", "BUS", "MOTORCYCLE", "EV_TAXI"]

    zone_coords = {
        "CBD": (12.9716, 77.5946),
        "TECHPARK": (12.9352, 77.6946),
        "AIRPORT": (13.1986, 77.7066),
        "SUBURB": (12.9121, 77.6446),
        "TRAINSTATION": (12.9784, 77.5696),
        "HARBOR": (13.0100, 77.5500)
    }

    now = datetime.utcnow()
    records = []
    quarantine_records = []

    for i in range(n_events):
        road = random.choice(roads)
        zone = random.choice(zones)
        weather = random.choice(weather_list)
        v_type = random.choice(vehicle_types)
        time_offset = timedelta(minutes=random.randint(0, 180))
        event_time = now - time_offset

        base_lat, base_lon = zone_coords.get(zone, (12.9716, 77.5946))
        lat = base_lat + random.uniform(-0.02, 0.02)
        lon = base_lon + random.uniform(-0.02, 0.02)

        is_anomaly = random.random() < 0.20

        if is_anomaly:
            reason = random.choice([
                "SPEED_OUT_OF_BOUNDS",
                "FUTURE_TIMESTAMP_ANOMALY",
                "CORRUPT_PAYLOAD",
                "MISSING_VEHICLE_ID",
                "EXCESSIVE_LATENCY"
            ])
            raw_speed = random.choice([-30, 290, "FAST", None])
            quarantine_records.append({
                "vehicle_id": f"VEH-{random.randint(100, 999)}" if reason != "MISSING_VEHICLE_ID" else None,
                "road_id": road,
                "city_zone": zone,
                "raw_speed": str(raw_speed),
                "raw_event_time": (now + timedelta(hours=1)).isoformat() if reason == "FUTURE_TIMESTAMP_ANOMALY" else event_time.isoformat(),
                "quarantine_reason": reason,
                "quarantine_ts": now.strftime("%Y-%m-%d %H:%M:%S")
            })
        else:
            speed_limit = speed_limits[road]
            congestion = random.randint(1, 5)
            speed = max(15, min(140, int(speed_limit * (1.2 - congestion * 0.15) * random.uniform(0.8, 1.1))))
            hour = event_time.hour
            peak_flag = 1 if (8 <= hour <= 11 or 17 <= hour <= 20) else 0

            records.append({
                "vehicle_id": f"VEH-{random.randint(1000, 9999)}",
                "road_id": road,
                "road_type": road_types[road],
                "speed_limit_kmh": speed_limit,
                "city_zone": zone,
                "speed_kmh": speed,
                "congestion_level": congestion,
                "congestion_risk_score": 3 if congestion >= 4 else (2 if congestion == 3 else 1),
                "speed_band": "LOW_SPEED" if speed < 30 else ("MEDIUM_SPEED" if speed < 70 else "HIGH_SPEED"),
                "peak_flag": peak_flag,
                "weather": weather,
                "vehicle_type": v_type,
                "latitude": lat,
                "longitude": lon,
                "event_ts": event_time,
                "event_hour": hour,
                "event_date": event_time.date(),
                "is_speeding": 1 if speed > speed_limit else 0
            })

    return pd.DataFrame(records), pd.DataFrame(quarantine_records)


df_clean, df_quarantine = generate_sample_lakehouse_data(600)

# =============================================================================
# SIDEBAR CONTROLS
# =============================================================================
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/traffic-light.png", width=64)
    st.title("Mobility Lakehouse")
    st.caption("Real-Time Telemetry & AI Ops")
    st.markdown("---")

    st.subheader("⚙️ Filter Pipeline View")
    selected_zones = st.multiselect(
        "City Zones",
        options=sorted(df_clean["city_zone"].unique()),
        default=sorted(df_clean["city_zone"].unique())
    )

    selected_roads = st.multiselect(
        "Road Segments",
        options=sorted(df_clean["road_id"].unique()),
        default=sorted(df_clean["road_id"].unique())
    )

    weather_filter = st.multiselect(
        "Weather Condition",
        options=sorted(df_clean["weather"].unique()),
        default=sorted(df_clean["weather"].unique())
    )

    st.markdown("---")
    st.caption("📌 **Tech Stack:** `Spark 3.5` | `Delta Lake 3.2` | `Kafka KRaft` | `FastAPI` | `Streamlit`")


# Apply Filters
filtered_df = df_clean[
    (df_clean["city_zone"].isin(selected_zones)) &
    (df_clean["road_id"].isin(selected_roads)) &
    (df_clean["weather"].isin(weather_filter))
]


# =============================================================================
# TOP KPI BANNER
# =============================================================================
st.title("🚦 Smart City Mobility Analytics & Real-Time Lakehouse")
st.markdown("Distributed Kafka streaming, Delta Lake Medallion architecture, AI Congestion Forecasting, and Dead-Letter Queue (DLQ) quarantine monitoring.")

total_clean = len(filtered_df)
total_quarantined = len(df_quarantine)
total_events = total_clean + total_quarantined
dq_pass_rate = (total_clean / total_events * 100) if total_events > 0 else 100
avg_velocity = filtered_df["speed_kmh"].mean() if not filtered_df.empty else 0
high_congestion_pct = (filtered_df["congestion_level"].apply(lambda x: 1 if x >= 4 else 0).mean() * 100) if not filtered_df.empty else 0

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Live Ingestion Flow</div>
        <div class="metric-value">{total_events:,}</div>
        <div class="metric-delta delta-blue">⚡ Streaming via Kafka</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">DQ Pass Rate (SLA)</div>
        <div class="metric-value">{dq_pass_rate:.1f}%</div>
        <div class="metric-delta delta-green">🛡️ Clean Silver Events</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Quarantined (DLQ)</div>
        <div class="metric-value">{total_quarantined}</div>
        <div class="metric-delta delta-red">⚠️ Anomalies Isolated</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Average Fleet Speed</div>
        <div class="metric-value">{avg_velocity:.1f} <span style="font-size:1.1rem;color:#94a3b8">km/h</span></div>
        <div class="metric-delta delta-blue">🚗 Network Velocity</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">High Congestion Rate</div>
        <div class="metric-value">{high_congestion_pct:.1f}%</div>
        <div class="metric-delta {'delta-red' if high_congestion_pct > 25 else 'delta-green'}">🔴 Level 4-5 Zones</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# =============================================================================
# DASHBOARD TABS
# =============================================================================
tab_map, tab1, tab_ai, tab2, tab3, tab4 = st.tabs([
    "🗺️ 3D Geospatial GPS Fleet Tracking",
    "📊 City Traffic & Congestion Ops",
    "🤖 AI Congestion Predictor",
    "🛡️ Data Quality & DLQ Observability",
    "⭐ Gold Star Schema & Rollups",
    "💻 Interactive SQL Workbench"
])

# =============================================================================
# TAB MAP: 3D GEOSPATIAL FLEET TRACKING
# =============================================================================
with tab_map:
    st.subheader("🗺️ Real-Time 3D Geospatial Vehicle Telemetry Map")
    st.markdown("Live GPS observations rendered in 3D, color-coded by velocity bands with dynamic spatial clustering.")

    if not filtered_df.empty:
        def get_color(speed):
            if speed < 30:
                return [239, 68, 68, 180]  # Red
            elif speed < 70:
                return [245, 158, 11, 180]  # Yellow
            return [16, 185, 129, 180]  # Green

        map_df = filtered_df.copy()
        map_df["color"] = map_df["speed_kmh"].apply(get_color)

        layer_points = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position=["longitude", "latitude"],
            get_color="color",
            get_radius=120,
            pickable=True,
            auto_highlight=True
        )

        layer_heatmap = pdk.Layer(
            "HeatmapLayer",
            data=map_df,
            get_position=["longitude", "latitude"],
            get_weight="congestion_level",
            radius_pixels=60
        )

        view_state = pdk.ViewState(
            latitude=map_df["latitude"].mean(),
            longitude=map_df["longitude"].mean(),
            zoom=11,
            pitch=45
        )

        deck = pdk.Deck(
            layers=[layer_heatmap, layer_points],
            initial_view_state=view_state,
            tooltip={"html": "<b>Vehicle:</b> {vehicle_id}<br/><b>Zone:</b> {city_zone}<br/><b>Speed:</b> {speed_kmh} km/h<br/><b>Congestion:</b> Level {congestion_level}/5", "style": {"color": "white"}},
            map_style="dark"
        )

        st.pydeck_chart(deck)
    else:
        st.info("No vehicle coordinates match current filters.")


# =============================================================================
# TAB 1: TRAFFIC & CONGESTION
# =============================================================================
with tab1:
    col_a, col_b = st.columns([6, 4])

    with col_a:
        st.subheader("Zone Congestion & Average Speed Matrix")
        zone_summary = filtered_df.groupby("city_zone").agg(
            avg_speed=("speed_kmh", "mean"),
            avg_congestion=("congestion_level", "mean"),
            vehicle_count=("vehicle_id", "count")
        ).reset_index()

        fig_zone = px.bar(
            zone_summary,
            x="city_zone",
            y="avg_congestion",
            color="avg_speed",
            color_continuous_scale="Viridis",
            labels={"avg_congestion": "Avg Congestion Level (1-5)", "city_zone": "City Zone", "avg_speed": "Avg Speed (km/h)"},
            title="Real-Time Congestion Index by Urban Zone"
        )
        fig_zone.update_layout(template="plotly_dark", height=360)
        st.plotly_chart(fig_zone, use_container_width=True)

    with col_b:
        st.subheader("Speed Band Distribution")
        fig_speed = px.pie(
            filtered_df,
            names="speed_band",
            color="speed_band",
            color_discrete_map={"LOW_SPEED": "#ef4444", "MEDIUM_SPEED": "#f59e0b", "HIGH_SPEED": "#10b981"},
            hole=0.45,
            title="Active Fleet Velocity Breakdown"
        )
        fig_speed.update_layout(template="plotly_dark", height=360)
        st.plotly_chart(fig_speed, use_container_width=True)


# =============================================================================
# TAB AI: AI PREDICTIVE CONGESTION ENGINE
# =============================================================================
with tab_ai:
    st.subheader("🤖 AI Predictive Congestion & Gridlock Forecasting Engine")
    st.markdown("Run real-time scenario simulation to forecast traffic gridlock risks based on weather, rush hours, and vehicle density.")

    col_ai1, col_ai2 = st.columns([4, 6])

    with col_ai1:
        st.write("##### 🎛️ Scenario Simulation Parameters")
        sim_zone = st.selectbox("Target City Sector", options=["CBD", "TECHPARK", "TRAINSTATION", "AIRPORT", "HARBOR", "SUBURB"], index=0)
        sim_road = st.selectbox("Roadway Class", options=["R100", "R200", "R300", "R400", "R500"], format_func=lambda x: f"{x} (Highway)" if x in ["R100", "R200"] else f"{x} (Urban/Arterial)")
        sim_weather = st.selectbox("Simulated Weather", options=["CLEAR", "RAIN", "FOG", "STORM", "SNOW"], index=1)
        sim_hour = st.slider("Hour of Day (24-hr)", min_value=0, max_value=23, value=9)
        sim_weekend = st.checkbox("Is Weekend / Public Holiday", value=False)
        sim_density = st.slider("Vehicle Density Factor", min_value=0.5, max_value=2.5, value=1.2, step=0.1)

        pred = AI_ENGINE.predict_congestion(
            city_zone=sim_zone,
            road_id=sim_road,
            weather=sim_weather,
            hour=sim_hour,
            is_weekend=sim_weekend,
            vehicle_density_factor=sim_density
        )

    with col_ai2:
        st.write("##### 🔮 AI Inference Results")
        col_res1, col_res2, col_res3 = st.columns(3)
        with col_res1:
            st.metric("Predicted Congestion", f"{pred.predicted_congestion_level:.2f} / 5.0")
        with col_res2:
            st.metric("Gridlock Risk", f"{pred.gridlock_probability_pct:.1f}%")
        with col_res3:
            st.metric("Risk Category", pred.risk_category)

        st.info(f"💡 **Recommended Action:** {pred.recommended_action}")

        st.write("##### 📊 Feature Importance & Driver Contributions")
        feat_df = pd.DataFrame(list(pred.feature_contributions.items()), columns=["Feature Driver", "Contribution Score"])
        fig_feat = px.bar(
            feat_df,
            x="Contribution Score",
            y="Feature Driver",
            orientation="h",
            color="Contribution Score",
            color_continuous_scale="Blues"
        )
        fig_feat.update_layout(template="plotly_dark", height=220)
        st.plotly_chart(fig_feat, use_container_width=True)


# =============================================================================
# TAB 2: DATA QUALITY & DLQ QUARANTINE
# =============================================================================
with tab2:
    st.subheader("🛡️ Enterprise Data Quality Observability & Quarantine Error Taxonomy")
    st.markdown("All incoming raw Kafka messages are validated against SLA rules. Non-compliant records are segregated into the **Delta Quarantine Table (DLQ)**.")

    col_dq1, col_dq2 = st.columns([4, 6])

    with col_dq1:
        if not df_quarantine.empty:
            dq_counts = df_quarantine["quarantine_reason"].value_counts().reset_index()
            dq_counts.columns = ["quarantine_reason", "count"]
            fig_dq = px.pie(
                dq_counts,
                names="quarantine_reason",
                values="count",
                hole=0.4,
                title="Quarantine Reason Distribution",
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_dq.update_layout(template="plotly_dark", height=340)
            st.plotly_chart(fig_dq, use_container_width=True)

    with col_dq2:
        st.write("##### Data Quality Rule SLA Matrix")
        st.dataframe(pd.DataFrame({
            "DQ Rule": ["Speed Range Check", "Timestamp Freshness", "Payload Integrity", "Vehicle Identifier", "Watermark Latency"],
            "Target Boundary": ["0 km/h <= speed <= 180 km/h", "ts <= now() + 10 mins", "Valid UTF-8 JSON schema", "Non-null UUID string", "ts >= now() - 3 hours"],
            "Violation Action": ["Route to DLQ", "Route to DLQ", "Route to DLQ", "Route to DLQ", "Route to DLQ"],
            "SLA Target": ["99.5%", "99.9%", "99.99%", "99.0%", "98.5%"]
        }), use_container_width=True)

    st.subheader("Recent Quarantined Records (DLQ Audit Log)")
    if not df_quarantine.empty:
        st.dataframe(
            df_quarantine[["vehicle_id", "road_id", "city_zone", "raw_speed", "quarantine_reason", "quarantine_ts"]].head(25),
            use_container_width=True
        )


# =============================================================================
# TAB 3: GOLD LAYER & HOURLY ROLLUPS
# =============================================================================
with tab3:
    st.subheader("⭐ Star Schema Gold Layer: Fact & Dimension Analytics")
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        st.write("##### Peak vs. Off-Peak Congestion Profiling")
        hourly_trend = filtered_df.groupby(["event_hour", "peak_flag"])["congestion_level"].mean().reset_index()
        fig_hourly = px.line(
            hourly_trend,
            x="event_hour",
            y="congestion_level",
            markers=True,
            title="Hourly Congestion Intensity Trend Curve",
            labels={"event_hour": "Hour of Day (0-23)", "congestion_level": "Avg Congestion Level"}
        )
        fig_hourly.update_layout(template="plotly_dark", height=320)
        st.plotly_chart(fig_hourly, use_container_width=True)

    with col_g2:
        st.write("##### Fleet Vehicle Distribution")
        v_dist = filtered_df["vehicle_type"].value_counts().reset_index()
        v_dist.columns = ["vehicle_type", "count"]
        fig_v = px.bar(
            v_dist,
            x="vehicle_type",
            y="count",
            color="vehicle_type",
            title="Active Vehicle Fleet Classification",
            labels={"count": "Observed Frequency", "vehicle_type": "Vehicle Type"}
        )
        fig_v.update_layout(template="plotly_dark", height=320)
        st.plotly_chart(fig_v, use_container_width=True)


# =============================================================================
# TAB 4: SQL WORKBENCH
# =============================================================================
with tab4:
    st.subheader("💻 Interactive SQL Query Workbench")
    st.caption("Query the Delta Lakehouse Metastore views directly.")

    preset_query = st.selectbox(
        "Select Analytical Template Query:",
        options=[
            "Top 5 Most Congested Urban Zones During Peak Hours",
            "Speed Limit Violation Rate by Road Type",
            "Weather Impact on Traffic Speed and Congestion"
        ]
    )

    if preset_query == "Top 5 Most Congested Urban Zones During Peak Hours":
        query_sql = """
SELECT city_zone, AVG(congestion_level) AS avg_congestion, COUNT(vehicle_id) AS total_vehicles
FROM fact_traffic WHERE peak_flag = 1 GROUP BY city_zone ORDER BY avg_congestion DESC LIMIT 5;
        """
        res_df = filtered_df[filtered_df["peak_flag"] == 1].groupby("city_zone").agg(
            avg_congestion=("congestion_level", "mean"),
            total_vehicles=("vehicle_id", "count")
        ).reset_index().sort_values("avg_congestion", ascending=False).head(5)
    elif preset_query == "Speed Limit Violation Rate by Road Type":
        query_sql = """
SELECT r.road_type, r.speed_limit_kmh, COUNT(f.vehicle_id) AS total_obs, SUM(f.is_speeding) AS speeding_count
FROM fact_traffic f JOIN dim_road r ON f.road_id = r.road_id GROUP BY r.road_type, r.speed_limit_kmh;
        """
        res_df = filtered_df.groupby(["road_type", "speed_limit_kmh"]).agg(
            total_obs=("vehicle_id", "count"),
            speeding_count=("is_speeding", "sum")
        ).reset_index()
    else:
        query_sql = "SELECT weather, AVG(speed_kmh) AS avg_speed FROM fact_traffic GROUP BY weather;"
        res_df = filtered_df.groupby("weather")["speed_kmh"].mean().reset_index()

    st.code(query_sql, language="sql")
    st.write("##### Query Results:")
    st.dataframe(res_df, use_container_width=True)


# Footer
st.markdown("---")
st.caption("Mobility Analytics Lakehouse Portfolio | Engineered with PySpark, Delta Lake, Apache Kafka, FastAPI & Streamlit")
