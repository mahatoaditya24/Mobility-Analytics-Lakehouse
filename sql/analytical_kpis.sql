-- ============================================================================
-- Mobility Analytics Lakehouse: Executive & Operational SQL Queries
-- Demonstrates high-performance dimensional querying, window functions, and rollups
-- ============================================================================

USE mobility_lakehouse;

-- ----------------------------------------------------------------------------
-- 1. Top 5 Most Congested Urban Zones During Peak Morning/Evening Hours
-- ----------------------------------------------------------------------------
SELECT 
    f.city_zone,
    z.zone_type,
    z.traffic_risk_tier,
    COUNT(f.vehicle_id) AS total_vehicles_observed,
    ROUND(AVG(f.speed_kmh), 1) AS avg_speed_kmh,
    ROUND(AVG(f.congestion_level), 2) AS avg_congestion_level,
    SUM(CASE WHEN f.congestion_level >= 4 THEN 1 ELSE 0 END) AS gridlock_events_count
FROM fact_traffic f
JOIN dim_zone z ON f.city_zone = z.city_zone
WHERE f.peak_flag = 1
GROUP BY f.city_zone, z.zone_type, z.traffic_risk_tier
ORDER BY avg_congestion_level DESC
LIMIT 5;

-- ----------------------------------------------------------------------------
-- 2. Speeding Violation Rate and Velocity Deficit by Road Class
-- ----------------------------------------------------------------------------
SELECT 
    r.road_id,
    r.road_type,
    r.speed_limit_kmh,
    COUNT(f.vehicle_id) AS total_sampled_vehicles,
    SUM(CASE WHEN f.speed_kmh > r.speed_limit_kmh THEN 1 ELSE 0 END) AS speeding_count,
    ROUND((SUM(CASE WHEN f.speed_kmh > r.speed_limit_kmh THEN 1 ELSE 0 END) * 100.0 / COUNT(f.vehicle_id)), 2) AS speeding_violation_rate_pct,
    ROUND(AVG(f.speed_kmh - r.speed_limit_kmh), 2) AS avg_speed_delta
FROM fact_traffic f
JOIN dim_road r ON f.road_id = r.road_id
GROUP BY r.road_id, r.road_type, r.speed_limit_kmh
ORDER BY speeding_violation_rate_pct DESC;

-- ----------------------------------------------------------------------------
-- 3. Weather Regime Impact on Traffic Flow & Velocity Reduction
-- ----------------------------------------------------------------------------
SELECT 
    weather,
    COUNT(vehicle_id) AS observation_volume,
    ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
    ROUND(AVG(congestion_level), 2) AS avg_congestion_level,
    ROUND(STDDEV(speed_kmh), 2) AS speed_variance
FROM fact_traffic
GROUP BY weather
ORDER BY avg_speed_kmh ASC;

-- ----------------------------------------------------------------------------
-- 4. Hourly Peak Traffic Curve & Rush-Hour Surge Multiplier
-- ----------------------------------------------------------------------------
WITH hourly_stats AS (
    SELECT 
        event_hour,
        peak_flag,
        COUNT(vehicle_id) AS vehicle_count,
        AVG(congestion_level) AS avg_congestion,
        AVG(speed_kmh) AS avg_speed
    FROM fact_traffic
    GROUP BY event_hour, peak_flag
)
SELECT 
    event_hour,
    CASE WHEN peak_flag = 1 THEN 'RUSH_HOUR' ELSE 'NORMAL' END AS period_type,
    vehicle_count,
    ROUND(avg_congestion, 2) AS avg_congestion,
    ROUND(avg_speed, 2) AS avg_speed_kmh,
    ROUND(vehicle_count * 100.0 / SUM(vehicle_count) OVER(), 2) AS pct_of_daily_volume
FROM hourly_stats
ORDER BY event_hour ASC;

-- ----------------------------------------------------------------------------
-- 5. Data Quality Quarantine SLA & Anomaly Root Cause Analysis
-- ----------------------------------------------------------------------------
SELECT 
    quarantine_reason,
    COUNT(*) AS total_rejected_events,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM traffic_quarantine), 2) AS pct_of_total_anomalies,
    MIN(quarantine_ts) AS first_detected_at,
    MAX(quarantine_ts) AS last_detected_at
FROM traffic_quarantine
GROUP BY quarantine_reason
ORDER BY total_rejected_events DESC;

-- ----------------------------------------------------------------------------
-- 6. Vehicle Fleet Emission & Congestion Distribution
-- ----------------------------------------------------------------------------
SELECT 
    vehicle_type,
    COUNT(vehicle_id) AS fleet_count,
    ROUND(AVG(speed_kmh), 2) AS avg_fleet_speed,
    ROUND(AVG(congestion_level), 2) AS avg_zone_congestion,
    ROUND(COUNT(vehicle_id) * 100.0 / (SELECT COUNT(*) FROM fact_traffic), 2) AS fleet_share_pct
FROM fact_traffic
GROUP BY vehicle_type
ORDER BY fleet_count DESC;
