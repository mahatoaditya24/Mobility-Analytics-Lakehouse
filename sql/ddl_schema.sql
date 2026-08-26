-- ============================================================================
-- Mobility Analytics Lakehouse DDL & Schema Definitions
-- Engine: Apache Spark SQL / Delta Lake 3.2 / Hive Metastore
-- Database: mobility_lakehouse
-- ============================================================================

CREATE DATABASE IF NOT EXISTS mobility_lakehouse
COMMENT 'Smart City Mobility & Telematics Real-Time Data Lakehouse'
LOCATION '/opt/spark/warehouse/mobility_lakehouse.db';

USE mobility_lakehouse;

-- ============================================================================
-- 1. BRONZE LAYER: Raw Telemetry Stream Archive
-- ============================================================================
CREATE TABLE IF NOT EXISTS traffic_bronze
USING DELTA
LOCATION '/opt/spark/warehouse/traffic_bronze'
COMMENT 'Raw immutable streaming telemetry events ingested from Kafka with ingestion metadata';

-- ============================================================================
-- 2. SILVER LAYER: Validated Clean Telemetry
-- ============================================================================
CREATE TABLE IF NOT EXISTS traffic_silver
USING DELTA
LOCATION '/opt/spark/warehouse/traffic_silver'
COMMENT 'Clean, deduplicated, and feature-engineered traffic telemetry events';

-- ============================================================================
-- 3. QUARANTINE / DEAD LETTER QUEUE (DLQ) LAYER
-- ============================================================================
CREATE TABLE IF NOT EXISTS traffic_quarantine
USING DELTA
LOCATION '/opt/spark/warehouse/traffic_quarantine'
COMMENT 'Quarantined telemetry anomalies violating Data Quality SLAs for auditability and replay';

-- ============================================================================
-- 4. GOLD LAYER: Star Schema Dimensional Model
-- ============================================================================

-- Dimension: Urban Zone
CREATE TABLE IF NOT EXISTS dim_zone
USING DELTA
LOCATION '/opt/spark/warehouse/dim_zone'
COMMENT 'Urban geographic zones, classifications, and traffic risk tiers';

-- Dimension: Road Segment
CREATE TABLE IF NOT EXISTS dim_road
USING DELTA
LOCATION '/opt/spark/warehouse/dim_road'
COMMENT 'Roadway segments, speed limits, lane capacity, and classifications';

-- Fact: Traffic Observations (Partitioned by event_date, event_hour)
CREATE TABLE IF NOT EXISTS fact_traffic
USING DELTA
LOCATION '/opt/spark/warehouse/fact_traffic'
COMMENT 'Core transactional fact table partitioned by date and hour for high-speed BI analytical queries';

-- Aggregated Rollup: Hourly Zone Congestion
CREATE TABLE IF NOT EXISTS agg_hourly_congestion
USING DELTA
LOCATION '/opt/spark/warehouse/agg_hourly_congestion'
COMMENT 'Pre-aggregated hourly traffic KPIs, speed violations, and congestion indices';

-- ============================================================================
-- 5. BI SERVING VIEWS (Hive / Thrift Server Optimized)
-- ============================================================================

CREATE OR REPLACE VIEW bi_fact_traffic AS
SELECT
    CAST(vehicle_id AS STRING) AS vehicle_id,
    CAST(road_id AS STRING) AS road_id,
    CAST(city_zone AS STRING) AS city_zone,
    CAST(speed_kmh AS DOUBLE) AS speed_kmh,
    CAST(congestion_level AS INT) AS congestion_level,
    CAST(congestion_risk_score AS INT) AS congestion_risk_score,
    CAST(speed_band AS STRING) AS speed_band,
    CAST(peak_flag AS INT) AS peak_flag,
    CAST(is_weekend AS INT) AS is_weekend,
    CAST(weather AS STRING) AS weather,
    CAST(vehicle_type AS STRING) AS vehicle_type,
    CAST(sensor_id AS STRING) AS sensor_id,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(event_ts AS TIMESTAMP) AS event_timestamp,
    CAST(event_date AS DATE) AS event_date,
    CAST(event_hour AS INT) AS event_hour
FROM fact_traffic;

CREATE OR REPLACE VIEW bi_dim_zone AS
SELECT
    CAST(city_zone AS STRING) AS city_zone,
    CAST(zone_type AS STRING) AS zone_type,
    CAST(traffic_risk_tier AS STRING) AS traffic_risk_tier
FROM dim_zone;

CREATE OR REPLACE VIEW bi_dim_road AS
SELECT
    CAST(road_id AS STRING) AS road_id,
    CAST(road_type AS STRING) AS road_type,
    CAST(speed_limit_kmh AS INT) AS speed_limit_kmh,
    CAST(lane_count AS INT) AS lane_count
FROM dim_road;

CREATE OR REPLACE VIEW bi_quarantine_audit AS
SELECT
    quarantine_reason,
    vehicle_id,
    road_id,
    city_zone,
    raw_speed,
    raw_event_time,
    quarantine_ts
FROM traffic_quarantine;
