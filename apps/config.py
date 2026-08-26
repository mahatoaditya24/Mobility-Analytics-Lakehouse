"""
Centralized Configuration for Mobility Analytics Lakehouse Pipeline.
Loads settings from environment variables with production-ready defaults.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    external_bootstrap_servers: str = os.getenv("KAFKA_EXTERNAL_BOOTSTRAP_SERVERS", "localhost:29092")
    topic: str = os.getenv("KAFKA_TOPIC", "traffic-topic")
    partitions: int = int(os.getenv("KAFKA_PARTITIONS", "3"))
    replication_factor: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "latest")


@dataclass(frozen=True)
class SparkConfig:
    master_url: str = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    app_name_prefix: str = os.getenv("SPARK_APP_PREFIX", "MobilityLakehouse")
    delta_extension: str = "io.delta.sql.DeltaSparkSessionExtension"
    delta_catalog: str = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    hive_metastore_uri: str = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
    log_level: str = os.getenv("SPARK_LOG_LEVEL", "WARN")


@dataclass(frozen=True)
class LakehousePaths:
    base_warehouse: str = os.getenv("WAREHOUSE_PATH", "/opt/spark/warehouse")
    checkpoint_base: str = os.getenv("CHECKPOINT_PATH", "/opt/spark/warehouse/chk")

    # Bronze Layer
    bronze_table: str = f"{base_warehouse}/traffic_bronze"
    bronze_checkpoint: str = f"{checkpoint_base}/traffic_bronze"

    # Silver Layer (Clean & Quarantine DLQ)
    silver_table: str = f"{base_warehouse}/traffic_silver"
    silver_checkpoint: str = f"{checkpoint_base}/traffic_silver"
    quarantine_table: str = f"{base_warehouse}/traffic_quarantine"
    quarantine_checkpoint: str = f"{checkpoint_base}/traffic_quarantine"

    # Gold Layer (Star Schema & Rollups)
    dim_zone_table: str = f"{base_warehouse}/dim_zone"
    dim_zone_checkpoint: str = f"{checkpoint_base}/dim_zone"
    dim_road_table: str = f"{base_warehouse}/dim_road"
    dim_road_checkpoint: str = f"{checkpoint_base}/dim_road"
    fact_traffic_table: str = f"{base_warehouse}/fact_traffic"
    fact_traffic_checkpoint: str = f"{checkpoint_base}/fact_traffic"
    agg_hourly_table: str = f"{base_warehouse}/agg_hourly_congestion"
    agg_hourly_checkpoint: str = f"{checkpoint_base}/agg_hourly_congestion"


# Instantiate singleton configurations
KAFKA_CONFIG = KafkaConfig()
SPARK_CONFIG = SparkConfig()
PATHS = LakehousePaths()
