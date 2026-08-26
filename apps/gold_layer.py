"""
Gold Layer: Star Schema Dimensional Modeling & Fact Table Streaming.
Reads clean Silver Delta stream, models Dimension tables (dim_zone, dim_road) and writes
partitioned Fact tables (fact_traffic) optimized for high-performance BI reporting & ad-hoc SQL.
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_date,
    when
)

# Import centralized configuration
try:
    from config import PATHS, SPARK_CONFIG
except ImportError:
    from apps.config import PATHS, SPARK_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("GoldLayer")


def create_spark_session() -> SparkSession:
    """Initializes Spark Session with Delta Lake and Hive Metastore support."""
    logger.info("Initializing Spark Session for Gold Star Schema Modeling...")
    spark = (
        SparkSession.builder
        .appName(f"{SPARK_CONFIG.app_name_prefix}-GoldLayer")
        .master(SPARK_CONFIG.master_url)
        .config("spark.sql.extensions", SPARK_CONFIG.delta_extension)
        .config("spark.sql.catalog.spark_catalog", SPARK_CONFIG.delta_catalog)
        .config("spark.hadoop.hive.metastore.uris", SPARK_CONFIG.hive_metastore_uri)
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(SPARK_CONFIG.log_level)
    return spark


def run_gold_pipeline():
    """Builds and runs the Gold Dimensional Modeling streaming pipelines."""
    spark = create_spark_session()

    logger.info(f"Reading stream from Silver Delta table: {PATHS.silver_table}")

    # Read clean Silver Delta stream
    silver_stream = (
        spark.readStream
        .format("delta")
        .load(PATHS.silver_table)
    )

    # =========================================================================
    # 1. DIMENSION: dim_zone
    # =========================================================================
    dim_zone = (
        silver_stream.select("city_zone")
        .dropDuplicates(["city_zone"])
        .withColumn(
            "zone_type",
            when(col("city_zone") == "CBD", "Commercial / Financial")
            .when(col("city_zone") == "TECHPARK", "IT & Technology Hub")
            .when(col("city_zone").isin("AIRPORT", "TRAINSTATION", "HARBOR"), "Multimodal Transit Hub")
            .otherwise("Residential & Suburban")
        )
        .withColumn(
            "traffic_risk_tier",
            when(col("city_zone").isin("CBD", "AIRPORT", "TRAINSTATION"), "CRITICAL")
            .when(col("city_zone") == "TECHPARK", "HIGH")
            .when(col("city_zone") == "HARBOR", "MEDIUM")
            .otherwise("LOW")
        )
        .withColumn("dim_updated_at", current_timestamp())
    )

    logger.info(f"Writing dim_zone stream -> {PATHS.dim_zone_table}")
    zone_query = (
        dim_zone.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", PATHS.dim_zone_checkpoint)
        .option("path", PATHS.dim_zone_table)
        .queryName("DimZoneStream")
        .start()
    )

    # =========================================================================
    # 2. DIMENSION: dim_road
    # =========================================================================
    dim_road = (
        silver_stream.select("road_id")
        .dropDuplicates(["road_id"])
        .withColumn(
            "road_type",
            when(col("road_id").isin("R100", "R200"), "Expressway / Highway")
            .when(col("road_id") == "R300", "Major Arterial Road")
            .when(col("road_id") == "R400", "Urban Collector Road")
            .otherwise("Local Access Street")
        )
        .withColumn(
            "speed_limit_kmh",
            when(col("road_id").isin("R100", "R200"), 100)
            .when(col("road_id") == "R300", 60)
            .when(col("road_id") == "R400", 50)
            .otherwise(40)
        )
        .withColumn(
            "lane_count",
            when(col("road_id").isin("R100", "R200"), 4)
            .when(col("road_id") == "R300", 3)
            .when(col("road_id") == "R400", 2)
            .otherwise(1)
        )
        .withColumn("dim_updated_at", current_timestamp())
    )

    logger.info(f"Writing dim_road stream -> {PATHS.dim_road_table}")
    road_query = (
        dim_road.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", PATHS.dim_road_checkpoint)
        .option("path", PATHS.dim_road_table)
        .queryName("DimRoadStream")
        .start()
    )

    # =========================================================================
    # 3. FACT TABLE: fact_traffic (Partitioned by event_date, event_hour)
    # =========================================================================
    fact_stream = (
        silver_stream.select(
            col("vehicle_id"),
            col("road_id"),
            col("city_zone"),
            col("speed_int").alias("speed_kmh"),
            col("congestion_level"),
            col("congestion_risk_score"),
            col("speed_band"),
            col("peak_flag"),
            col("is_weekend"),
            col("weather"),
            col("vehicle_type"),
            col("sensor_id"),
            col("latitude"),
            col("longitude"),
            col("event_ts"),
            to_date(col("event_ts")).alias("event_date"),
            col("event_hour")
        )
    )

    logger.info(f"Writing fact_traffic stream -> {PATHS.fact_traffic_table}")
    fact_query = (
        fact_stream.writeStream
        .format("delta")
        .outputMode("append")
        .partitionBy("event_date", "event_hour")
        .option("checkpointLocation", PATHS.fact_traffic_checkpoint)
        .option("path", PATHS.fact_traffic_table)
        .queryName("FactTrafficStream")
        .start()
    )

    logger.info("Gold Star Schema streaming pipelines active. Awaiting microbatches...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_gold_pipeline()
