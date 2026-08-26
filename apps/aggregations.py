"""
Continuous Rollup & Analytics Aggregations.
Computes pre-aggregated hourly traffic KPIs by zone and road type from Delta Fact table.
Powers low-latency analytical queries and executive dashboard charts.
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
    when
)

# Import centralized configuration
try:
    from config import PATHS, SPARK_CONFIG
except ImportError:
    from apps.config import PATHS, SPARK_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Aggregations")


def create_spark_session() -> SparkSession:
    """Initializes Spark Session with Delta Lake and Hive Metastore support."""
    logger.info("Initializing Spark Session for Hourly Rollup Aggregations...")
    spark = (
        SparkSession.builder
        .appName(f"{SPARK_CONFIG.app_name_prefix}-HourlyAggregations")
        .master(SPARK_CONFIG.master_url)
        .config("spark.sql.extensions", SPARK_CONFIG.delta_extension)
        .config("spark.sql.catalog.spark_catalog", SPARK_CONFIG.delta_catalog)
        .config("spark.hadoop.hive.metastore.uris", SPARK_CONFIG.hive_metastore_uri)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(SPARK_CONFIG.log_level)
    return spark


def compute_hourly_rollups(batch_mode: bool = True):
    """Computes hourly zone and road traffic rollup KPIs from fact_traffic."""
    spark = create_spark_session()

    logger.info(f"Loading Fact table from: {PATHS.fact_traffic_table}")
    fact_df = spark.read.format("delta").load(PATHS.fact_traffic_table)
    dim_road_df = spark.read.format("delta").load(PATHS.dim_road_table)

    # Join fact with road dimension to identify speed violations
    joined_df = fact_df.join(
        dim_road_df.select("road_id", "speed_limit_kmh"),
        on="road_id",
        how="left"
    )

    # Compute aggregated KPIs per zone, road, date, and hour
    agg_df = (
        joined_df
        .groupBy("city_zone", "road_id", "event_date", "event_hour")
        .agg(
            count("vehicle_id").alias("total_vehicle_flow"),
            spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
            spark_min("speed_kmh").alias("min_speed_kmh"),
            spark_max("speed_kmh").alias("max_speed_kmh"),
            spark_round(avg("congestion_level"), 2).alias("avg_congestion_level"),
            spark_sum(when(col("congestion_level") >= 4, 1).otherwise(0)).alias("high_congestion_event_count"),
            spark_sum(when(col("speed_kmh") > col("speed_limit_kmh"), 1).otherwise(0)).alias("speed_violation_count"),
        )
        .withColumn(
            "high_congestion_pct",
            spark_round((col("high_congestion_event_count") / col("total_vehicle_flow")) * 100, 1)
        )
        .withColumn("computed_at", current_timestamp())
    )

    logger.info(f"Writing Aggregations to Delta table: {PATHS.agg_hourly_table}")

    # Overwrite / Upsert into aggregate Delta table
    (
        agg_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(PATHS.agg_hourly_table)
    )

    logger.info("Hourly rollups successfully computed and persisted to Delta Lake.")


if __name__ == "__main__":
    compute_hourly_rollups()
