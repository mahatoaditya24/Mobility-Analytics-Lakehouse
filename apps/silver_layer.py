"""
Silver Layer: Data Cleaning, Quality Validation, Feature Engineering & Quarantine DLQ.
Reads raw Bronze Delta stream, enforces strict enterprise data quality rules,
routes clean validated events to the Silver Delta Lakehouse, and concurrently routes
quarantined anomalies/corrupt events to the Dead Letter Queue (DLQ) Delta table for observability.
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    dayofweek,
    expr,
    hour,
    to_timestamp,
    when
)

# Import centralized configuration
try:
    from config import PATHS, SPARK_CONFIG
except ImportError:
    from apps.config import PATHS, SPARK_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SilverLayer")


def create_spark_session() -> SparkSession:
    """Initializes Spark Session with Delta Lake and Hive Metastore support."""
    logger.info("Initializing Spark Session for Silver Cleaning & DLQ Routing...")
    spark = (
        SparkSession.builder
        .appName(f"{SPARK_CONFIG.app_name_prefix}-SilverLayer")
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


def run_silver_pipeline():
    """Builds and runs the Silver cleaning & DLQ routing streaming pipeline."""
    spark = create_spark_session()

    logger.info(f"Reading stream from Bronze Delta table: {PATHS.bronze_table}")

    # 1. Read Bronze Delta Table as a Stream
    bronze_stream = (
        spark.readStream
        .format("delta")
        .load(PATHS.bronze_table)
    )

    # 2. Type casting and timestamp standardization
    typed_df = (
        bronze_stream
        .withColumn("speed_int", col("speed").cast("int"))
        .withColumn("event_ts", to_timestamp(col("event_time")))
        .withColumn("processed_at", current_timestamp())
    )

    # 3. Enterprise Data Quality Rules Engine
    dq_evaluated_df = typed_df.withColumn(
        "dq_status",
        when(col("raw_payload").contains("CORRUPT") | col("raw").isNotNull(), "CORRUPT_PAYLOAD")
        .when(col("vehicle_id").isNull(), "MISSING_VEHICLE_ID")
        .when(col("event_ts").isNull(), "MISSING_OR_INVALID_TIMESTAMP")
        .when(col("speed_int").isNull() | (col("speed_int") < 0) | (col("speed_int") > 180), "SPEED_OUT_OF_BOUNDS")
        .when(col("event_ts") > current_timestamp() + expr("INTERVAL 10 MINUTES"), "FUTURE_TIMESTAMP_ANOMALY")
        .when(col("event_ts") < current_timestamp() - expr("INTERVAL 3 HOURS"), "EXCESSIVE_LATENCY")
        .otherwise("VALID")
    )

    # =========================================================================
    # A. DEAD LETTER QUEUE (DLQ) / QUARANTINE STREAM
    # =========================================================================
    quarantine_stream = (
        dq_evaluated_df
        .filter(col("dq_status") != "VALID")
        .select(
            col("raw_payload"),
            col("vehicle_id"),
            col("road_id"),
            col("city_zone"),
            col("speed").alias("raw_speed"),
            col("event_time").alias("raw_event_time"),
            col("dq_status").alias("quarantine_reason"),
            col("kafka_timestamp"),
            col("processed_at").alias("quarantine_ts")
        )
    )

    logger.info(f"Starting Quarantine (DLQ) sink -> {PATHS.quarantine_table}")
    quarantine_query = (
        quarantine_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", PATHS.quarantine_checkpoint)
        .option("path", PATHS.quarantine_table)
        .queryName("QuarantineDLQStream")
        .start()
    )

    # =========================================================================
    # B. CLEAN SILVER STREAM & ENRICHMENT
    # =========================================================================
    clean_stream = dq_evaluated_df.filter(col("dq_status") == "VALID")

    # Watermarking for out-of-order data & Deduplication
    watermarked_stream = clean_stream.withWatermark("event_ts", "15 minutes")
    deduped_stream = watermarked_stream.dropDuplicates(["vehicle_id", "event_ts"])

    # Feature Engineering & Business Metrics
    silver_enriched_stream = (
        deduped_stream
        .withColumn("event_hour", hour("event_ts"))
        .withColumn(
            "is_weekend",
            when(dayofweek("event_ts").isin(1, 7), 1).otherwise(0)
        )
        .withColumn(
            "peak_flag",
            when(
                (col("event_hour").between(8, 11)) | (col("event_hour").between(17, 20)),
                1
            ).otherwise(0)
        )
        .withColumn(
            "speed_band",
            when(col("speed_int") < 30, "LOW_SPEED")
            .when(col("speed_int") < 70, "MEDIUM_SPEED")
            .otherwise("HIGH_SPEED")
        )
        .withColumn(
            "congestion_risk_score",
            when(col("congestion_level") >= 4, 3)
            .when(col("congestion_level") == 3, 2)
            .otherwise(1)
        )
    )

    logger.info(f"Starting Silver sink -> {PATHS.silver_table}")
    silver_query = (
        silver_enriched_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", PATHS.silver_checkpoint)
        .option("path", PATHS.silver_table)
        .queryName("SilverCleanStream")
        .start()
    )

    logger.info("Silver Clean & DLQ Quarantine pipelines active. Processing microbatches...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_silver_pipeline()
