"""
Bronze Layer: Real-Time Raw Ingestion Stream.
Ingests raw JSON mobility telemetry from Apache Kafka topic into Delta Lake Bronze Table.
Preserves complete payload fidelity and adds technical ingestion metadata for auditability.
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    struct
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Import centralized configuration
try:
    from config import KAFKA_CONFIG, PATHS, SPARK_CONFIG
except ImportError:
    from apps.config import KAFKA_CONFIG, PATHS, SPARK_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BronzeLayer")


def create_spark_session() -> SparkSession:
    """Initializes Spark Session with Delta Lake and Hive Metastore support."""
    logger.info("Initializing Spark Session for Bronze Ingestion...")
    spark = (
        SparkSession.builder
        .appName(f"{SPARK_CONFIG.app_name_prefix}-BronzeLayer")
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


# Flexible Bronze Schema definition
TRAFFIC_PAYLOAD_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("road_id", StringType(), True),
    StructField("city_zone", StringType(), True),
    StructField("speed", StringType(), True),  # StringType to safely ingest dirty/string values
    StructField("congestion_level", IntegerType(), True),
    StructField("weather", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("event_time", StringType(), True),
    StructField("raw", StringType(), True),     # Captures corrupt payload strings
])


def run_bronze_pipeline():
    """Builds and executes the Bronze Structured Streaming pipeline."""
    spark = create_spark_session()

    logger.info(f"Subscribing to Kafka topic: '{KAFKA_CONFIG.topic}' @ {KAFKA_CONFIG.bootstrap_servers}")

    # 1. Ingest raw stream from Kafka
    raw_kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_CONFIG.bootstrap_servers)
        .option("subscribe", KAFKA_CONFIG.topic)
        .option("startingOffsets", KAFKA_CONFIG.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Extract technical metadata and raw string payload
    payload_df = raw_kafka_stream.select(
        col("value").cast("string").alias("raw_payload"),
        col("timestamp").alias("kafka_timestamp"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        current_timestamp().alias("ingestion_ts")
    )

    # 3. Parse JSON while maintaining raw string for corrupt record recovery
    parsed_stream = (
        payload_df
        .withColumn("parsed_data", from_json(col("raw_payload"), TRAFFIC_PAYLOAD_SCHEMA))
        .select(
            "raw_payload",
            "kafka_timestamp",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "ingestion_ts",
            "parsed_data.*"
        )
    )

    logger.info(f"Writing Bronze stream to Delta table at: {PATHS.bronze_table}")

    # 4. Sink to Bronze Delta Table with Checkpointing
    bronze_query = (
        parsed_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", PATHS.bronze_checkpoint)
        .option("path", PATHS.bronze_table)
        .queryName("BronzeIngestionStream")
        .start()
    )

    logger.info("Bronze Streaming pipeline active. Awaiting new events...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_bronze_pipeline()
