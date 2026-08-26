"""
Delta Lake Maintenance, Compaction & Storage Optimization Suite.
Provides automated OPTIMIZE with Z-ORDER clustering, VACUUM storage governance,
and Time Travel transaction log inspection for enterprise Lakehouse operations.
"""

import sys
import logging
from typing import List, Optional
from pyspark.sql import SparkSession

# Import centralized configuration
try:
    from config import PATHS, SPARK_CONFIG
except ImportError:
    from apps.config import PATHS, SPARK_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LakehouseMaintenance")


def get_spark_session() -> SparkSession:
    """Initializes Spark Session configured for Delta Lake SQL operations."""
    return (
        SparkSession.builder
        .appName(f"{SPARK_CONFIG.app_name_prefix}-MaintenanceJob")
        .master(SPARK_CONFIG.master_url)
        .config("spark.sql.extensions", SPARK_CONFIG.delta_extension)
        .config("spark.sql.catalog.spark_catalog", SPARK_CONFIG.delta_catalog)
        .config("spark.hadoop.hive.metastore.uris", SPARK_CONFIG.hive_metastore_uri)
        .enableHiveSupport()
        .getOrCreate()
    )


def optimize_table(spark: SparkSession, table_path: str, zorder_columns: Optional[List[str]] = None):
    """
    Executes file compaction (Bin-Packing) and Z-Order multi-dimensional clustering
    to dramatically accelerate predicate pushdown and query performance.
    """
    logger.info(f"Optimizing Delta Table at: {table_path}")
    if zorder_columns:
        zorder_clause = ", ".join(zorder_columns)
        sql_stmt = f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_clause})"
    else:
        sql_stmt = f"OPTIMIZE delta.`{table_path}`"

    logger.info(f"Executing SQL: {sql_stmt}")
    res = spark.sql(sql_stmt)
    res.show(truncate=False)


def vacuum_table(spark: SparkSession, table_path: str, retention_hours: int = 168):
    """
    Purges uncommitted and obsolete transaction files older than the retention threshold
    (Default: 168 hours / 7 days) to reclaim storage space while preserving Time-Travel safety.
    """
    logger.info(f"Vacuuming Delta Table at: {table_path} (Retention: {retention_hours} hours)")
    # Retention check bypass only if retention is less than 168h and user explicitly enables it
    if retention_hours < 168:
        spark.conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")

    sql_stmt = f"VACUUM delta.`{table_path}` RETAIN {retention_hours} HOURS"
    logger.info(f"Executing SQL: {sql_stmt}")
    res = spark.sql(sql_stmt)
    res.show(truncate=False)


def inspect_table_history(spark: SparkSession, table_path: str, limit: int = 10):
    """Retrieves the transaction log history for auditing, data lineage, and time-travel inspection."""
    logger.info(f"Fetching commit transaction history for: {table_path}")
    history_df = spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`")
    history_df.select("version", "timestamp", "userId", "userName", "operation", "operationParameters").show(limit, truncate=False)


def run_full_maintenance_cycle():
    """Runs end-to-end compaction and governance maintenance on all Lakehouse tables."""
    spark = get_spark_session()
    logger.info("==========================================================")
    logger.info("Starting Enterprise Lakehouse Maintenance & Compaction Run")
    logger.info("==========================================================")

    # 1. Optimize Fact Table with Z-Ordering on primary filter dimensions
    try:
        optimize_table(
            spark=spark,
            table_path=PATHS.fact_traffic_table,
            zorder_columns=["road_id", "city_zone"]
        )
    except Exception as e:
        logger.warning(f"Skipping fact_traffic optimize (Table may not exist yet): {e}")

    # 2. Optimize Silver Table
    try:
        optimize_table(
            spark=spark,
            table_path=PATHS.silver_table,
            zorder_columns=["vehicle_id", "event_ts"]
        )
    except Exception as e:
        logger.warning(f"Skipping traffic_silver optimize: {e}")

    # 3. Vacuum unneeded snapshots (7 days retention)
    for tbl in [PATHS.fact_traffic_table, PATHS.silver_table, PATHS.quarantine_table]:
        try:
            vacuum_table(spark, tbl, retention_hours=168)
        except Exception as e:
            logger.warning(f"Skipping vacuum for {tbl}: {e}")

    logger.info("==========================================================")
    logger.info("✅ Lakehouse Maintenance Cycle Complete!")
    logger.info("==========================================================")


if __name__ == "__main__":
    run_full_maintenance_cycle()
