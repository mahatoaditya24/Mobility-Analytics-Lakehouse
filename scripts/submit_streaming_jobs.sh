#!/usr/bin/env bash
# =============================================================================
# Submit Spark Streaming Lakehouse Pipelines to Spark Worker
# =============================================================================
set -e

LAYER=${1:-"all"}
DELTA_PACKAGES="io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

echo "=========================================================="
echo "⚡ Submitting Spark Streaming Pipelines (Target: $LAYER)"
echo "=========================================================="

submit_job() {
  local script_name=$1
  local job_desc=$2
  echo "🚀 Launching $job_desc (/opt/spark-apps/$script_name)..."
  docker exec -d spark-worker /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy=/tmp/.ivy \
    --packages "$DELTA_PACKAGES" \
    /opt/spark-apps/"$script_name"
}

if [[ "$LAYER" == "bronze" || "$LAYER" == "all" ]]; then
  submit_job "bronze_layer.py" "Bronze Raw Ingestion Pipeline"
fi

if [[ "$LAYER" == "silver" || "$LAYER" == "all" ]]; then
  submit_job "silver_layer.py" "Silver Clean & DLQ Quarantine Pipeline"
fi

if [[ "$LAYER" == "gold" || "$LAYER" == "all" ]]; then
  submit_job "gold_layer.py" "Gold Star Schema & Dimensional Pipeline"
fi

echo ""
echo "✅ Pipelines submitted in background! Monitor jobs at http://localhost:8080 or http://localhost:4040"
