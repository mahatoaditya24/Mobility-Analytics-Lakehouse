#!/usr/bin/env bash
# =============================================================================
# Start Mobility Analytics Docker Platform
# =============================================================================
set -e

echo "=========================================================="
echo "🚀 Starting Mobility Analytics Real-Time Lakehouse Services"
echo "=========================================================="

# 1. Ensure storage directories exist
mkdir -p warehouse spark-ivy

# 2. Spin up Docker containers in detached mode
docker compose up -d

echo ""
echo "⏳ Waiting for Kafka broker and Hive Metastore to initialize..."
sleep 10

# 3. Create Kafka Topic if not exists
echo "📦 Ensuring Kafka topic 'traffic-topic' is created..."
docker exec -i kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic traffic-topic \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1

echo ""
echo "=========================================================="
echo "✅ All Infrastructure Services are UP and Running!"
echo "=========================================================="
echo "🌐 Kafka UI:         http://localhost:8090"
echo "⚡ Spark Master UI:  http://localhost:8080"
echo "👷 Spark Worker UI:  http://localhost:8081"
echo "🗄️ Hive Metastore:   thrift://localhost:9083"
echo "🐘 Postgres DB:      localhost:5435 (user: hive / pass: hive)"
echo "=========================================================="
