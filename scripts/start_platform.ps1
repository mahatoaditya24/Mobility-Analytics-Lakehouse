# =============================================================================
# Start Mobility Analytics Docker Platform (Windows PowerShell)
# =============================================================================
$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "🚀 Starting Mobility Analytics Real-Time Lakehouse Services" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Ensure storage directories exist
if (-not (Test-Path "warehouse")) { New-Item -ItemType Directory -Path "warehouse" | Out-Null }
if (-not (Test-Path "spark-ivy")) { New-Item -ItemType Directory -Path "spark-ivy" | Out-Null }

# 2. Spin up Docker containers
docker compose up -d

Write-Host "`n⏳ Waiting for Kafka and Hive services to be ready..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# 3. Create Kafka topic
Write-Host "📦 Ensuring Kafka topic 'traffic-topic' is created..." -ForegroundColor Green
docker exec -i kafka /opt/kafka/bin/kafka-topics.sh `
  --create `
  --if-not-exists `
  --topic traffic-topic `
  --bootstrap-server kafka:9092 `
  --partitions 3 `
  --replication-factor 1

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host "✅ All Infrastructure Services are UP and Running!" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
Write-Host "🌐 Kafka UI:         http://localhost:8090"
Write-Host "⚡ Spark Master UI:  http://localhost:8080"
Write-Host "👷 Spark Worker UI:  http://localhost:8081"
Write-Host "🗄️ Hive Metastore:   thrift://localhost:9083"
Write-Host "🐘 Postgres DB:      localhost:5435 (user: hive / pass: hive)"
Write-Host "=========================================================="
