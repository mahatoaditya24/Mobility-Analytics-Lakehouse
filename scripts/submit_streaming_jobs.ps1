# =============================================================================
# Submit Spark Streaming Lakehouse Pipelines (Windows PowerShell)
# =============================================================================
param(
    [string]$Layer = "all"
)

$DeltaPackages = "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "⚡ Submitting Spark Streaming Pipelines (Target: $Layer)" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

function Submit-SparkJob($ScriptName, $JobDesc) {
    Write-Host "🚀 Launching $JobDesc (/opt/spark-apps/$ScriptName)..." -ForegroundColor Yellow
    docker exec -d spark-worker /opt/spark/bin/spark-submit `
        --conf spark.jars.ivy=/tmp/.ivy `
        --packages $DeltaPackages `
        /opt/spark-apps/$ScriptName
}

if ($Layer -eq "bronze" -or $Layer -eq "all") {
    Submit-SparkJob "bronze_layer.py" "Bronze Raw Ingestion Pipeline"
}

if ($Layer -eq "silver" -or $Layer -eq "all") {
    Submit-SparkJob "silver_layer.py" "Silver Clean & DLQ Quarantine Pipeline"
}

if ($Layer -eq "gold" -or $Layer -eq "all") {
    Submit-SparkJob "gold_layer.py" "Gold Star Schema & Dimensional Pipeline"
}

Write-Host "`n✅ Pipelines submitted in background! Monitor jobs at http://localhost:8080" -ForegroundColor Green
