#!/bin/bash

# CSV to Parquet conversion script with progress bars and clean output
# This script converts both airline.csv and carriers.csv to Parquet format

echo -e "\n\n🚀 Starting CSV to Parquet Conversion"
echo "===================================="

# Use pre-built project
echo "📦 Using pre-built project (from Docker image)..."
# Skip SBT build - use pre-built JAR

# Create output directories
mkdir -p data/parquet/flights
mkdir -p data/parquet/carriers

echo ""
echo "🔄 Converting AIRLINE.CSV to Parquet..."
echo "======================================"

# Function to get CPU count cross-platform
get_cpu_count() {
    if command -v nproc >/dev/null 2>&1; then
        # Linux
        nproc --all
    elif command -v sysctl >/dev/null 2>&1; then
        # macOS
        sysctl -n hw.logicalcpu
    else
        # Fallback
        getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4
    fi
}

# Usage
THREADS=$(get_cpu_count)
PARTITIONS=$((THREADS * 2))

# Convert airline.csv (large file)
spark-submit \
  --class com.helioribeiro.ConvertCsvToParquet \
  --driver-memory 16g \
  --conf spark.ui.enabled=false \
  --conf spark.eventLog.enabled=false \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.adaptive.coalescePartitions.enabled=false \
  --conf log4j.rootCategory=ERROR,console \
  --conf log4j.logger.org.apache.spark=ERROR \
  --conf log4j.logger.org.apache.hadoop=ERROR \
  --conf log4j.logger.org.apache.parquet=ERROR \
  --conf log4j.logger.org.apache.spark.sql.execution.datasources=ERROR \
  --conf log4j.logger.org.apache.spark.sql.execution=ERROR \
  --conf log4j.logger.org.apache.spark.storage=ERROR \
  --conf log4j.logger.org.apache.spark.scheduler=ERROR \
  target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar \
  data/airline.csv data/parquet/flights "$PARTITIONS" 2>&1 | \
  grep -E "(ERROR|WARN|Input|Output|Partitions|Initial memory|Repartitioning|Writing|Total rows|Total execution|Final memory|Output written|CSV to Parquet conversion completed|Loading|█|░|🔄|📁|✔|⏱️|💾|📊)" | \
  while IFS= read -r line; do
    echo "$line"
  done

echo ""
echo "🔄 Converting CARRIERS.CSV to Parquet..."
echo "======================================="

# Convert carriers.csv (small file)
spark-submit \
  --class com.helioribeiro.ConvertCsvToParquet \
  --driver-memory 2g \
  --conf spark.ui.enabled=false \
  --conf spark.eventLog.enabled=false \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.adaptive.coalescePartitions.enabled=false \
  --conf log4j.rootCategory=ERROR,console \
  --conf log4j.logger.org.apache.spark=ERROR \
  --conf log4j.logger.org.apache.hadoop=ERROR \
  --conf log4j.logger.org.apache.parquet=ERROR \
  --conf log4j.logger.org.apache.spark.sql.execution.datasources=ERROR \
  --conf log4j.logger.org.apache.spark.sql.execution=ERROR \
  --conf log4j.logger.org.apache.spark.storage=ERROR \
  --conf log4j.logger.org.apache.spark.scheduler=ERROR \
  target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar \
  data/carriers.csv data/parquet/carriers 1 2>&1 | \
  grep -E "(ERROR|WARN|Input|Output|Partitions|Initial memory|Repartitioning|Writing|Total rows|Total execution|Final memory|Output written|CSV to Parquet conversion completed|Loading|█|░|🔄|📁|✔|⏱️|💾|📊)" | \
  while IFS= read -r line; do
    echo "$line"
  done

echo ""
echo "✅ CSV to Parquet conversion completed!"
echo ""
echo "📋 SUMMARY:"
echo "==========="
echo "• Airline data: data/airline.csv → data/parquet/flights/"
echo "• Carriers data: data/carriers.csv → data/parquet/carriers/"
echo "• Both files are now in optimized Parquet format"
echo "• Ready to run join performance comparison!"
echo ""
echo "💡 Next step: Run ./compare_joins.sh to test broadcast vs regular join" 