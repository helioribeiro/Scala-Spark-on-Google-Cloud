#!/bin/bash

# Performance comparison script for Broadcast vs Regular Join
# Make sure you've already converted CSV to Parquet first!

echo -e "\n\n🚀 Starting performance comparison between Broadcast and Regular Join"
echo "=================================================================="

# Clean memory and build the project fresh
echo -e "\n🧹 Cleaning memory and building project..."
echo "📦 Using pre-built JAR (from Docker image)..."
# Skip SBT build - use pre-built JAR

# Force garbage collection to free memory
echo "🗑️  Running garbage collection..."
java -cp target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar com.helioribeiro.BroadcastJoin --help > /dev/null 2>&1 || true

# Create output directories with proper permissions
echo "📁 Creating output directories..."
mkdir -p output/broadcast_join_result 2>/dev/null || {
    echo "⚠️  Permission denied creating output directories. Trying with sudo..."
    sudo mkdir -p output/broadcast_join_result
    sudo chown -R $USER:$USER output/
    sudo chmod -R 755 output/
}
mkdir -p output/regular_join_result 2>/dev/null || {
    echo "⚠️  Permission denied creating output directories. Trying with sudo..."
    sudo mkdir -p output/regular_join_result
    sudo chown -R $USER:$USER output/
    sudo chmod -R 755 output/
}

# Function to extract metrics from output
extract_metrics() {
    local output_file=$1
    local join_type=$2
    
    echo ""
    echo "📊 $join_type METRICS:"
    echo "========================"
    
    # Extract timing - look for the actual value
    local exec_time=$(grep "Total execution time:" "$output_file" | tail -1 | sed 's/.*Total execution time: \([0-9.]*\) seconds.*/\1/')
    local avg_memory=$(grep "Average memory usage:" "$output_file" | tail -1 | sed 's/.*Average memory usage: \([0-9.]* [A-Z]*\).*/\1/')
    local peak_memory=$(grep "Peak memory usage:" "$output_file" | tail -1 | sed 's/.*Peak memory usage: \([0-9.]* [A-Z]*\).*/\1/')
    local result_count=$(grep "Final result count:" "$output_file" | tail -1 | sed 's/.*Final result count: \([0-9]*\) rows.*/\1/')
    
    echo "⏱️  Execution Time: ${exec_time:-"N/A"} seconds"
    echo "💾 Average Memory: ${avg_memory:-"N/A"}"
    echo "💾 Peak Memory: ${peak_memory:-"N/A"}"
    echo "📊 Result Rows: ${result_count:-"N/A"}"
}

echo ""
echo "🔄 Running BROADCAST JOIN..."
echo "=========================="

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

echo ""
echo "🧠 Detected $THREADS logical threads"
echo "📊 Using $PARTITIONS partitions for Spark"

# Run broadcast join with real-time output and capture for metrics
broadcast_output=$(mktemp)
spark-submit \
  --class com.helioribeiro.BroadcastJoin \
  --driver-memory 16g \
  --conf spark.sql.shuffle.partitions="$PARTITIONS" \
  --conf spark.ui.enabled=true \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
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
  data/parquet/flights  data/parquet/carriers  output/broadcast_join_result 2>&1 | \
  tee "$broadcast_output" | \
  grep -E "(ERROR|WARN|Initial memory|Flights dataset|Carriers dataset|Performing|Writing|Total execution|Average memory|Peak memory|Final result|Broadcast join finished|Loading|█|░|🔄|📁|✔|⏱️|💾|📊)" | \
  while IFS= read -r line; do
    echo "$line"
  done

echo ""
echo "🔄 Running REGULAR SHUFFLE JOIN..."
echo "================================="

# Run regular join with real-time output and capture for metrics
regular_output=$(mktemp)
spark-submit \
  --class com.helioribeiro.RegularJoin \
  --driver-memory 16g \
  --conf spark.sql.shuffle.partitions="$PARTITIONS" \
  --conf spark.ui.enabled=true \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.adaptive.coalescePartitions.enabled=false \
  --conf log4j.rootCategory=ERROR,console \
  --conf log4j.logger.org.apache.spark=ERROR \
  --conf log4j.logger.org.apache.hadoop=ERROR \
  --conf log4j.logger.org.apache.spark.sql.execution.datasources=ERROR \
  --conf log4j.logger.org.apache.spark.sql.execution=ERROR \
  --conf log4j.logger.org.apache.spark.storage=ERROR \
  --conf log4j.logger.org.apache.spark.scheduler=ERROR \
  target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar \
  data/parquet/flights  data/parquet/carriers  output/regular_join_result 2>&1 | \
  tee "$regular_output" | \
  grep -E "(ERROR|WARN|Initial memory|Flights dataset|Carriers dataset|Performing|Writing|Total execution|Average memory|Peak memory|Final result|Regular shuffle join finished|Loading|█|░|🔄|📁|✔|⏱️|💾|📊)" | \
  while IFS= read -r line; do
    echo "$line"
  done

echo ""
echo "📈 PERFORMANCE COMPARISON SUMMARY"
echo "================================="

# Extract and display metrics
extract_metrics "$broadcast_output" "BROADCAST JOIN"
extract_metrics "$regular_output" "REGULAR SHUFFLE JOIN"

echo ""
echo "🏆 COMPARISON RESULTS:"
echo "====================="

# Get execution times from the captured output
broadcast_time=$(grep "Total execution time:" "$broadcast_output" | tail -1 | sed 's/.*Total execution time: \([0-9.]*\) seconds.*/\1/')
regular_time=$(grep "Total execution time:" "$regular_output" | tail -1 | sed 's/.*Total execution time: \([0-9.]*\) seconds.*/\1/')

echo "⏱️  Broadcast Join Time: ${broadcast_time:-"N/A"} seconds"
echo "⏱️  Regular Join Time: ${regular_time:-"N/A"} seconds"

# Calculate speedup
if [[ -n "$broadcast_time" && -n "$regular_time" && "$broadcast_time" != "N/A" && "$regular_time" != "N/A" ]]; then
    speedup=$(echo "scale=2; $regular_time / $broadcast_time" | bc -l)
    echo "🚀 Speedup: ${speedup}x faster with broadcast join"
    
    # Calculate time savings
    time_saved=$(echo "scale=2; $regular_time - $broadcast_time" | bc -l)
    echo "⏰ Time saved: ${time_saved} seconds"
fi

# Calculate memory savings
broadcast_peak=$(grep "Peak memory usage:" "$broadcast_output" | tail -1 | sed 's/.*Peak memory usage: \([0-9.]*\) \([A-Z]*\).*/\1 \2/')
regular_peak=$(grep "Peak memory usage:" "$regular_output" | tail -1 | sed 's/.*Peak memory usage: \([0-9.]*\) \([A-Z]*\).*/\1 \2/')

if [[ -n "$broadcast_peak" && -n "$regular_peak" && "$broadcast_peak" != "N/A" && "$regular_peak" != "N/A" ]]; then
    # Extract values and units
    broadcast_val=$(echo "$broadcast_peak" | awk '{print $1}')
    broadcast_unit=$(echo "$broadcast_peak" | awk '{print $2}')
    regular_val=$(echo "$regular_peak" | awk '{print $1}')
    regular_unit=$(echo "$regular_peak" | awk '{print $2}')
    
    # Convert to GB for comparison
    if [[ "$broadcast_unit" == "GB" ]]; then
        broadcast_gb=$broadcast_val
    elif [[ "$broadcast_unit" == "MB" ]]; then
        broadcast_gb=$(echo "scale=3; $broadcast_val / 1024" | bc -l)
    elif [[ "$broadcast_unit" == "KB" ]]; then
        broadcast_gb=$(echo "scale=6; $broadcast_val / (1024 * 1024)" | bc -l)
    else
        broadcast_gb=$(echo "scale=9; $broadcast_val / (1024 * 1024 * 1024)" | bc -l)
    fi
    
    if [[ "$regular_unit" == "GB" ]]; then
        regular_gb=$regular_val
    elif [[ "$regular_unit" == "MB" ]]; then
        regular_gb=$(echo "scale=3; $regular_val / 1024" | bc -l)
    elif [[ "$regular_unit" == "KB" ]]; then
        regular_gb=$(echo "scale=6; $regular_val / (1024 * 1024)" | bc -l)
    else
        regular_gb=$(echo "scale=9; $regular_val / (1024 * 1024 * 1024)" | bc -l)
    fi
    
    if (( $(echo "$regular_gb > $broadcast_gb" | bc -l) )); then
        memory_saved_gb=$(echo "scale=2; $regular_gb - $broadcast_gb" | bc -l)
        echo "💾 Memory saved: ${memory_saved_gb} GB less memory with broadcast join"
    else
        echo "💾 Memory: Broadcast join used more memory (unusual case)"
    fi
fi

echo ""
echo "🔍 DATA VALIDATION QUERY"
echo "========================"
echo "Querying top 5 airlines by flight count from broadcast join result..."

# Run SQL query to get top 5 airlines by flight count with nice formatting
# Use heredoc approach that works reliably
top_airlines_result=$(spark-sql --master local[*] \
  --conf spark.sql.warehouse.dir=/tmp/spark-warehouse \
  --conf log4j.rootCategory=ERROR,console \
  <<'SQL'
SELECT AirlineName, COUNT(*) AS flights
FROM parquet.`output/broadcast_join_result`
GROUP BY AirlineName
ORDER BY flights DESC
LIMIT 5;
SQL
)

# Display the result with nice formatting
echo ""
echo "┌─────────────────────────────────────────────────────────────┐"
echo "│                TOP 5 AIRLINES BY FLIGHT COUNT               │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│  Rank │  Airline Name                    │  Flight Count    │"
echo "├─────────────────────────────────────────────────────────────┤"

# Process each line and format it - simplified approach
rank=1
echo "$top_airlines_result" | grep -v "^$" | grep -v "^Time taken" | grep -v "^spark-sql" | tail -5 | while IFS= read -r line; do
  if [[ -n "$line" && "$line" != *"Time taken"* && "$line" != *"spark-sql"* ]]; then
    # Simple parsing - split by spaces and take first part as name, last as count
    airline_name=$(echo "$line" | awk '{for(i=1;i<NF;i++) printf "%s ", $i; print ""}' | sed 's/[[:space:]]*$//')
    flight_count=$(echo "$line" | awk '{print $NF}')
    
    # Truncate airline name if too long
    if [[ ${#airline_name} -gt 28 ]]; then
      airline_name="${airline_name:0:25}..."
    fi
    
    printf "│  %-4s │  %-28s │  %-13s │\n" "$rank" "$airline_name" "$flight_count"
    rank=$((rank + 1))
  fi
done

echo "└─────────────────────────────────────────────────────────────┘"

echo ""
echo "✅ Performance comparison completed!"
echo "💡 Both methods should produce identical results"
echo ""
echo "📋 SUMMARY:"
echo "==========="
echo "• Broadcast join: Optimized for large + small table joins"
echo "• Regular join: Standard shuffle join (slower for this use case)"
echo "• Expected: Broadcast join should be significantly faster"
echo "• Memory: Regular join typically uses more memory due to shuffling"

# Clean up temp files
rm -f "$broadcast_output" "$regular_output" 