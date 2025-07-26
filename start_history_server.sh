#!/bin/bash

# Spark History Server Script
# This script starts the Spark History Server to persist UI after jobs finish

echo "📊 Starting Spark History Server"
echo "==============================="
echo ""

# Create events directory if it doesn't exist
mkdir -p /tmp/spark-events

echo "📁 Events directory: /tmp/spark-events"
echo ""

echo "🚀 Starting Spark History Server..."
echo "   This will allow you to access Spark UI after jobs complete"
echo ""

# Start the history server
$SPARK_HOME/sbin/start-history-server.sh

echo ""
echo "✅ Spark History Server started!"
echo ""
echo "🌐 Access the History Server UI at:"
echo "   🔗 http://localhost:18080"
echo ""
echo "📋 What you can do:"
echo "   • View completed jobs"
echo "   • Analyze performance metrics"
echo "   • Compare different job runs"
echo "   • Access DAG visualizations"
echo ""
echo "💡 Tips:"
echo "   • Keep this terminal open to keep the server running"
echo "   • Run your jobs with event logging enabled (already configured)"
echo "   • Jobs will appear in the UI after they complete"
echo ""
echo "🛑 To stop the server later, run:"
echo "   $SPARK_HOME/sbin/stop-history-server.sh" 