# Scala-Spark Broadcast Join Demo

<p align="center">
  <img alt="Java" src="https://img.shields.io/badge/Java-11-orange?logo=coffeescript" />
  <img alt="Scala" src="https://img.shields.io/badge/Scala-2.12-red?logo=scala" />
  <img alt="Apache Spark" src="https://img.shields.io/badge/Apache%20Spark-3.5-orange?logo=apachespark" />
  <img alt="SBT" src="https://img.shields.io/badge/SBT-1.9+-purple?logo=sbt" />
  <img alt="Google Cloud" src="https://img.shields.io/badge/GCP-Dataproc-blue?logo=googlecloud" />
  <img alt="Docker" src="https://img.shields.io/badge/Docker-Containerized-blue?logo=docker" />
</p>

> **Demo**: Optimize large-scale data joins using **broadcast join** technique with Apache Spark.  
> Compare performance between broadcast join vs regular shuffle join on a 123M+ flights dataset,  
> with real-time monitoring and persistent Spark UI analysis.

This repository demonstrates an **optimized broadcast join** on a large flights
dataset using **Scala 2.12 + Apache Spark 3.5.0**.  
You can run everything in Docker first, then lift the exact same code to
Google Cloud (Dataproc + Composer) later.

> **⚠️ Disclaimer**: This project has been tested on **M-Series MacBook** with ARM architecture.  
> Performance results may vary on different hardware configurations.

---

## 📖 Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Quick Start with Docker](#2-quick-start-with-docker)
3. [Docker Commands Reference](#3-docker-commands-reference)
4. [Performance Comparison](#4-performance-comparison)
5. [Data Quality Testing](#5-data-quality-testing)
6. [Spark UI & Monitoring](#6-spark-ui--monitoring)
7. [Local Installation (Optional)](#7-local-installation-optional)
8. [Performance Insights](#8-performance-insights)
9. [Project Structure](#9-project-structure)
10. [Configuration](#10-configuration)
11. [Next Steps](#11-next-steps)
12. [Learn More](#12-learn-more)

---

## 1. Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| **Docker** | ≥ 20.10 | **Required** - Container runtime |
| **Docker Compose** | ≥ 2.0 | Usually included with Docker |
| **macOS** | M-series | Local execution tested on ARM, 48GB RAM |
| **RAM** | ≥ 8GB | Minimum for Docker container |
| **Data Files** | Required | Must download airline.csv & carriers.csv |

> **📥 Data Download Required**: Before running any Docker commands, you must download the required data files to the `data/` folder. See the [Quick Start](#2-quick-start-with-docker) section for the exact commands.

### Docker Installation

**macOS:**
```bash
# Install Docker Desktop
brew install --cask docker

# Start Docker Desktop
open /Applications/Docker.app
```

**Linux:**
```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Add user to docker group
sudo usermod -aG docker $USER
```

**Windows:**
- Download [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Install and start Docker Desktop

### Verify Docker Installation
```bash
# Check Docker is running
docker --version
docker-compose --version

# Test Docker
docker run hello-world
```

> **⚠️ Important**: Make sure Docker is **running** before starting any tests!

---

## 2. Quick Start with Docker

**🚀 Recommended approach - No local installation required!**

```bash
# 1️⃣ Clone the repository
git clone https://github.com/helioribeiro/Scala-Spark-on-Google-Cloud.git
cd Scala-Spark-on-Google-Cloud

# 2️⃣ 📥 Download required data files (REQUIRED!)
# Before running any Docker commands, you must download the data files:
cd data
curl -O https://storage.googleapis.com/scala_broadcast_join/data/airline.csv
curl -O https://storage.googleapis.com/scala_broadcast_join/data/carriers.csv
cd ..

# 3️⃣ 🔧 Fix permissions
mkdir -p output
sudo chmod -R 755 output

# 4️⃣ Build the Docker image (one-time setup)
./docker-run.sh build

# 5️⃣ 🎯 Start Spark History Server (Recommended!)
# Open a new terminal and run:
./docker-run.sh history
# **Access:** http://localhost:18080

# 6️⃣ Run the complete pipeline
./docker-run.sh all
```

That's it! 🎉 The Docker container will:
- ✅ Convert CSV files to Parquet format
- ✅ Run broadcast join vs regular join comparison
- ✅ Execute data quality tests
- ✅ Display performance results and top airlines

**🎯 Pro Tip:** Start the History Server first to get **real-time job monitoring** and **detailed performance analysis**!

**Expected Results:**
- **Broadcast Join**: ~50 seconds
- **Regular Join**: ~120 seconds  
- **Speedup**: ~2.5x faster with broadcast join
- **Top Airlines**: Delta, Southwest, American, US Airways, United

---

## 3. Docker Commands Reference

### 🏗️ Build Commands

#### `./docker-run.sh build`
**What it does:** Builds the complete Docker image with all dependencies
- Installs Java 11, Scala 2.12, Apache Spark 3.5, SBT 1.11
- Compiles the Scala project and creates assembly JAR
- Sets up the containerized environment
- **Time:** ~5-10 minutes (one-time setup)

```bash
./docker-run.sh build
```

### 🚀 Execution Commands

#### `./docker-run.sh all`
**What it does:** Runs the complete data pipeline end-to-end
- Converts CSV files to Parquet format
- Executes broadcast join vs regular join comparison
- Runs comprehensive data quality tests
- Displays formatted results and performance metrics
- **Time:** ~10-15 minutes

```bash
./docker-run.sh all
```

#### `./docker-run.sh convert`
**What it does:** Converts CSV files to optimized Parquet format
- Processes airline.csv (123M+ rows) and carriers.csv (1.5K rows)
- Creates compressed, columnar Parquet files
- Optimizes for Spark processing
- **Time:** ~5-8 minutes

```bash
./docker-run.sh convert
```

#### `./docker-run.sh compare`
**What it does:** Runs performance comparison between join strategies
- Executes broadcast join (optimized for large + small table)
- Executes regular shuffle join (standard approach)
- Calculates speedup, memory savings, and time differences
- Displays top 5 airlines by flight count
- **Time:** ~3-5 minutes

> **💡 Tip:** Run `./docker-run.sh history` first to monitor jobs in real-time!

```bash
./docker-run.sh compare
```

#### `./docker-run.sh test`
**What it does:** Runs comprehensive data quality validation
- Validates row counts (CSV ↔ Parquet ↔ Join output)
- Checks schema integrity and data types
- Verifies no null values in key columns
- Ensures data consistency across pipeline stages
- **Time:** ~2-3 minutes

```bash
./docker-run.sh test
```

### 🔧 Development Commands

#### `./docker-run.sh shell`
**What it does:** Opens an interactive shell inside the container
- Provides full access to the containerized environment
- Allows manual execution of commands
- Useful for debugging and exploration
- **Usage:** Run commands manually, exit with `exit`

```bash
./docker-run.sh shell
```

#### `./docker-run.sh history`
**What it does:** Starts the Spark History Server for job analysis
- Enables persistent Spark UI access
- Allows analysis of completed jobs
- Provides detailed performance metrics
- **Access:** http://localhost:18080

> **🎯 Highly Recommended!** Start this before running any jobs to get:
> - 📊 **Real-time job monitoring** during execution
> - 🔍 **Detailed DAG visualizations** of your joins
> - 📈 **Performance metrics** and bottlenecks
> - 💾 **Memory usage analysis** and optimization insights

```bash
./docker-run.sh history
```

### 🧹 Maintenance Commands

#### `./docker-run.sh clean`
**What it does:** Stops and removes all containers
- Frees up system resources
- Removes temporary containers
- Cleans up Docker volumes
- **Use when:** Switching between different runs

```bash
./docker-run.sh clean
```

#### `./docker-run.sh logs`
**What it does:** Shows real-time container logs
- Displays Spark application logs
- Shows build and execution progress
- Useful for debugging issues
- **Usage:** Monitor long-running operations

```bash
./docker-run.sh logs
```

#### `./docker-run.sh help`
**What it does:** Shows all available commands
- Displays command descriptions
- Provides usage examples
- Quick reference for all options

```bash
./docker-run.sh help
```

### 🎯 Command Summary

| Command | Purpose | Time | When to Use |
|---------|---------|------|-------------|
| `build` | Setup environment | 5-10 min | First time only |
| `all` | Complete pipeline | 10-15 min | **Recommended** |
| `convert` | Data conversion | 5-8 min | Data preparation |
| `compare` | Performance test | 3-5 min | Join optimization |
| `test` | Quality validation | 2-3 min | Data integrity |
| `shell` | Interactive mode | - | Development |
| `history` | Spark UI | - | Analysis |
| `clean` | Cleanup | - | Maintenance |
| `logs` | Monitor | - | Debugging |
| `help` | Documentation | - | Reference |

---

## 4. Performance Comparison

Run a complete performance comparison between broadcast join and regular shuffle join:

> **🎯 For the best experience, start the History Server first:**
> ```bash
> # Terminal 1: Start History Server
> ./docker-run.sh history
> 
> # Terminal 2: Run comparison (in a new terminal)
> ./docker-run.sh compare
> ```

```bash
# Run the comparison script
./docker-run.sh compare
```

This script will:
- Clean and rebuild the project
- Run both broadcast and regular joins
- Display real-time progress and metrics
- Show a detailed performance comparison
- Calculate speedup and memory savings
- **Query the results**: Display top 5 airlines by flight count in a formatted table

**🌐 Monitor your jobs in real-time at:** http://localhost:18080

---

## 5. Data Quality Testing

Ensure your data pipeline integrity with comprehensive quality tests:

```bash
# Run all data quality tests
./docker-run.sh test
```

### What the Tests Validate:

#### 1. **Row Count Validation**
- ✅ Original CSV datasets have expected row counts
- ✅ Parquet conversion preserves all rows (CSV ↔ Parquet)
- ✅ Broadcast join output matches input row count

#### 2. **Schema Validation**
- ✅ Joined dataset contains required columns (`UniqueCarrier`, `AirlineName`)
- ✅ Carrier code column properly dropped (no duplicates)
- ✅ Data types are correct

#### 3. **Data Integrity Checks**
- ✅ No null values in key columns
- ✅ Sample data contains valid airline names
- ✅ Data size is within reasonable ranges

#### 4. **Performance Validation**
- ✅ Row counts are within expected bounds (1M-200M)
- ✅ No data corruption during processing

### Expected Output

```
🔍 Starting Data Quality Tests
==============================

📋 Checking prerequisites...
✅ All prerequisites found

🔨 Building project...
✅ Build successful

🧪 Running Data Quality Tests...
================================

📊 Original CSV Row Counts:
   Flights: 123534969 rows
   Carriers: 1491 rows

📊 Parquet vs CSV Row Counts:
   Flights CSV: 123534969 → Parquet: 123534969
   Carriers CSV: 1491 → Parquet: 1491

📊 Broadcast Join Output:
   Input Flights: 123534969 rows
   Output Joined: 123534969 rows

📊 Schema Validation:
   Columns: UniqueCarrier, AirlineName, ...

📊 Data Quality Checks:
   Null UniqueCarrier: 0
   Null AirlineName: 0

🎉 All Data Quality Tests Passed!
✅ Your data pipeline is working correctly
```

---

## 6. Spark UI & Monitoring

### Persistent Spark History Server
**🎯 Start this BEFORE running any jobs for the best experience!**

To analyze completed jobs and compare performance, follow these steps:

#### Step 1: Start the History Server
Open a **new terminal window** and run:
```bash
./docker-run.sh history
```

You should see output like:
```
📊 Starting Spark History Server
===============================

📁 Events directory: /tmp/spark-events

🚀 Starting Spark History Server...
   This will allow you to access Spark UI after jobs complete

✅ Spark History Server started!

🌐 Access the History Server UI at:
   🔗 http://localhost:18080
```

> **💡 Why start it first?** You'll get:
> - 📊 **Real-time job monitoring** as they execute
> - 🔍 **Live DAG visualizations** showing your join strategies
> - 📈 **Instant performance metrics** and bottlenecks
> - 💾 **Memory usage tracking** throughout execution

#### Step 2: Run Your Jobs
In your **original terminal**, run the performance comparison:
```bash
./docker-run.sh compare
```

#### Step 3: Access the Persistent UI
Open your web browser and go to:
**http://localhost:18080**

#### Step 4: Analyze Your Results
In the History Server UI, you can:
- **View completed jobs** - See all your broadcast and regular join runs
- **Compare performance** - Click on different jobs to compare execution times
- **Analyze DAGs** - View the execution plans and stage details
- **Check metrics** - See memory usage, shuffle data, and other performance indicators

#### Step 5: Stop the History Server (when done)
In the history server terminal, press `Ctrl+C` or run:
```bash
./docker-run.sh clean
```

**What you can analyze:**
- ✅ **Completed jobs** - View after they finish
- 📈 **Performance metrics** - Execution times, memory usage
- 🔍 **DAG visualizations** - Query execution plans
- 📊 **Stage details** - Individual stage performance
- 💾 **Storage info** - Cache effectiveness
- ⚙️ **Configuration** - All Spark settings

---

## 7. Local Installation (Optional)

> **💡 Note**: This section is optional. Docker is the recommended approach.

If you prefer to run locally without Docker, follow these installation steps:

### Prerequisites
Make sure you have [Homebrew](https://brew.sh/) installed:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### Step 1: Install Java 11
```bash
# Install Java 11
brew install openjdk@11

# Add Java to your PATH (add this to your ~/.zshrc or ~/.bash_profile)
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@11"' >> ~/.zshrc

# Reload your shell configuration
source ~/.zshrc

# Verify installation
java -version
```

### Step 2: Install Scala
```bash
# Install Scala
brew install scala

# Verify installation
scala -version
```

### Step 3: Install Apache Spark
```bash
# Install Apache Spark
brew install apache-spark

# Add Spark to your PATH (add this to your ~/.zshrc or ~/.bash_profile)
echo 'export SPARK_HOME="/opt/homebrew/opt/apache-spark"' >> ~/.zshrc
echo 'export PATH="$SPARK_HOME/bin:$PATH"' >> ~/.zshrc

# Reload your shell configuration
source ~/.zshrc

# Verify installation
spark-shell --version
```

### Step 4: Install SBT
```bash
# Install SBT (Scala Build Tool)
brew install sbt

# Verify installation
sbt --version
```

### Step 5: Verify All Installations
```bash
# Check all versions
echo "Java version:"
java -version

echo "Scala version:"
scala -version

echo "Spark version:"
spark-shell --version

echo "SBT version:"
sbt --version
```

### Local Quick Start

```bash
git clone https://github.com/helioribeiro/Scala-Spark-on-Google-Cloud.git
cd Scala-Spark-on-Google-Cloud

# 1️⃣  build a fat JAR
sbt assembly

# 2️⃣  convert CSV ➜ Parquet  (one-time, about 4–5 min)
spark-submit \
  --class com.helioribeiro.ConvertCsvToParquet \
  --driver-memory 16g \
  target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar \
  data/airline.csv  data/parquet/flights_single  28

spark-submit \
  --class com.helioribeiro.ConvertCsvToParquet \
  --driver-memory 2g \
  target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar \
  data/carriers.csv data/parquet/carriers  1

# 3️⃣  run the broadcast join (2–3 min)
spark-submit \
  --class com.helioribeiro.BroadcastJoin \
  --driver-memory 16g \
  --conf spark.sql.shuffle.partitions=28 \
  target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar \
  data/parquet/flights  data/parquet/carriers  output/join_result

# 4️⃣  run queries on joined dataset
  spark-sql -e "SELECT AirlineName, COUNT(*) AS flights FROM parquet.\`output/join_result\` GROUP BY AirlineName ORDER BY flights DESC LIMIT 5;"
```

---

## 8. Performance Insights

### Expected Results
- **Broadcast Join**: ~40-50 seconds (optimized for large + small table)
- **Regular Join**: ~90-120 seconds (shuffle-based, slower)
- **Speedup**: 2-3x faster with broadcast join
- **Memory**: Broadcast join typically uses less memory

### Key Optimizations
- **Broadcast Join**: Small carriers table broadcasted to all executors
- **Event Logging**: Persistent job history for analysis

---

## 9. Project Structure

```
├── src/main/scala/com/helioribeiro/
│   ├── BroadcastJoin.scala      # Optimized broadcast join
│   ├── RegularJoin.scala        # Standard shuffle join
│   └── ConvertCsvToParquet.scala # Data conversion utility
├── data/
│   ├── airline.csv              # Large flights dataset
│   ├── carriers.csv             # Small lookup table
│   └── parquet/                 # Converted Parquet files
├── output/                      # Job results
├── docker/                      # Docker configuration
│   ├── Dockerfile               # Container definition
│   ├── docker-compose.yml       # Orchestration
│   └── .dockerignore           # Build exclusions
├── scripts/                     # Execution scripts
│   ├── compare_joins.sh         # Performance comparison
│   ├── start_history_server.sh  # Spark history server
│   └── run_data_quality_tests.sh # Data validation
├── docker-run.sh               # Main Docker interface
├── build.sbt                   # SBT build configuration
└── README.md                   # This file
```

---

## 10. Configuration

### Spark Settings
- **Driver Memory**: 16-32GB (adjust based on your system)
- **Shuffle Partitions**: 28-200 (optimized for dataset size)
- **Event Logging**: Enabled for persistent UI
- **Auto Broadcast**: Disabled (manual control)

### Memory Requirements
- **Minimum**: 16GB RAM
- **Recommended**: 32GB+ RAM
- **Dataset Size**: ~123M flights, ~1.5K carriers

### Docker Configuration
Edit `docker-compose.yml` to adjust memory settings:

```yaml
deploy:
  resources:
    limits:
      memory: 16G  # Maximum memory
    reservations:
      memory: 8G   # Minimum memory
```

---

## 11. Next Steps

1. **Analyze Performance**: Use the history server to compare join strategies
2. **Optimize Further**: Adjust partitions, memory, and caching
3. **Scale Up**: Run on Google Cloud Dataproc for larger datasets
4. **Production**: Deploy with proper monitoring and alerting

---

## 12. Learn More

- [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Broadcast Join Optimization](https://spark.apache.org/docs/latest/sql-performance-tuning.html#broadcast-hint-for-sql-queries)
- [Spark UI Guide](https://spark.apache.org/docs/latest/web-ui.html)