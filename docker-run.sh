#!/bin/bash

# Docker Runner Script for Scala Spark Broadcast Join Project
# This script provides easy access to all project functionality via Docker

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}🚀 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Function to check available memory
check_memory() {
    local required_memory=8  # GB
    local available_memory=$(docker system info --format '{{.MemTotal}}' | awk '{print int($1/1024/1024/1024)}')
    
    if [ "$available_memory" -lt "$required_memory" ]; then
        print_warning "Available Docker memory (${available_memory}GB) is less than recommended (${required_memory}GB)"
        print_warning "Consider increasing Docker memory limit in Docker Desktop settings"
    fi
}

# Function to show usage
show_usage() {
    echo "Docker Runner for Scala Spark Broadcast Join Project"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  build           Build the Docker image"
    echo "  shell           Start an interactive shell in the container"
    echo "  convert         Convert CSV to Parquet (interactive)"
    echo "  compare         Run performance comparison (interactive)"
    echo "  history         Start Spark History Server"
    echo "  test            Run data quality tests"
    echo "  all             Run complete pipeline (convert + compare + test)"
    echo "  clean           Stop and remove containers"
    echo "  logs            Show container logs"
    echo "  help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 build        # Build the Docker image"
    echo "  $0 shell        # Interactive shell"
    echo "  $0 all          # Run complete pipeline"
    echo ""
    echo "Prerequisites:"
    echo "  - Docker installed and running"
    echo "  - At least 8GB RAM available for Docker"
    echo "  - CSV files in ./data/ directory"
}

# Function to build the image
build_image() {
    print_status "\nBuilding Docker image..."
    docker-compose build
    print_success "Docker image built successfully!"
}

# Function to start interactive shell
start_shell() {
    print_status "Starting interactive shell..."
    docker-compose up -d
    docker-compose exec scala-spark bash
}

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

# Function to run CSV to Parquet conversion
run_convert() {
    print_status "Running CSV to Parquet conversion..."
    echo ""
    echo "🧠 Detected $THREADS logical threads"
    echo "📊 Using $PARTITIONS partitions for Spark"
    docker-compose up -d
    docker-compose exec scala-spark bash -c "
        echo 'Converting CSV to Parquet...'
        spark-submit \\
          --class com.helioribeiro.ConvertCsvToParquet \\
          --driver-memory 8g \\
          target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar \\
          data/airline.csv data/parquet/flights \"$PARTITIONS\"
        
        spark-submit \\
          --class com.helioribeiro.ConvertCsvToParquet \\
          --driver-memory 2g \\
          target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar \\
          data/carriers.csv data/parquet/carriers 1
        
        echo 'Conversion completed!'
    "
}

# Function to run performance comparison
run_compare() {
    print_status "Running performance comparison..."
    docker-compose up -d
    docker-compose exec scala-spark bash -c "
        echo 'Running performance comparison...'
        ./compare_joins.sh
    "
}

# Function to start history server
start_history() {
    print_status "Starting Spark History Server..."
    docker-compose up -d
    docker-compose exec scala-spark bash -c "
        echo 'Starting Spark History Server...'
        ./start_history_server.sh
    "
}

# Function to run tests
run_tests() {
    print_status "Running data quality tests..."
    docker-compose up -d
    docker-compose exec scala-spark bash -c "
        echo 'Running data quality tests...'
        ./run_data_quality_tests.sh
    "
}

# Function to run complete pipeline
run_all() {
    print_status "Running complete pipeline..."
    docker-compose up -d
    
    # Convert CSV to Parquet
    print_status "Step 1: Converting CSV to Parquet..."
    docker-compose exec scala-spark bash -c "
        spark-submit \\
          --class com.helioribeiro.ConvertCsvToParquet \\
          --driver-memory 8g \\
          target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar \\
          data/airline.csv data/parquet/flights \"$PARTITIONS\"
        
        spark-submit \\
          --class com.helioribeiro.ConvertCsvToParquet \\
          --driver-memory 2g \\
          target/scala-2.12/scala-spark-broadcast-join-assembly-0.1.0.jar \\
          data/carriers.csv data/parquet/carriers 1
    "
    
    # Run performance comparison
    print_status "Step 2: Running performance comparison..."
    docker-compose exec scala-spark bash -c "./compare_joins.sh"
    
    # Run tests
    print_status "Step 3: Running data quality tests..."
    docker-compose exec scala-spark bash -c "./run_data_quality_tests.sh"
    
    print_success "Complete pipeline finished!"
    print_status "Access Spark UI at: http://localhost:4040"
    print_status "Access History Server at: http://localhost:18080"
}

# Function to clean up
clean_up() {
    print_status "Cleaning up containers..."
    docker-compose down
    print_success "Containers stopped and removed!"
}

# Function to show logs
show_logs() {
    print_status "Showing container logs..."
    docker-compose logs -f scala-spark
}

# Main script logic
main() {
    check_docker
    check_memory
    
    case "${1:-help}" in
        build)
            build_image
            ;;
        shell)
            start_shell
            ;;
        convert)
            run_convert
            ;;
        compare)
            run_compare
            ;;
        history)
            start_history
            ;;
        test)
            run_tests
            ;;
        all)
            run_all
            ;;
        clean)
            clean_up
            ;;
        logs)
            show_logs
            ;;
        help|--help|-h)
            show_usage
            ;;
        *)
            print_error "Unknown command: $1"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@" 