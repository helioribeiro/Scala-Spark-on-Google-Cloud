package com.helioribeiro

import org.apache.spark.sql.{SparkSession, DataFrame}

/** Data Quality Test Runner - Main class for running data quality tests */
object DataQualityRunner {

  def main(args: Array[String]): Unit = {
    println("🔍 Starting Data Quality Tests")
    println("==============================")
    println("")

    // Initialize Spark
    val spark = SparkSession.builder()
      .appName("DataQualityRunner")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .getOrCreate()
    
    // Set log level to ERROR to reduce verbose output
    spark.sparkContext.setLogLevel("ERROR")

    try {
      // Test 1: Original CSV Datasets
      println("📋 Checking prerequisites...")
      testCsvDatasets(spark)
      println("✅ All prerequisites found")
      println("")

      // Test 2: Parquet Conversion
      println("🔨 Testing Parquet conversion...")
      testParquetConversion(spark)
      println("✅ Parquet conversion verified")
      println("")

      // Test 3: Broadcast Join Output
      println("🧪 Testing Broadcast Join output...")
      testBroadcastJoinOutput(spark)
      println("✅ Broadcast Join output verified")
      println("")

      // Test 4: Schema Validation
      println("📊 Testing Schema validation...")
      testSchemaValidation(spark)
      println("✅ Schema validation passed")
      println("")

      // Test 5: Data Quality
      println("🔍 Testing Data quality...")
      testDataQuality(spark)
      println("✅ Data quality checks passed")
      println("")

      // Test 6: Sample Data
      println("📋 Testing Sample data...")
      testSampleData(spark)
      println("✅ Sample data validated")
      println("")

      // Test 7: Performance Metrics
      println("📈 Testing Performance metrics...")
      testPerformanceMetrics(spark)
      println("✅ Performance metrics verified")
      println("")

      println("🎉 All Data Quality Tests Passed!")
      println("✅ Your data pipeline is working correctly")
      println("")
      println("📊 Summary:")
      println("   • CSV datasets validated")
      println("   • Parquet conversion verified")
      println("   • Broadcast join output checked")
      println("   • Schema validation passed")
      println("   • Data integrity confirmed")
      println("   • Sample data validated")
      println("   • Performance metrics verified")

    } catch {
      case e: Exception =>
        println(s"❌ Data Quality Tests Failed!")
        println(s"   Error: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }

  def testCsvDatasets(spark: SparkSession): Unit = {
    val flightsCsv = spark.read.option("header", "true").csv("data/airline.csv")
    val carriersCsv = spark.read.option("header", "true").csv("data/carriers.csv")
    
    val flightsCount = flightsCsv.count()
    val carriersCount = carriersCsv.count()
    
    println(s"📊 Original CSV Row Counts:")
    println(s"   Flights: ${flightsCount} rows")
    println(s"   Carriers: ${carriersCount} rows")
    
    // Assertions
    if (flightsCount <= 0) throw new RuntimeException("Flights CSV has no rows")
    if (carriersCount <= 0) throw new RuntimeException("Carriers CSV has no rows")
    if (flightsCount <= carriersCount) throw new RuntimeException("Flights should be much larger than carriers")
  }

  def testParquetConversion(spark: SparkSession): Unit = {
    val flightsParquet = spark.read.parquet("data/parquet/flights")
    val carriersParquet = spark.read.parquet("data/parquet/carriers")
    
    val flightsCsv = spark.read.option("header", "true").csv("data/airline.csv")
    val carriersCsv = spark.read.option("header", "true").csv("data/carriers.csv")
    
    val flightsParquetCount = flightsParquet.count()
    val carriersParquetCount = carriersParquet.count()
    val flightsCsvCount = flightsCsv.count()
    val carriersCsvCount = carriersCsv.count()
    
    println(s"📊 Parquet vs CSV Row Counts:")
    println(s"   Flights CSV: ${flightsCsvCount} → Parquet: ${flightsParquetCount}")
    println(s"   Carriers CSV: ${carriersCsvCount} → Parquet: ${carriersParquetCount}")
    
    // Assertions - row counts should match
    if (flightsParquetCount != flightsCsvCount) throw new RuntimeException("Flights Parquet count doesn't match CSV")
    if (carriersParquetCount != carriersCsvCount) throw new RuntimeException("Carriers Parquet count doesn't match CSV")
  }

  def testBroadcastJoinOutput(spark: SparkSession): Unit = {
    val joinedData = spark.read.parquet("output/broadcast_join_result")
    val flightsParquet = spark.read.parquet("data/parquet/flights")
    
    val joinedCount = joinedData.count()
    val flightsCount = flightsParquet.count()
    
    println(s"📊 Broadcast Join Output:")
    println(s"   Input Flights: ${flightsCount} rows")
    println(s"   Output Joined: ${joinedCount} rows")
    
    // Assertions - joined data should have same count as flights (inner join)
    if (joinedCount != flightsCount) throw new RuntimeException("Joined count doesn't match flights count")
  }

  def testSchemaValidation(spark: SparkSession): Unit = {
    val joinedData = spark.read.parquet("output/broadcast_join_result")
    val columns = joinedData.columns.toSet
    
    println(s"📊 Schema Validation:")
    println(s"   Columns: ${columns.mkString(", ")}")
    
    // Check for required columns
    if (!columns.contains("UniqueCarrier")) throw new RuntimeException("Missing UniqueCarrier column")
    if (!columns.contains("AirlineName")) throw new RuntimeException("Missing AirlineName column")
    
    // Check that carrier code column was dropped (as per join logic)
    if (columns.contains("Code")) throw new RuntimeException("Code column should have been dropped")
  }

  def testDataQuality(spark: SparkSession): Unit = {
    val joinedData = spark.read.parquet("output/broadcast_join_result")
    
    // Check for null values in key columns
    val nullCarriers = joinedData.filter("UniqueCarrier IS NULL").count()
    val nullAirlineNames = joinedData.filter("AirlineName IS NULL").count()
    
    println(s"📊 Data Quality Checks:")
    println(s"   Null UniqueCarrier: ${nullCarriers}")
    println(s"   Null AirlineName: ${nullAirlineNames}")
    
    // Assertions - no nulls in key columns
    if (nullCarriers > 0) throw new RuntimeException("Found null values in UniqueCarrier")
    if (nullAirlineNames > 0) throw new RuntimeException("Found null values in AirlineName")
  }

  def testSampleData(spark: SparkSession): Unit = {
    val joinedData = spark.read.parquet("output/broadcast_join_result")
    
    // Get sample of airline names
    val sampleAirlines = joinedData.select("AirlineName")
      .distinct()
      .limit(5)
      .collect()
      .map(_.getString(0))
    
    println(s"📊 Sample Airlines:")
    sampleAirlines.foreach(airline => println(s"   - ${airline}"))
    
    // Assertions - should have some airlines
    if (sampleAirlines.length == 0) throw new RuntimeException("No airlines found in sample")
    sampleAirlines.foreach(airline => 
      if (airline == null || airline.trim.isEmpty) throw new RuntimeException("Found empty airline name")
    )
  }

  def testPerformanceMetrics(spark: SparkSession): Unit = {
    val joinedData = spark.read.parquet("output/broadcast_join_result")
    
    // Check data size
    val rowCount = joinedData.count()
    
    println(s"📊 Performance Validation:")
    println(s"   Total Rows: ${rowCount}")
    
    // Assertions - reasonable data size
    if (rowCount <= 1000000L) throw new RuntimeException("Should have millions of rows")
    if (rowCount >= 200000000L) throw new RuntimeException("Data size seems unreasonably large")
  }
} 