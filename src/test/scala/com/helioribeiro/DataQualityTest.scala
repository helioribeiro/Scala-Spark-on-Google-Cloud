package com.helioribeiro

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

/** Data Quality Test Suite for Broadcast Join Pipeline */
class DataQualityTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DataQualityTest")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .getOrCreate()
    
    // Set log level to ERROR to reduce verbose output
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  "Original CSV Datasets" should "have expected row counts" in {
    val flightsCsv = spark.read.option("header", "true").csv("data/airline.csv")
    val carriersCsv = spark.read.option("header", "true").csv("data/carriers.csv")
    
    val flightsCount = flightsCsv.count()
    val carriersCount = carriersCsv.count()
    
    println(s"📊 Original CSV Row Counts:")
    println(s"   Flights: ${flightsCount} rows")
    println(s"   Carriers: ${carriersCount} rows")
    
    // Assertions
    flightsCount should be > 0L
    carriersCount should be > 0L
    flightsCount should be > carriersCount // Flights should be much larger
  }

  "Converted Parquet Datasets" should "match original CSV row counts" in {
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
    flightsParquetCount shouldEqual flightsCsvCount
    carriersParquetCount shouldEqual carriersCsvCount
  }

  "Broadcast Join Output" should "have correct row count and data integrity" in {
    val joinedData = spark.read.parquet("output/broadcast_join_result")
    val flightsParquet = spark.read.parquet("data/parquet/flights")
    
    val joinedCount = joinedData.count()
    val flightsCount = flightsParquet.count()
    
    println(s"📊 Broadcast Join Output:")
    println(s"   Input Flights: ${flightsCount} rows")
    println(s"   Output Joined: ${joinedCount} rows")
    
    // Assertions - joined data should have same count as flights (inner join)
    joinedCount shouldEqual flightsCount
  }

  "Joined Dataset Schema" should "contain expected columns" in {
    val joinedData = spark.read.parquet("output/broadcast_join_result")
    val columns = joinedData.columns.toSet
    
    println(s"📊 Schema Validation:")
    println(s"   Columns: ${columns.mkString(", ")}")
    
    // Check for required columns
    columns should contain("UniqueCarrier")
    columns should contain("AirlineName")
    
    // Check that carrier code column was dropped (as per join logic)
    columns should not contain "Code"
  }

  "Data Quality" should "pass integrity checks" in {
    val joinedData = spark.read.parquet("output/broadcast_join_result")
    
    // Check for null values in key columns
    val nullCarriers = joinedData.filter("UniqueCarrier IS NULL").count()
    val nullAirlineNames = joinedData.filter("AirlineName IS NULL").count()
    
    println(s"📊 Data Quality Checks:")
    println(s"   Null UniqueCarrier: ${nullCarriers}")
    println(s"   Null AirlineName: ${nullAirlineNames}")
    
    // Assertions - no nulls in key columns
    nullCarriers shouldEqual 0L
    nullAirlineNames shouldEqual 0L
  }

  "Sample Data" should "contain expected airline names" in {
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
    sampleAirlines.length should be > 0
    sampleAirlines.foreach(airline => airline should not be empty)
  }

  "Performance Metrics" should "be within expected ranges" in {
    val joinedData = spark.read.parquet("output/broadcast_join_result")
    
    // Check data size
    val rowCount = joinedData.count()
    
    println(s"📊 Performance Validation:")
    println(s"   Total Rows: ${rowCount}")
    
    // Assertions - reasonable data size
    rowCount should be > 1000000L // Should have millions of rows
    rowCount should be < 200000000L // Should not be unreasonably large
  }
} 