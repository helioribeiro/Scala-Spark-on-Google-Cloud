package com.helioribeiro

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

/** 1-off utility: convert large CSVs (with header) to snappy-compressed Parquet. */
object ConvertCsvToParquet {
  
  // Memory monitoring utilities
  private def getCurrentMemoryUsage(): Long = {
    val runtime = Runtime.getRuntime
    runtime.totalMemory() - runtime.freeMemory()
  }

  private def formatBytes(bytes: Long): String = {
    val units = Array("B", "KB", "MB", "GB")
    var size = bytes.toDouble
    var unitIndex = 0
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024
      unitIndex += 1
    }
    f"${size}%.2f ${units(unitIndex)}"
  }

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    
    if (args.length != 3) {
      System.err.println(
        "Usage: ConvertCsvToParquet <inputCsv> <outputParquetDir> <partitions>\n" +
        "Example: ConvertCsvToParquet data/airline.csv data/parquet/flights 200")
      sys.exit(1)
    }
    
    val Array(inCsv, outParquet, partitions) = args
    
    println(s"🕐 Starting CSV to Parquet conversion at ${new java.util.Date()}")
    println(s"📁 Input: $inCsv")
    println(s"📁 Output: $outParquet")
    println(s"📊 Partitions: $partitions")
    println(s"📊 Initial memory usage: ${formatBytes(getCurrentMemoryUsage())}")

    val spark = SparkSession.builder()
      .appName("CSV-to-Parquet")
      .master("local[*]")              // remove if submitting to a cluster
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")   // large files; schema inference is OK once
      .csv(inCsv)
    
    val rowCount = df.count()
    
    val repartitioned = df.repartition(partitions.toInt)   // avoid huge single output file
    
    repartitioned.write.mode("overwrite").parquet(outParquet)
    
    val endTime = System.currentTimeMillis()
    val totalTime = endTime - startTime
    
    println(s"✔ CSV to Parquet conversion completed!")
    println(s"📊 Total rows processed: $rowCount")
    println(s"⏱️  Total execution time: ${totalTime / 1000.0} seconds")
    println(s"💾 Final memory usage: ${formatBytes(getCurrentMemoryUsage())}")
    println(s"📁 Output written to: $outParquet")
    
    spark.stop()
  }
}