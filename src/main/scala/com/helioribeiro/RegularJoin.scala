package com.helioribeiro

import org.apache.spark.sql.{SparkSession, DataFrame}
import java.lang.management.ManagementFactory
import scala.collection.mutable.ArrayBuffer

/** Spark job that performs a regular shuffle join (no broadcast) for performance comparison. */
object RegularJoin {

  case class Params(
    flightsParquet: String = "",
    carriersParquet: String = "",
    outputDir: String = ""
  )

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

  // Simple CLI parsing (no external lib required)
  private def parseArgs(args: Array[String]): Params = {
    if (args.length != 3) {
      System.err.println(
        "Usage: RegularJoin <flights.parquet> <carriers.parquet> <outputDir>")
      sys.exit(1)
    }
    Params(args(0), args(1), args(2))
  }

  def main(rawArgs: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val memorySamples = new ArrayBuffer[Long]()
    
    // Start memory monitoring thread
    val memoryMonitor = new Thread(() => {
      while (!Thread.currentThread().isInterrupted) {
        memorySamples += getCurrentMemoryUsage()
        Thread.sleep(1000) // Sample every second
      }
    })
    memoryMonitor.setDaemon(true)
    memoryMonitor.start()

    val p = parseArgs(rawArgs)
    val spark = SparkSession.builder()
      .appName("RegularJoin")
      .master("local[*]")         // comment out when you run on Dataproc
      .config("spark.sql.autoBroadcastJoinThreshold", -1) // disable auto broadcast
      .getOrCreate()

    import spark.implicits._

    println(s"🕐 Starting regular shuffle join at ${new java.util.Date()}")
    println(s"📊 Initial memory usage: ${formatBytes(getCurrentMemoryUsage())}")

    val flights = spark.read.parquet(p.flightsParquet)
    val flightsCount = flights.count()
    
    val carriers = spark.read.parquet(p.carriersParquet)
    val carriersCount = carriers.count()

    println(s"📁 Flights dataset: ${flightsCount} rows")
    println(s"📁 Carriers dataset: ${carriersCount} rows")

    // Same join logic as BroadcastJoin but WITHOUT broadcast()
    // This will trigger a shuffle join (much slower for large + small table)
    val joined = flights.join(
      carriers,  // No broadcast() wrapper
      flights("UniqueCarrier") === carriers("Code"),
      "left"
    ).drop(carriers("Code"))
     .withColumnRenamed("Description", "AirlineName")

    joined.write.mode("overwrite").parquet(p.outputDir)
    
    val endTime = System.currentTimeMillis()
    val totalTime = endTime - startTime
    
    // Stop memory monitoring
    memoryMonitor.interrupt()
    memoryMonitor.join()
    
    // Calculate memory statistics
    val avgMemory = if (memorySamples.nonEmpty) memorySamples.sum / memorySamples.length else 0L
    val maxMemory = if (memorySamples.nonEmpty) memorySamples.max else 0L
    
    println(s"✔ Regular shuffle join finished. Output written to ${p.outputDir}")
    println(s"⏱️  Total execution time: ${totalTime / 1000.0} seconds")
    println(s"💾 Average memory usage: ${formatBytes(avgMemory)}")
    println(s"💾 Peak memory usage: ${formatBytes(maxMemory)}")
    println(s"📊 Final result count: ${joined.count()} rows")

    spark.stop()
  }
} 