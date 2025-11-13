package com.example.logagg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import org.apache.spark.sql.streaming.Trigger


object FileLogProcessor extends App {

  val spark = SparkSession.builder()
    .appName("FileLogProcessor")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  println(" Starting Spark Structured Streaming from file...")

  //  Read streaming logs from file
  val logDir = "logs/"
  val rawStream = spark.readStream
    .format("text")
    .load(logDir)

  //   Simple log classification
  val logs = rawStream.withColumn("type",
    when(col("value").contains("OS]"), "os")
      .when(col("value").contains("GET"), "nginx")
      .otherwise("app")
  )

  //  Add timestamp and basic parsing
  val logsWithTime = logs.withColumn("timestamp", current_timestamp())

  // Count log types per 10-second window
  val counts = logsWithTime.groupBy(
    window(col("timestamp"), "10 seconds"),
    col("type")
  ).count()

  // Output results to console
  val query = counts.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("2 seconds"))
    .start()

  query.awaitTermination()
}
