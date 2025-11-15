package com.example.logagg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object SparkKafkaConsumer extends App {

  val spark = SparkSession.builder()
    .appName("KafkaToCassandra")
    .master("local[*]")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
//    .config("spark.sql.streaming.checkpointLocation", "file:///tmp/spark-checkpoint")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.port", "4050")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  println(" Started Streaming from kafka to console and cassandra")

  val rawData = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "logs_topic")
    .option("startingOffsets", "latest")
    .load()

  val logs = rawData
    .selectExpr("CAST(value AS STRING) AS log", "timestamp AS event_ts")

  val typeAdded = logs.withColumn(
    "type",
    when(col("log").contains("OS]"), "os")
      .when(col("log").contains("GET"), "nginx")
      .otherwise("app")
  )

  val logCounts = typeAdded
    .groupBy(
      window(col("event_ts"), "10 seconds"),
      col("type")
    )
    .count()
    .select(
      col("window.start").cast("timestamp").as("window_start"),
      col("window.end").   cast("timestamp").as("window_end"),
      col("type"),
      col("count").as("cnt")
    )


  val nginxLogs = typeAdded
    .filter(col("type") === "nginx")
    .withColumn("status", regexp_extract(col("log"), """HTTP/1\.1" (\d{3})""", 1).cast("int"))
    .withColumn("is_error", when(col("status") >= 400, 1).otherwise(0))

  val errorRate30s = nginxLogs
    .groupBy(window(col("event_ts"), "30 seconds"))
    .agg(
      sum("is_error").as("error_logs"),
      count("*").as("total_logs")
    )
    .withColumn("error_rate", col("error_logs").cast("double") / col("total_logs"))
    .select(
      col("window.start").cast("timestamp").as("window_start"),
      col("error_rate"),
      col("total_logs"),
      col("error_logs")
    )


  def writeLogCounts(batchDF: DataFrame, batchId: Long): Unit = {
    if (!batchDF.isEmpty) {
      println(s"\n[info] Batch $batchId → log_counts")
      batchDF.show(truncate = false)
      batchDF.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map(
          "keyspace" -> "log_ks",
          "table"    -> "log_counts"
        ))
        .mode("append")
        .save()
    }
  }

  def writeErrorRate(batchDF: DataFrame, batchId: Long): Unit = {
    if (!batchDF.isEmpty) {
      println(s"\n[info] Batch $batchId → error_rate_30s")
      batchDF.show(truncate = false)
      batchDF.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map(
          "keyspace" -> "log_ks",
          "table"    -> "error_rate_30s"
        ))
        .mode("append")
        .save()
    }
  }

  val q1 = logCounts.writeStream
    .outputMode("update")
    .foreachBatch(writeLogCounts _)
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .option("checkpointLocation", "file:///tmp/spark-checkpoint/log_counts")
    .start()

  val q2 = errorRate30s.writeStream
    .outputMode("complete")
    .foreachBatch(writeErrorRate _)
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .option("checkpointLocation", "file:///tmp/spark-checkpoint/error_rate_30s")
    .start()

  q1.awaitTermination()
  q2.awaitTermination()
}