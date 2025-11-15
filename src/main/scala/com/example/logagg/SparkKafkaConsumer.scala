package com.example.logagg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.expressions.Window

object SparkKafkaConsumer extends App {

  val spark = SparkSession.builder()
    .appName("KafkaToCassandra")
    .master("local[*]")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
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
    .groupBy(window(col("event_ts"), "10 seconds"), col("type"))
    .count()
    .select(
      col("window.start").cast("timestamp").as("window_start"),
      col("window.end").cast("timestamp").as("window_end"),
      col("type"),
      col("count").as("cnt")
    )

  val typeCounts10s = logCounts
    .select(
      col("window_start"),
      col("type"),
      col("cnt")
    )

  val nginxLogs = typeAdded
    .filter(col("type") === "nginx")
    .withColumn("status", regexp_extract(col("log"), """HTTP/1\.1" (\d{3})""", 1).cast("int"))
    .withColumn("is_error", when(col("status") >= 400, 1).otherwise(0))
    .withColumn("bytes", regexp_extract(col("log"), """ (\d+)$""", 1).cast("long"))
    .withColumn("endpoint", regexp_extract(col("log"), """GET (/\S*)""", 1))





  val topEndpointsBase = nginxLogs
    .groupBy(window(col("event_ts"), "30 seconds"), col("endpoint"))
    .agg(
      count("*").as("hits"),
      sum("bytes").as("total_bytes"),
      sum("is_error").as("error_hits")
    )
    .select(
      col("window.start").cast("timestamp").as("window_start"),
      col("window.end").cast("timestamp").as("window_end"),
      col("endpoint"),
      col("hits"),
      col("total_bytes"),
      col("error_hits")
    )

  val topErrorsBase = nginxLogs
    .filter(col("status") >= 400)
    .withColumn(
      "error_msg",
      regexp_extract(col("log"), "\"[A-Z]+ ([^ ]+) HTTP", 1)
    )
    .groupBy(
      window(col("event_ts"), "30 seconds"),
      col("error_msg")
    )
    .agg(count("*").as("cnt"))
    .select(
      col("window.start").cast("timestamp").as("window_start"),
      col("window.end").cast("timestamp").as("window_end"),
      col("error_msg").as("message"),
      col("cnt")
    )


  def writeTopEndpoints(batchDF: DataFrame, batchId: Long): Unit = {
    if (!batchDF.isEmpty) {
      val ranked = batchDF
        .withColumn("rank",
          row_number().over(Window.partitionBy("window_start").orderBy(col("hits").desc))
        )
        .filter(col("rank") <= 10)
        .select(
          col("window_start"),
          col("window_end"),
          col("rank"),
          col("endpoint"),
          col("hits"),
          col("total_bytes"),
          col("error_hits")
        )
      println(s"\n[info] Batch $batchId → top_endpoints_30s")
      ranked.orderBy("window_start", "rank").show(50, truncate = false)
      ranked.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "log_ks", "table" -> "top_endpoints_30s"))
        .mode("append")
        .save()
    }
  }

  def writeTopErrors(batchDF: DataFrame, batchId: Long): Unit = {
    if (!batchDF.isEmpty) {

      val ranked = batchDF
        .withColumn(
          "rank",
          row_number().over(
            Window.partitionBy("window_start")
              .orderBy(col("cnt").desc)
          )
        )
        .filter(col("rank") <= 5)
        .select(
          col("window_start"),
          col("window_end"),
          col("rank"),
          col("message"),
          col("cnt")
        )

      println(s"\n[info] Batch $batchId → top_errors_30s")
      ranked.orderBy("window_start", "rank").show(50, truncate = false)

      ranked.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "log_ks", "table" -> "top_errors_30s"))
        .mode("append")
        .save()
    }
  }

  val query1 = topEndpointsBase.writeStream
    .outputMode("complete")
    .foreachBatch(writeTopEndpoints _)
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .option("checkpointLocation", "file:///tmp/spark-checkpoint/top_endpoints_30s")
    .start()

  val query2 = topErrorsBase.writeStream
    .outputMode("complete")
    .foreachBatch(writeTopErrors _)
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .option("checkpointLocation", "file:///tmp/spark-checkpoint/top_errors_30s")
    .start()

  query1.awaitTermination()
  query2.awaitTermination()
}