package com.example.logagg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, _}

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


  val topIPsDF = nginxLogs
    .withColumn("ip", regexp_extract(col("log"), """^(\S+)""", 1))
    .groupBy(window(col("event_ts"), "30 seconds"), col("ip"))
    .agg(
      count("*").as("hits"),
      sum("bytes").as("total_bytes"),
      sum("is_error").as("error_hits")
    )
    .select(
      col("window.start").cast("timestamp").as("window_start"),
      col("window.end").cast("timestamp").as("window_end"),
      col("ip"),
      col("hits"),
      col("total_bytes"),
      col("error_hits")
    )

  val numberOfStatusCounts = nginxLogs
    .groupBy(window(col("event_ts"), "30 seconds"), col("status"))
    .count()
    .select(
      col("window.start").cast("timestamp").as("window_start"),
      col("status").as("status_code"),
      col("count").as("cnt")
    )

  val avgOSMetrics = typeAdded.filter(col("type") === "os")
    .withColumn("metric", regexp_extract(col("log"), """\[OS\]\s*\[(\w+)\]""", 1))
    .withColumn("value", regexp_extract(col("log"), """Usage at (\d+)%""", 1).cast("double"))
    .groupBy(window(col("event_ts"), "30 seconds"), col("metric"))
    .agg(avg("value").as("avg_value"))
    .select(
      col("window.start").cast("timestamp").as("window_start"),
      col("metric"),
      round(col("avg_value"), 2).as("avg_value")
    )

  val appErrorCounts = typeAdded
    .filter(col("type") === "app")
    .withColumn("level",  regexp_extract(col("log"), """\[(\w+)\]""", 1))
    .withColumn("module", regexp_extract(col("log"), """\[(\w+)\]\s*\[([A-Za-z]+)\]""", 2))
    .groupBy(window(col("event_ts"), "30 seconds"), col("module"))
    .agg(
      count(when(col("level") === "ERROR", 1)).as("error_count"),
      count(when(col("level") === "WARN",  1)).as("warn_count"),
      count(when(col("level") === "INFO",  1)).as("info_count")
    )
    .select(
      col("window.start").cast("timestamp").as("window_start"),
      col("window.end").cast("timestamp").as("window_end"),
      col("module"),
      col("error_count"),
      col("warn_count")
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

  def writeIPsToCassandra(batchDF: DataFrame, batchId: Long): Unit = {
    if (!batchDF.isEmpty) {
      val ranked = batchDF
        .withColumn("rank", row_number().over(Window.partitionBy("window_start").orderBy(col("hits").desc)))
        .filter(col("rank") <= 10)
        .select(
          "window_start",
          "window_end",
          "rank",
          "ip",
          "hits",
          "total_bytes",
          "error_hits"
        )

      println(s"\n[info] Batch $batchId → top_ips_30s")
      ranked.orderBy("window_start", "rank").show(20, truncate = false)

      ranked.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "log_ks", "table" -> "top_ips_30s"))
        .mode("append")
        .save()
    }
  }

  def writeNumberOfStatusCodesToCassandra(batchDF: DataFrame, batchId: Long): Unit = {
    if (!batchDF.isEmpty) {
      println(s"\n[info] Batch $batchId → status_codes_30s")
      batchDF.orderBy("window_start", "status_code").show(50, truncate = false)

      batchDF.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "log_ks", "table" -> "status_codes_30s"))
        .mode("append")
        .save()
    }
  }

  def writeAvgOSMetrics(batchDF: DataFrame, batchId: Long): Unit = {
    if (!batchDF.isEmpty) {
      println(s"\n[info] Batch $batchId → os_metrics_avg_30s")
      batchDF.orderBy("window_start", "metric").show(20, truncate = false)

      batchDF.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "log_ks", "table" -> "os_metrics_avg_30s"))
        .mode("append")
        .save()
    }
  }

  def writeAppModuleErrors(batchDF: DataFrame, batchId: Long): Unit = {
    if (!batchDF.isEmpty) {
      println(s"\n[info] Batch $batchId → app_module_errors_30s")
      batchDF
        .orderBy(col("error_count").desc, col("window_start"))
        .show(20, truncate = false)

      batchDF.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> "log_ks", "table" -> "app_module_errors_30s"))
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

  val query3 = topIPsDF.writeStream
    .outputMode("complete")
    .foreachBatch(writeIPsToCassandra _)
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .option("checkpointLocation", "file:///tmp/spark-checkpoint/top_ips_30s")
    .start()

  val query4 = numberOfStatusCounts.writeStream
    .outputMode("complete")
    .foreachBatch(writeNumberOfStatusCodesToCassandra _)
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .option("checkpointLocation", "file:///tmp/spark-checkpoint/status_codes_30s")
    .start()

  val query5 = avgOSMetrics.writeStream
    .outputMode("complete")
    .foreachBatch(writeAvgOSMetrics _)
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .option("checkpointLocation", "file:///tmp/spark-checkpoint/os_metrics_avg_30s")
    .start()

  val query6 = appErrorCounts.writeStream
    .outputMode("complete")
    .foreachBatch(writeAppModuleErrors _)
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .option("checkpointLocation", "file:///tmp/spark-checkpoint/app_module_errors_30s")
    .start()



  query1.awaitTermination()
  query2.awaitTermination()
  query3.awaitTermination()
  query4.awaitTermination()
  query5.awaitTermination()
  query6.awaitTermination()

}