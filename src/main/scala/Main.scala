package com.example.logagg
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
//    println("Starting Spark Test...")
//
//    // Create Spark session (local mode)
//    val spark = SparkSession.builder()
//      .appName("SimpleSparkTest")
//      .master("local[*]")  // Run locally using all CPU cores
//      // === SILENCE ALL WARNINGS ===
//      .config("spark.driver.host", "127.0.0.1")                    // Fixes hostname loopback warning
//      .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") // Fixes native-hadoop warning
//      .config("spark.ui.enabled", "false")                         // Disables Spark UI (optional, removes port 4040)
//      .getOrCreate()
//
//    // Only show actual errors
////    spark.sparkContext.setLogLevel("ERROR")
//
//    import spark.implicits._
//
//    // Create data
//    val data = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
//    val rdd = spark.sparkContext.parallelize(data, 4)  // 4 partitions
//
//    // Do some work
//    val count = rdd.count()
//    val sum = rdd.reduce(_ + _)
//    val average = sum.toDouble / count
//
//    // Print results
//    println(s"Count: $count")
//    println(s"Sum: $sum")
//    println(s"Average: $average")
//
//    // Show in DataFrame (optional)
//    val df = data.toDF("number")
//    df.show()
//
//    // Stop Spark
//    spark.stop()
//    println("Spark Test Completed Successfully!")
  }
}
