package com.example.logagg

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random

object KafkaLogger extends App {

  // configure kafka
  val kafkaBootstrap = "localhost:9092"
  val topic = "logs_topic"

  val props = new Properties()
  props.put("bootstrap.servers", kafkaBootstrap)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  //  Sample log data
  val random = new Random()
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  val ipAddresses = Seq("192.168.1.10", "10.0.0.15", "172.16.0.22", "203.0.113.5")
  val endpoints = Seq("/index.html", "/login", "/api/v1/data", "/dashboard", "/metrics")
  val statuses = Seq(200, 201, 400, 401, 403, 404, 500)
  val appModules = Seq("AuthService", "PaymentService", "UserService", "AnalyticsService")
  val osMetrics = Seq("CPU", "MEMORY", "DISK", "NETWORK")

  def nginxLog(): String = {
    val ip = ipAddresses(random.nextInt(ipAddresses.length))
    val endpoint = endpoints(random.nextInt(endpoints.length))
    val status = statuses(random.nextInt(statuses.length))
    val bytes = random.nextInt(5000) + 100
    s"""$ip - - [${LocalDateTime.now().format(formatter)}] "GET $endpoint HTTP/1.1" $status $bytes"""
  }

  def appLog(): String = {
    val level = Seq("INFO", "WARN", "ERROR")(random.nextInt(3))
    val module = appModules(random.nextInt(appModules.length))
    val message = level match {
      case "INFO" => s"$module started successfully."
      case "WARN" => s"$module experiencing slow response."
      case "ERROR" => s"$module failed to connect to database."
    }
    s"${LocalDateTime.now().format(formatter)} [$level] [$module] $message"
  }

  def osLog(): String = {
    val metric = osMetrics(random.nextInt(osMetrics.length))
    val value = random.nextInt(100)
    s"${LocalDateTime.now().format(formatter)} [OS] [$metric] Usage at $value%"
  }

  println("Kafka Log Generator started...")

  while (true) {
    val logLine = random.nextInt(3) match {
      case 0 => nginxLog()
      case 1 => appLog()
      case 2 => osLog()
    }

    val record = new ProducerRecord[String, String](topic, null, logLine)
    producer.send(record)
    println(logLine)

    Thread.sleep(500 + random.nextInt(500))
  }
}