package com.example.logagg

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random

object LogGenerator {

  private val random = new Random()
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  private val ipAddresses = Seq("192.168.1.10", "10.0.0.15", "172.16.0.22", "203.0.113.5")
  private val endpoints = Seq("/index.html", "/login", "/api/v1/data", "/dashboard", "/metrics")
  private val statuses = Seq(200, 201, 400, 401, 403, 404, 500)
  private val appModules = Seq("AuthService", "PaymentService", "UserService", "AnalyticsService")
  private val osMetrics = Seq("CPU", "MEMORY", "DISK", "NETWORK")

  private def nginxLog(): String = {
    val ip = ipAddresses(random.nextInt(ipAddresses.length))
    val endpoint = endpoints(random.nextInt(endpoints.length))
    val status = statuses(random.nextInt(statuses.length))
    val bytes = random.nextInt(5000) + 100
    s"""$ip - - [${LocalDateTime.now().format(formatter)}] "GET $endpoint HTTP/1.1" $status $bytes"""
  }

  private def appLog(): String = {
    val level = Seq("INFO", "WARN", "ERROR")(random.nextInt(3))
    val module = appModules(random.nextInt(appModules.length))
    val message = level match {
      case "INFO"  => s"$module started successfully."
      case "WARN"  => s"$module experiencing slow response."
      case "ERROR" => s"$module failed to connect to database."
    }
    s"${LocalDateTime.now().format(formatter)} [$level] [$module] $message"
  }

  private def osLog(): String = {
    val metric = osMetrics(random.nextInt(osMetrics.length))
    val value = random.nextInt(100)
    s"${LocalDateTime.now().format(formatter)} [OS] [$metric] Usage at $value%"
  }

  /** Randomly pick one log type */
  def randomLog(): String = random.nextInt(3) match {
    case 0 => nginxLog()
    case 1 => appLog()
    case 2 => osLog()
  }
}
