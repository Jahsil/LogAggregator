package com.example.logagg

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpEntity

import com.datastax.oss.driver.api.core.CqlSession

import scala.concurrent.{ExecutionContext, Future}
import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

object MetricsExporter extends App {

  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "metrics-exporter")
  implicit val ec: ExecutionContext = system.executionContext

  private val session = CqlSession.builder()
    .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
    .withLocalDatacenter("datacenter1")
    .build()

  private def sanitizeBlock(s: String): String =
    Option(s).getOrElse("").replace("\r", "")
      .split("\n").map(_.trim).filter(_.nonEmpty).mkString("\n")

  /** Fetch Status Codes Metric */
  private def fetchStatusCodes(): Future[String] = Future {
    val rows = session.execute("SELECT window_start, status_code, cnt FROM log_ks.status_codes_30s ALLOW FILTERING")
      .iterator().asScala.toSeq

    if (rows.isEmpty) ""
    else {
      val latestTs = rows.map(_.getInstant("window_start")).max
      rows.filter(_.getInstant("window_start") == latestTs).map { row =>
        s"""log_status_code_total{status_code="${row.getInt("status_code")}"} ${row.getLong("cnt")}"""
      }.mkString("\n")
    }
  }

  /** Fetch Top Endpoints Metric */
  private def fetchTopEndpoints(): Future[String] = Future {
    val rows = session.execute(
      "SELECT window_start, rank, endpoint, hits, total_bytes FROM log_ks.top_endpoints_30s ALLOW FILTERING"
    ).iterator().asScala.toSeq

    if (rows.isEmpty) ""
    else {
      val latestTs = rows.map(_.getInstant("window_start")).max
      rows.filter(_.getInstant("window_start") == latestTs).map { row =>
        s"""log_top_endpoints_hits{endpoint="${row.getString("endpoint")}",rank="${row.getInt("rank")}"} ${row.getLong("hits")}
log_top_endpoints_bytes{endpoint="${row.getString("endpoint")}",rank="${row.getInt("rank")}"} ${row.getLong("total_bytes")}"""
      }.mkString("\n")
    }
  }

  /** Fetch Top Errors Metric */
  private def fetchTopErrors(): Future[String] = Future {
    val rows = session.execute(
      "SELECT window_start, rank, message, cnt FROM log_ks.top_errors_30s ALLOW FILTERING"
    ).iterator().asScala.toSeq

    if (rows.isEmpty) ""
    else {
      val latestTs = rows.map(_.getInstant("window_start")).max
      rows.filter(_.getInstant("window_start") == latestTs).map { row =>
        s"""log_top_errors{message="${row.getString("message")}",rank="${row.getInt("rank")}"} ${row.getLong("cnt")}"""
      }.mkString("\n")
    }
  }

  /** Fetch Top IPs Metric */
  private def fetchTopIps(): Future[String] = Future {
    val rows = session.execute(
      "SELECT window_start, rank, ip, hits, total_bytes, error_hits FROM log_ks.top_ips_30s ALLOW FILTERING"
    ).iterator().asScala.toSeq

    if (rows.isEmpty) ""
    else {
      val latestTs = rows.map(_.getInstant("window_start")).max
      rows.filter(_.getInstant("window_start") == latestTs).map { row =>
        s"""log_top_ips_hits{ip="${row.getString("ip")}",rank="${row.getInt("rank")}"} ${row.getLong("hits")}
log_top_ips_bytes{ip="${row.getString("ip")}",rank="${row.getInt("rank")}"} ${row.getLong("total_bytes")}
log_top_ips_error_hits{ip="${row.getString("ip")}",rank="${row.getInt("rank")}"} ${row.getLong("error_hits")}"""
      }.mkString("\n")
    }
  }

  /** Fetch App Module Errors Metric */
  private def fetchAppModuleErrors(): Future[String] = Future {
    val rows = session.execute(
      "SELECT window_start, module, error_count, warn_count FROM log_ks.app_module_errors_30s ALLOW FILTERING"
    ).iterator().asScala.toSeq

    if (rows.isEmpty) ""
    else {
      val latestTs = rows.map(_.getInstant("window_start")).max
      rows.filter(_.getInstant("window_start") == latestTs).map { row =>
        s"""log_app_module_errors{module="${row.getString("module")}"} ${row.getLong("error_count")}
log_app_module_warns{module="${row.getString("module")}"} ${row.getLong("warn_count")}"""
      }.mkString("\n")
    }
  }

  /** Fetch OS Metrics Avg Metric */
  private def fetchOsMetrics(): Future[String] = Future {
    val rows = session.execute(
      "SELECT window_start, metric, avg_value FROM log_ks.os_metrics_avg_30s ALLOW FILTERING"
    ).iterator().asScala.toSeq

    if (rows.isEmpty) ""
    else {
      val latestTs = rows.map(_.getInstant("window_start")).max
      rows.filter(_.getInstant("window_start") == latestTs).map { row =>
        s"""log_os_metric_avg{metric="${row.getString("metric")}"} ${row.getDouble("avg_value")}"""
      }.mkString("\n")
    }
  }

  /** Prometheus /metrics HTTP endpoint */
  val route =
    path("metrics") {
      get {
        complete {
          for {
            sc   <- fetchStatusCodes()
            top  <- fetchTopEndpoints()
            err  <- fetchTopErrors()
            ips  <- fetchTopIps()
            mod  <- fetchAppModuleErrors()
            osm  <- fetchOsMetrics()
          } yield {
            val sb = new StringBuilder

            if (sc.nonEmpty) {
              sb.append("# HELP log_status_code_total Total logs by HTTP status code\n")
              sb.append("# TYPE log_status_code_total counter\n")
              sb.append(sanitizeBlock(sc)).append("\n\n")
            }

            if (top.nonEmpty) {
              sb.append("# HELP log_top_endpoints_hits Top endpoints by hit count\n")
              sb.append("# TYPE log_top_endpoints_hits gauge\n")
              sb.append("# HELP log_top_endpoints_bytes Top endpoints by bytes sent\n")
              sb.append("# TYPE log_top_endpoints_bytes gauge\n")
              sb.append(sanitizeBlock(top)).append("\n\n")
            }

            if (err.nonEmpty) {
              sb.append("# HELP log_top_errors Top error messages\n")
              sb.append("# TYPE log_top_errors gauge\n")
              sb.append(sanitizeBlock(err)).append("\n\n")
            }

            if (ips.nonEmpty) {
              sb.append("# HELP log_top_ips_hits Top IPs by hit count\n")
              sb.append("# TYPE log_top_ips_hits gauge\n")
              sb.append("# HELP log_top_ips_bytes Top IPs by bytes sent\n")
              sb.append("# TYPE log_top_ips_bytes gauge\n")
              sb.append("# HELP log_top_ips_error_hits Top IPs by error hits\n")
              sb.append("# TYPE log_top_ips_error_hits gauge\n")
              sb.append(sanitizeBlock(ips)).append("\n\n")
            }

            if (mod.nonEmpty) {
              sb.append("# HELP log_app_module_errors Error counts per module\n")
              sb.append("# TYPE log_app_module_errors gauge\n")
              sb.append("# HELP log_app_module_warns Warning counts per module\n")
              sb.append("# TYPE log_app_module_warns gauge\n")
              sb.append(sanitizeBlock(mod)).append("\n\n")
            }

            if (osm.nonEmpty) {
              sb.append("# HELP log_os_metric_avg Average OS metrics per 30s window\n")
              sb.append("# TYPE log_os_metric_avg gauge\n")
              sb.append(sanitizeBlock(osm)).append("\n")
            }

            HttpEntity(ContentTypes.`text/plain(UTF-8)`, sb.toString.replace("\r", ""))
          }
        }
      }
    }

  Http().newServerAt("0.0.0.0", 9095).bind(route)
  println("🚀 Metrics exporter running on http://localhost:9095/metrics")
}
