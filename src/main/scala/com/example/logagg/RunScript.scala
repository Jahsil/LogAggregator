package com.example.logagg

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.ConfigFactory
import scala.io.Source
import java.net.InetSocketAddress

object RunScript extends App {

  private val config = ConfigFactory.load()

  // Read Cassandra configuration (adjust the path to match your config structure)
  private val host       = config.getString("cassandra.host")
  private val port       = config.getInt("cassandra.port")
  private val datacenter = config.getString("cassandra.datacenter")

  private val session = CqlSession.builder()
    .addContactPoint(new InetSocketAddress(host, port))
    .withLocalDatacenter(datacenter)
    .build()

  private val script = Source.fromResource("schema.cql").mkString
  script.split(";")
    .map(_.trim)
    .filter(_.nonEmpty)
    .foreach(stmt => session.execute(stmt))

  println("Cassandra schema initialized!")
  session.close()
}
