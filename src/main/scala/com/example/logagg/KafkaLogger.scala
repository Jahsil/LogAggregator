package com.example.logagg

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random

object KafkaLogger extends App {

  private val config = ConfigFactory.load()
  private val kafkaCfg = config.getConfig("kafka")
  private val genCfg = config.getConfig("generator")

  private def getRequiredString(cfg: com.typesafe.config.Config, path: String): String =
    if (!cfg.hasPath(path)) throw new RuntimeException(s"Missing required config: $path")
    else cfg.getString(path)

  private val kafkaBootstrap = getRequiredString(kafkaCfg, "bootstrap")
  private val topic = getRequiredString(kafkaCfg, "topic")
  private val keySerializer = getRequiredString(kafkaCfg, "keySerializer")
  private val valueSerializer = getRequiredString(kafkaCfg, "valueSerializer")

  private val minDelay = genCfg.getInt("minDelayMs")
  private val maxExtra = genCfg.getInt("maxExtraDelayMs")

  private val props = new Properties()
  props.put("bootstrap.servers", kafkaBootstrap)
  props.put("key.serializer", keySerializer)
  props.put("value.serializer", valueSerializer)
  private val producer = new KafkaProducer[String, String](props)

  private val random = new Random()

  println("Kafka Log Generator started...")

  while (true) {
    val logLine = LogGenerator.randomLog()

    producer.send(new ProducerRecord(topic, logLine), (meta, err) => {
      if (err != null) println(s"Failed to send => ${err.getMessage}")
      else println(s"Sent to ${meta.topic()} [${meta.partition()}] offset ${meta.offset()}")
    })

    println(logLine)
    Thread.sleep(minDelay + random.nextInt(maxExtra))
  }
}
