package com.example.logagg

import java.io.{BufferedWriter, FileWriter}
import scala.util.Random

object ContinuousLogGenerator extends App {

  private val logFile = "logs/output.log"
  private val intervalMs = 500
  val random = new Random()

  new java.io.File("logs").mkdirs()
  println(s"🟢 Log generator started... Writing to $logFile every ${intervalMs}ms")

  private def writeLogLine(): Unit = {
    val writer = new BufferedWriter(new FileWriter(logFile, true))
    val logLine = LogGenerator.randomLog()
    writer.write(logLine + "\n")
    writer.close()
    println(logLine)
  }

  while (true) {
    writeLogLine()
    Thread.sleep(intervalMs + random.nextInt(1000))
  }
}
