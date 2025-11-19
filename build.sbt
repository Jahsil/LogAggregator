ThisBuild / scalaVersion := "2.13.12"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"


import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy
import sbtassembly.PathList


lazy val root = (project in file("."))
  .settings(
    name := "LogAggregator",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql"  % "3.5.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
      "org.apache.kafka" % "kafka-clients" % "3.6.1",
      "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.0",
      "com.typesafe" % "config" % "1.4.5",
      // Akka libraries
      "com.typesafe.akka" %% "akka-actor-typed" % "2.8.8",
      "com.typesafe.akka" %% "akka-stream" % "2.8.8",
      "com.typesafe.akka" %% "akka-http" % "10.5.3"
    ),


    // Required for Java 17+ module access
    fork := true,

    javaOptions ++= Seq(
      "-Xmx2g",

      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",

      "-Dlog4j2.configurationFile=file:src/main/resources/log4j2.xml"
    )
  )

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"               => MergeStrategy.concat
  case PathList("com","datastax", _ @ _*) => MergeStrategy.first
  case PathList("io","netty", _ @ _*) => MergeStrategy.first
  case x                              => MergeStrategy.first
}