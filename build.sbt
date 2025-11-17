ThisBuild / scalaVersion := "2.13.12"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"


lazy val root = (project in file("."))
  .settings(
    name := "LogAggregator",
//    idePackagePrefix := Some("com.example.logagg")

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql"  % "3.5.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
      "org.apache.kafka" % "kafka-clients" % "3.6.1",
      "com.datastax.spark" %% "spark-cassandra-connector" % "3.5.0",
      "com.typesafe" % "config" % "1.4.5",
      "com.datastax.oss" % "java-driver-core" % "4.17.0"
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
