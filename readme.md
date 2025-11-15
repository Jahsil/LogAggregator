
#  Real-Time Log Aggregator
### Apache Spark Structured Streaming + Kafka + Cassandra

This project demonstrates a **real-time log processing pipeline** using:

- **Kafka** — log ingestion
- **Spark Structured Streaming** — processing and aggregations
- **Cassandra** — persistent storage for analytics queries

It includes:
1. A **Kafka log generator** (`KafkaLogger`) that continuously publishes simulated logs.
2. A **Spark streaming consumer** (`SparkKafkaConsumer`) that:
    - Reads logs from Kafka
    - Classifies log types (`nginx`, `app`, `os`)
    - Computes 10-second log counts
    - Computes 30-second NGINX error rate
    - Writes results to **Cassandra**
    - Prints batches to console

---


---

#  Requirements

You need the following services running:

| Component | Version | Notes                               |
|----------|---------|-------------------------------------|
| **Apache Kafka** | latest  | Single-broker local setup is fine   |
| **Apache Spark** | 3.5+    | Using Structured Streaming          |
| **Cassandra** | latest  | Must have keyspace + tables created |
| **Scala** | 2.13.12 | Matches Spark version               |
| **sbt** | 1.1+    | Build tool                          |
 **IntelliJ** | latest  | IDE                                 |
 **JDK** | 17.0.17 | Java version   


#  Cassandra Setup

Create keyspace:

```sql
cqlsh -f schema.cql
```

#  Procedure

Make sure all services are up:

```sql
brew services list

You should see :
Name      Status  User   File
cassandra started eyouel ~/Library/LaunchAgents/homebrew.mxcl.cassandra.plist
kafka     started eyouel ~/Library/LaunchAgents/homebrew.mxcl.kafka.plist
```

Rebuild sbt or use UI to restart it:

```sql
sbt reload
```

Run consumer:

```sql
sbt "runMain com.example.logagg.SparkKafkaConsumer"
```

