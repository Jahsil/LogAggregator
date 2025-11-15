CREATE KEYSPACE IF NOT EXISTS log_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE log_ks;

CREATE TABLE type_counts_10s (
    window_start timestamp,
    type text,
    cnt bigint,
    PRIMARY KEY (window_start, type)
);

CREATE TABLE error_rate_30s (
    window_start timestamp,
    error_rate double,
    total_logs bigint,
    error_logs bigint,
    PRIMARY KEY (window_start)
);

CREATE TABLE latency_30s (
    window_start timestamp,
    avg_ms double,
    p95_ms double,
    p99_ms double,
    PRIMARY KEY (window_start)
);

CREATE TABLE top_endpoints_30s (
    window_start timestamp,
    rank int,
    endpoint text,
    hits bigint,
    total_bytes bigint,
    PRIMARY KEY (window_start, rank)
);

CREATE TABLE top_errors_30s (
    window_start timestamp,
    rank int,
    message text,
    cnt bigint,
    PRIMARY KEY (window_start, rank)
);

CREATE TABLE ip_sessions (
    ip text,
    session_start timestamp,
    session_end timestamp,
    log_count bigint,
    endpoints set<text>,
    PRIMARY KEY (ip, session_start)
) WITH CLUSTERING ORDER BY (session_start DESC);

CREATE TABLE log_counts (
    window_start timestamp,
    window_end timestamp,
    type text,
    cnt bigint,
    PRIMARY KEY (type, window_start)   -- ← Critical: allows upsert
) WITH CLUSTERING ORDER BY (window_start DESC);

-- any one of you can use this command to create the tables on you own machines
-- cqlsh -f schema.cql
