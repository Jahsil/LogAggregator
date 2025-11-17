CREATE KEYSPACE IF NOT EXISTS log_ks
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS log_ks.top_endpoints_30s (
    window_start   timestamp,
    window_end     timestamp,
    rank           int,
    endpoint       text,
    hits           bigint,
    total_bytes    bigint,
    error_hits     bigint,
    PRIMARY KEY (window_start, rank)
) WITH CLUSTERING ORDER BY (rank ASC);


CREATE TABLE IF NOT EXISTS log_ks.top_errors_30s (
    window_start   timestamp,
    window_end     timestamp,
    rank           int,
    message        text,
    cnt            bigint,
    PRIMARY KEY (window_start, rank)
) WITH CLUSTERING ORDER BY (rank ASC);

CREATE TABLE IF NOT EXISTS log_ks.top_ips_30s (
  window_start timestamp,
  window_end timestamp,
  rank int,
  ip text,
  hits bigint,
  total_bytes bigint,
  error_hits bigint,
  PRIMARY KEY (window_start, rank)
) WITH CLUSTERING ORDER BY (rank ASC);

CREATE TABLE IF NOT EXISTS log_ks.status_codes_30s (
  window_start timestamp,
  status_code int,
  cnt bigint,
  PRIMARY KEY (window_start, status_code)
);

CREATE TABLE IF NOT EXISTS log_ks.app_module_errors_30s (
  window_start timestamp,
  window_end timestamp,
  module text,
  error_count bigint,
  warn_count bigint,
  PRIMARY KEY (window_start, module)
);

CREATE TABLE IF NOT EXISTS log_ks.os_metrics_avg_30s (
  window_start timestamp,
  metric text,
  avg_value double,
  PRIMARY KEY (window_start, metric)
);

