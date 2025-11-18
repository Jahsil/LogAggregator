CREATE KEYSPACE IF NOT EXISTS log_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS log_ks.top_endpoints_30s (
    bucket_date date,
    window_start timestamp,
    rank int,
    endpoint text,
    hits bigint,
    total_bytes bigint,
    error_hits bigint,
    PRIMARY KEY (bucket_date, window_start, rank)
) WITH CLUSTERING ORDER BY (window_start DESC, rank ASC);

CREATE TABLE IF NOT EXISTS log_ks.top_errors_30s (
    bucket_date date,
    window_start timestamp,
    rank int,
    message text,
    cnt bigint,
    PRIMARY KEY (bucket_date, window_start, rank)
) WITH CLUSTERING ORDER BY (window_start DESC, rank ASC);

CREATE TABLE IF NOT EXISTS log_ks.top_ips_30s (
    bucket_date date,
    window_start timestamp,
    rank int,
    ip text,
    hits bigint,
    total_bytes bigint,
    error_hits bigint,
    PRIMARY KEY (bucket_date, window_start, rank)
) WITH CLUSTERING ORDER BY (window_start DESC, rank ASC);

CREATE TABLE IF NOT EXISTS log_ks.status_codes_30s (
    bucket_date date,
    window_start timestamp,
    status_code int,
    cnt bigint,
    PRIMARY KEY (bucket_date, window_start, status_code)
) WITH CLUSTERING ORDER BY (window_start DESC, status_code ASC);

CREATE TABLE IF NOT EXISTS log_ks.app_module_errors_30s (
    bucket_date date,
    window_start timestamp,
    module text,
    error_count bigint,
    warn_count bigint,
    PRIMARY KEY (bucket_date, window_start, module)
) WITH CLUSTERING ORDER BY (window_start DESC, module ASC);

CREATE TABLE IF NOT EXISTS log_ks.os_metrics_avg_30s (
    bucket_date date,
    window_start timestamp,
    metric text,
    avg_value double,
    PRIMARY KEY (bucket_date, window_start, metric)
) WITH CLUSTERING ORDER BY (window_start DESC, metric ASC);
