drop table processed_events_aggregated_ip_host;
CREATE TABLE IF NOT EXISTS processed_events_aggregated_ip_host (
    ip VARCHAR,
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
);
