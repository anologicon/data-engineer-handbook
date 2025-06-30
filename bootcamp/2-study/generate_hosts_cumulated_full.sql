DROP TABLE IF EXISTS hosts_cumulated;
CREATE TABLE hosts_cumulated (host TEXT, host_activity_datelist DATE[]);
INSERT INTO hosts_cumulated
SELECT host,
    ARRAY_AGG(DISTINCT DATE(event_time)) AS host_activity_datelist
FROM events
GROUP BY 1;