DROP TABLE IF EXISTS hosts_cumulated;
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    PRIMARY KEY (host)
);

INSERT INTO hosts_cumulated
WITH today AS (
    SELECT * FROM hosts_cumulated
),
yesterday AS (
    SELECT
        DISTINCT
        host
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-01')
)
SELECT
    COALESCE(yesterday.host, today.host) AS host,
    CASE 
        WHEN yesterday.host IS NOT NULL
            THEN COALESCE(today.host_activity_datelist, ARRAY[]::DATE[]) || ARRAY[DATE('2023-01-01')]
        ELSE COALESCE(today.host_activity_datelist, ARRAY[]::DATE[]) 
    END AS host_activity_datelist
FROM yesterday
FULL OUTER JOIN today
    ON yesterday.host = today.host
ON CONFLICT (host)
DO 
UPDATE SET host_activity_datelist = EXCLUDED.host_activity_datelist;
