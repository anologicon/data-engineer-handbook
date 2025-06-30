-- Drop the table if it exists
DROP TABLE IF EXISTS hosts_cumulated;
-- Create a table to store hosts and their activity dates with a primary key
CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    PRIMARY KEY (host)
);

-- Insert or update host activity dates incrementally
-- today: Gets the current state of the hosts_cumulated table
INSERT INTO hosts_cumulated
WITH today AS (
    SELECT * FROM hosts_cumulated
),
-- yesterday: Gets the hosts that had activity on the specified date
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
