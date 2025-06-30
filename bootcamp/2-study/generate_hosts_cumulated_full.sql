-- Drop the table if it exists
DROP TABLE IF EXISTS hosts_cumulated;
-- Create a table to store hosts and their activity dates
CREATE TABLE hosts_cumulated (host TEXT, host_activity_datelist DATE[]);
-- Insert aggregated host activity dates into the table
-- No CTEs are used in this script, so no CTE documentation is needed
INSERT INTO hosts_cumulated
SELECT host,
    ARRAY_AGG(DISTINCT DATE(event_time)) AS host_activity_datelist
FROM events
GROUP BY 1;