DROP TABLE host_activity_reduced;
CREATE TABLE host_activity_reduced (
    month DATE,
    host TEXT,
    hit_array BIGINT [],
    unique_visitors_array BIGINT [],
    PRIMARY KEY (month, host)
);
INSERT INTO host_activity_reduced
WITH yesterday AS (
        SELECT *
        FROM host_activity_reduced
        WHERE month = '2023-01-01'
    ),
    today AS (
        SELECT host,
            DATE(DATE_TRUNC('day', DATE(event_time))) AS today_date,
            COUNT(1) AS num_hits,
            COUNT(DISTINCT user_id) AS num_unique
        FROM events
        WHERE DATE_TRUNC('day', DATE(event_time)) = DATE('2023-01-02')
            AND user_id IS NOT NULL
        GROUP BY host,
            DATE_TRUNC('day', DATE(event_time))
    )
SELECT DATE('2023-01-01') AS month,
    COALESCE(y.host, t.host) AS host,
    COALESCE(
        y.hit_array,
        ARRAY_FILL(
            NULL::BIGINT,
            ARRAY [DATE('2023-01-02') - DATE('2023-01-01')]
        )
    ) || ARRAY [t.num_hits] AS hit_array,
    COALESCE(
        y.unique_visitors_array,
        ARRAY_FILL(
            NULL::BIGINT,
            ARRAY [DATE('2023-01-02') - DATE('2023-01-01')]
        )
    ) || ARRAY [t.num_unique] AS unique_visitors_array
FROM yesterday y
    FULL OUTER JOIN today t ON y.host = t.host
ON CONFLICT (host, month) DO UPDATE
SET (hit_array, unique_visitors_array) = (
        EXCLUDED.hit_array,
        EXCLUDED.unique_visitors_array
    );