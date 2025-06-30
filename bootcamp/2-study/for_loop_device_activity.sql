-- PL/pgSQL script to update user_devices_cumulated table for each day in the date range
DO $$
DECLARE
    dt RECORD;
    -- date(dt.generate_series) + interval '1 day'
BEGIN
    FOR dt IN SELECT GENERATE_SERIES('2023-01-01'::DATE, '2023-03-31'::DATE, INTERVAL '1 day') LOOP
        INSERT INTO user_devices_cumulated
        -- yesterday: Gets the previous day's user device activity for reference and merging
        WITH yesterday AS (
                SELECT *
                FROM user_devices_cumulated
                WHERE date_reference = DATE(dt.generate_series)
            ),
            -- today: Aggregates today's device activity per user and browser type
            today AS (
                SELECT CAST(events.user_id AS TEXT) AS user_id,
                    DATE_TRUNC('day', DATE(events.event_time)) AS date_event,
                    COALESCE(LOWER(devices.browser_type), 'unknow_device') AS browser_type,
                    COUNT(1) AS num_events
                FROM events
                    LEFT JOIN devices USING(device_id)
                WHERE DATE_TRUNC('day', DATE(events.event_time)) = DATE(dt.generate_series) + INTERVAL '1 day'
                    AND events.user_id IS NOT NULL
                GROUP BY 1, 2, 3
            ),
            -- today_json: Converts today's activity into a JSONB object mapping browser type to an array of dates
            today_json AS (
                SELECT user_id,
                    date_event,
                    TO_JSONB(
                        JSON_OBJECT_AGG(
                            COALESCE(browser_type),
                            ARRAY [DATE(date_event)]
                        )
                    ) AS today_events
                FROM today
                GROUP BY 1, 2
            )
        SELECT COALESCE(today.user_id, yesterday.user_id) AS user_id,
            CASE
                WHEN today.user_id IS NOT NULL THEN (
                    -- Merge today's and yesterday's device activity by browser type
                    WITH all_keys AS (
                        -- all_keys: Union of all browser types present in today and yesterday
                        SELECT key
                        FROM (
                                SELECT JSONB_OBJECT_KEYS(today.today_events) AS key
                                UNION
                                SELECT JSONB_OBJECT_KEYS(yesterday.device_activity_datelist) AS key
                            ) AS merge_k
                    ),
                    -- union_vall: Concatenates arrays of dates for each browser type
                    union_vall AS (
                        SELECT key,
                            COALESCE(today.today_events->key, '[]'::jsonb) || COALESCE(
                                yesterday.device_activity_datelist->key,
                                '[]'::jsonb
                            ) AS array_union
                        FROM all_keys
                    )
                    SELECT JSONB_OBJECT_AGG(key, array_union)
                    FROM union_vall
                )
                ELSE COALESCE(
                    yesterday.device_activity_datelist,
                    today.today_events
                )
            END AS device_activity_datelist,
            COALESCE(
                DATE(today.date_event),
                DATE(yesterday.date_reference + INTERVAL '1 day')
            ) AS date_reference
        FROM yesterday
            FULL OUTER JOIN today_json today USING(user_id);
        
    END LOOP;
END;
$$