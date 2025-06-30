DROP TABLE user_devices_datelist_int;
CREATE TABLE user_devices_datelist_int (
	user_id TEXT,
	datelist_int JSON,
	date_ref DATE
);
INSERT INTO user_devices_datelist_int
WITH date_series AS (
		SELECT GENERATE_SERIES('2022-12-31', '2023-01-31', INTERVAL '1 day') AS valid_date
	)
SELECT user_id,
	JSONB_OBJECT_AGG(
		key,
		(
			WITH pow_cte AS (
				SELECT SUM(
						CASE
							WHEN ARRAY(
								(
									SELECT JSONB_ARRAY_ELEMENTS_TEXT(uc.device_activity_datelist->key)::DATE
								)
							) @> ARRAY [DATE(d.valid_date)] THEN POW(
								2,
								32 - EXTRACT(
									DAY
									FROM DATE('2023-01-31') - d.valid_date
								)
							)
							ELSE 0
						END
					)::BIGINT::BIT(32) bit_date
				FROM date_series AS d
			)
			SELECT bit_date
			FROM pow_cte
		)
	) AS datelist_int,
	DATE('2023-01-31') AS date_ref
FROM user_devices_cumulated uc,
	JSONB_OBJECT_KEYS(device_activity_datelist) AS key
GROUP BY user_id;