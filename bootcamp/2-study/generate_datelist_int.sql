drop table user_devices_datelist_int;
create table user_devices_datelist_int (
	user_id text,
	datelist_int json,
	date_ref date
);

insert into user_devices_datelist_int
with date_series as (
	SELECT generate_series('2022-12-31', '2023-01-31', INTERVAL '1 day') AS valid_date
)
SELECT 
	user_id,
	JSONB_OBJECT_AGG(key,(
		with pow_cte as (
		  	select sum(
		  	case when array((select jsonb_array_elements_text(uc.device_activity_datelist -> key)::date)) @> array[date(d.valid_date)]
		  	then POW(2,32 - EXTRACT(DAY FROM DATE('2023-01-31') - d.valid_date)) else 0 end)::bigint::bit(32) bit_date
		  	from date_series as d
		)
		select  bit_date from pow_cte
	)) as datelist_int,
	DATE('2023-01-31') as date_ref
	
	FROM user_devices_cumulated uc, JSONB_OBJECT_KEYS(device_activity_datelist) AS key
	group by user_id

