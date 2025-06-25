DROP TABLE IF EXISTS actors_history_scd;

create table actors_history_scd (
	actorid TEXT,
	actor TEXT,
	quality_class quality_class,
	is_active BOOL,
	start_date INTEGER,
	end_date INTEGER,
	primary key (actorid, start_date)
);
