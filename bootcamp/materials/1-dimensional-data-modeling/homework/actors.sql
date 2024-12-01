CREATE TYPE movie AS (
	film TEXT,
	votes REAL,
	rating REAL,
	filmid TEXT
);

CREATE TYPE quality_class AS 
	ENUM ('star','good','average','bad')
;

CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	films movie[],
	quality_class quality_class,
	year real,
	is_active BOOl,
	PRIMARY KEY (actorid, year)
);