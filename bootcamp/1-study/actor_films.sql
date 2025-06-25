DROP TYPE IF EXISTS film_type CASCADE;
DROP TYPE IF EXISTS quality_class CASCADE;
DROP TABLE IF EXISTS actors CASCADE;

CREATE TYPE film_type AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

CREATE TYPE quality_class AS
    ENUM ('bad', 'average', 'good', 'star');

CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films film_type[],
    quality_class quality_class,
    year INTEGER,
    is_active BOOLEAN,
    PRIMARY KEY (actorid, year)
);