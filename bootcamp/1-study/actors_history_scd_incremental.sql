DROP TYPE IF EXISTS scd_type;
CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active BOOL,
    start_date INTEGER,
    end_date INTEGER
);
INSERT INTO actors_history_scd 
WITH last_season_scd AS (
        SELECT *
        FROM actors_history_scd
        WHERE end_date = 9999
    ),
    historical_scd AS (
        SELECT actor,
            actorid,
            quality_class,
            is_active,
            start_date,
            end_date
        FROM actors_history_scd
        WHERE end_date < 9999
    ),
    this_season_data AS (
        SELECT *
        FROM actors
        WHERE year = 1974
    ),
    unchanged_records AS (
        SELECT ts.actor,
            ts.actorid,
            ts.quality_class,
            ts.is_active,
            ls.start_date,
            ls.end_date
        FROM this_season_data ts
            JOIN last_season_scd ls ON ls.actorid = ts.actorid
        WHERE ts.quality_class = ls.quality_class
            AND ts.is_active = ls.is_active
    ),
    new_records AS (
        SELECT ts.actor,
            ts.actorid,
            ts.quality_class,
            ts.is_active,
            ts.year AS start_date,
            9999 AS end_date
        FROM this_season_data ts
            LEFT JOIN last_season_scd ls ON ls.actorid = ts.actorid
        WHERE ls.actorid IS NULL
    ),
    changed_records AS (
        SELECT ts.actor,
            ts.actorid,
            UNNEST(
                ARRAY [
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_date,
                        ts.year
                    )::scd_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.year,
                        9999
                    )::scd_type
                ]
            ) AS records
        FROM this_season_data ts
            LEFT JOIN last_season_scd ls ON ls.actorid = ts.actorid
        WHERE ts.quality_class != ls.quality_class
            OR ts.is_active != ls.is_active
    ),
    unnested_changed_records AS (
        SELECT actor,
            actorid,
            (records::scd_type).quality_class,
            (records::scd_type).is_active,
            (records::scd_type).start_date,
            (records::scd_type).end_date
        FROM changed_records
    )
SELECT *
FROM (
        SELECT *
        FROM historical_scd
        UNION ALL
        SELECT *
        FROM unchanged_records
        UNION ALL
        SELECT *
        FROM unnested_changed_records
        UNION ALL
        SELECT *
        FROM new_records
    ) a