INSERT INTO actors 
WITH last_year AS (
    SELECT *
    FROM actors
    WHERE year = 1971
),
this_year AS (
    SELECT actor,
        actorid,
        year,
        ARRAY_AGG(
            ROW(
                film,
                votes,
                rating,
                filmid
            )::film_type
        ) AS movies,
        SUM(rating) / COUNT(1) AS avg_rating
    FROM actor_films
    WHERE year = 1972
    GROUP BY 1, 2, 3
),
actors_historycal_concat AS (
    SELECT COALESCE(ly.actor, ty.actor) AS actor,
        COALESCE(ly.actorid, ty.actorid) AS actorid,
        COALESCE(ly.films, ARRAY []::film_type []) || CASE
            WHEN ty.actorid IS NOT NULL THEN (movies)
            ELSE ARRAY []::film_type []
        END AS films,
        CASE
            WHEN ty.actorid IS NOT NULL THEN (
                CASE
                    WHEN ty.avg_rating > 8 THEN 'star'
                    WHEN ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
                    WHEN ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
                    ELSE 'bad'
                END
            )::quality_class
            ELSE ly.quality_class
        END AS quality_class,
        1972 AS year,
        ty.actorid IS NOT NULL AS is_active
    FROM last_year ly
        FULL OUTER JOIN this_year ty ON ly.actorid = ty.actorid
)
SELECT *
FROM actors_historycal_concat
