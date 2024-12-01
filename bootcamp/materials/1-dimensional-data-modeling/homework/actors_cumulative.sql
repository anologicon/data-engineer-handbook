
WITH films_last_year AS (
	
	SELECT * FROM actors
	WHERE year = 1971
	
), films_this_year as (
	
	select * from actor_films
	where year = 1972

)
insert into actors
	select
		coalesce(films_last_year.actor, films_this_year.actor) as actor,
		coalesce(films_last_year.actorid, films_this_year.actorid) as actorid,
		coalesce(
			films_last_year.films, 
			array[]::movie[]
		)
		||
		case when films_this_year.actorid is not null then
		    array_agg(row(
				films_this_year.film,
				films_this_year.votes,
				films_this_year.rating,
				films_this_year.filmid
			)::movie)
		else
		ARRAY[]::movie[] end as films,
		case
			when films_this_year.actorid is not null then
				(
				 case when avg(rating) > 8 then 'star'
				 when avg(rating) > 7 and avg(rating) <= 8 then 'good'
				 when avg(rating) > 6 and avg(rating) <= 7 then 'average'
				 else 'bad'
				 end
				)::quality_class
			else films_last_year.quality_class
		end as quality_class
		,
		1972 as year,
		case when films_this_year.actorid is null then false else true end as is_active
	
	from films_last_year 
	full outer join films_this_year on films_last_year.actorid = films_this_year.actorid
	
	group by 1,2, films_last_year.films, films_last_year.quality_class, films_this_year.actorid;

