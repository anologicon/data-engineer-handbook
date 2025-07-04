drop table if exists players_carrear_tracking;
create table players_carrear_tracking (
	player_name text,
	first_season_active int,
	last_season_active int,
	season_active_sate text,
	season int,
	primary key (player_name, season)
);

insert into players_carrear_tracking
with
	
	last_season as (
		select *
		from players_carrear_tracking
		where season = 1995
	),
	
	this_season as (
		select
			player_name,
			season
		from player_seasons
		where season = 1996
	)
	
	select
		coalesce(this_season.player_name, last_season.player_name) as player_name,
		coalesce(last_season.first_season_active, this_season.season) as first_season_active,
		coalesce(this_season.season, last_season.last_season_active) as last_season_active,
		case
			when last_season.player_name is null then 'New'
			when last_season.last_season_active = this_season.season - 1 then 'Continued Playing'
			when last_season.last_season_active < this_season.season - 1 then 'Returned from Retirement'
			when this_season.player_name is null and last_season.last_season_active = last_season .season then 'Retired'
			else 'Stayed Retired'
		end as season_active_sate,
		coalesce(this_season.season, last_season.season + 1) as season
			
	from this_season
	full outer join last_season on this_season.player_name = last_season.player_name
--	where this_season.player_name = 'Michael Jordan'
;