with
	game_augmented as (
		select
			game_details.player_name,
			game_details.team_abbreviation,
			coalesce(cast(games.season as text), 'unknow') as season,
			coalesce(game_details.pts, 0) as points,
			case 
				when game_details.team_id = games.home_team_id and home_team_wins = 1 then 1
				else 0
			end as team_win
		from game_details
		inner join games on games.game_id = game_details.game_id
	),
	
	group_sets as (
		select
	coalesce(player_name, '(overall)') as player_name,
	coalesce(team_abbreviation, '(overall)') as team_abbreviation,
	coalesce(season, '(overall)') as season,
	SUM(points) as total_points,
	COUNT(team_win) as total_wins
from game_augmented
group by grouping sets (
	(player_name, team_abbreviation),
	(player_name, season),
	(team_abbreviation)
)	
	)
	/** who scored the most points playing for one team?
	select
		player_name,
		team_abbreviation,
		total_points
	from group_sets
	where season = '(overall)' and player_name != '(overall)' and team_abbreviation != '(overall)'
	order by total_points desc
	limit 1
	**/
	
	/** who scored the most points in one season
	select
		player_name,
		season,
		total_points
	from group_sets
	where season != '(overall)' and player_name != '(overall)' and team_abbreviation = '(overall)'
	order by total_points desc
	limit 1
	**/
	
	select
		team_abbreviation,
		total_wins
	from group_sets
	where season = '(overall)' and player_name = '(overall)' and team_abbreviation != '(overall)'
	order by total_wins desc
	limit 1
