WITH
	game_augmented AS (
		SELECT
			game_details.player_name,
			game_details.team_abbreviation,
			COALESCE(CAST(games.season AS TEXT), 'unknow') AS season,
			COALESCE(game_details.pts, 0) AS points,
			CASE 
				WHEN game_details.team_id = games.home_team_id AND home_team_wins = 1 THEN 1
				ELSE 0
			END AS team_win
		FROM game_details
		INNER JOIN games ON games.game_id = game_details.game_id
	),
	
	group_sets AS (
		SELECT
			COALESCE(player_name, '(overall)') AS player_name,
			COALESCE(team_abbreviation, '(overall)') AS team_abbreviation,
			COALESCE(season, '(overall)') AS season,
			SUM(points) AS total_points,
			COUNT(team_win) AS total_wins
		FROM game_augmented
		GROUP BY GROUPING SETS (
			(player_name, team_abbreviation),
			(player_name, season),
			(team_abbreviation)
		)	
	)
	/** WHO SCORED THE MOST POINTS PLAYING FOR ONE TEAM?
	SELECT
		player_name,
		team_abbreviation,
		total_points
	FROM group_sets
	WHERE season = '(overall)' AND player_name != '(overall)' AND team_abbreviation != '(overall)'
	ORDER BY total_points DESC
	LIMIT 1
	**/
	
	/** WHO SCORED THE MOST POINTS IN ONE SEASON
	SELECT
		player_name,
		season,
		total_points
	FROM group_sets
	WHERE season != '(overall)' AND player_name != '(overall)' AND team_abbreviation = '(overall)'
	ORDER BY total_points DESC
	LIMIT 1
	**/
	/**
	WHICH TEAM WON THE MOST GAMES IN ONE SEASON
	**/
	SELECT
		team_abbreviation,
		total_wins
	FROM group_sets
	WHERE season = '(overall)' AND player_name = '(overall)' AND team_abbreviation != '(overall)'
	ORDER BY total_wins DESC
	LIMIT 1
