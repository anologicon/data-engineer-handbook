DROP TABLE IF EXISTS players_carrear_tracking;
CREATE TABLE players_carrear_tracking (
	player_name TEXT,
	first_season_active INT,
	last_season_active INT,
	season_active_sate TEXT,
	season INT,
	PRIMARY KEY (player_name, season)
);

INSERT INTO players_carrear_tracking
WITH
	
	last_season AS (
		SELECT *
		FROM players_carrear_tracking
		WHERE season = 1995
	),
	
	this_season AS (
		SELECT
			player_name,
			season
		FROM player_seasons
		WHERE season = 1996
	)
	
	SELECT
		COALESCE(this_season.player_name, last_season.player_name) AS player_name,
		COALESCE(last_season.first_season_active, this_season.season) AS first_season_active,
		COALESCE(this_season.season, last_season.last_season_active) AS last_season_active,
		CASE
			WHEN last_season.player_name IS NULL THEN 'New'
			WHEN last_season.last_season_active = this_season.season - 1 THEN 'Continued Playing'
			WHEN last_season.last_season_active < this_season.season - 1 THEN 'Returned from Retirement'
			WHEN this_season.player_name IS NULL AND last_season.last_season_active = last_season.season THEN 'Retired'
			ELSE 'Stayed Retired'
		END AS season_active_sate,
		COALESCE(this_season.season, last_season.season + 1) AS season
			
	FROM this_season
	FULL OUTER JOIN last_season ON this_season.player_name = last_season.player_name
--	WHERE this_season.player_name = 'Michael Jordan'
;