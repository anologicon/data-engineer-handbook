WITH
	lebron_games AS (
		SELECT
			game_id,
			games.game_date_est,
			player_id,
			player_name,
			CASE WHEN pts > 10 THEN 1 ELSE 0 END AS over_10
		FROM game_details
		INNER JOIN games USING(game_id)
		WHERE LOWER(player_name) LIKE '%lebron%'
		ORDER BY game_date_est DESC
	),

	grp_cte AS (
		SELECT
			*,
			ROW_NUMBER() OVER (ORDER BY game_date_est) -
			ROW_NUMBER() OVER (PARTITION BY over_10 ORDER BY game_date_est) AS grp
		FROM lebron_games
		ORDER BY game_date_est
	)

SELECT
	COUNT(1) AS consecutive
FROM grp_cte
GROUP BY grp
ORDER BY 1 DESC
LIMIT 1
