WITH games_win AS (
    SELECT g.game_id,
        g.game_date_est,
        ARRAY [
            json_build_object(
                'team_id', g.home_team_id,
                'team_win', CASE WHEN g.home_team_wins = 1 THEN 1 ELSE 0 END
            ),
            json_build_object(
                'team_id', g.visitor_team_id,
                'team_win', CASE WHEN g.home_team_wins = 0 THEN 1 ELSE 0 END
            )
        ] AS teams_result
    FROM games g
),
unnest_game AS (
    SELECT game_id,
        game_date_est AS date_ref,
        team_result->>'team_id' AS team_ref,
        CAST(team_result->>'team_win' AS real) AS win
    FROM games_win,
        UNNEST(teams_result) AS team_result
)
SELECT DISTINCT team_ref,
    teams.nickname,
    date_ref,
    SUM(win) OVER (
        PARTITION BY team_ref
        ORDER BY date_ref ASC ROWS BETWEEN 90 PRECEDING AND CURRENT ROW
    )
FROM unnest_game
    LEFT JOIN teams ON CAST(teams.team_id AS text) = unnest_game.team_ref
ORDER BY date_ref DESC,
    4 DESC

LIMIT 1;