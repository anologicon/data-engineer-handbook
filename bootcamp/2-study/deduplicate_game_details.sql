WITH row_number_qualify AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS __row
    FROM game_details gd
)
SELECT *
FROM row_number_qualify
WHERE __row = 1;