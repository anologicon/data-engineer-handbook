-- Remove duplicates from game_details by keeping only the first row per (game_id, team_id, player_id)
-- row_number_qualify: Assigns a row number to each row partitioned by game_id, team_id, and player_id
WITH row_number_qualify AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS __row
    FROM game_details gd
)
SELECT *
FROM row_number_qualify
WHERE __row = 1;