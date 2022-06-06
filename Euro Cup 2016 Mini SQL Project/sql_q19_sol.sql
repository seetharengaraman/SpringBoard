/*Write a SQL query to find the number of captains who were also goalkeepers*/

mysql>    SELECT COUNT(DISTINCT player_captain) AS goalkeeper_captain_count
    ->      FROM match_captain mc
    ->INNER JOIN match_details md
    ->        ON mc.match_no = md. match_no
    ->       AND mc.team_id = md.team_id
    ->       AND mc.player_captain = md.player_gk;
+--------------------------+
| goalkeeper_captain_count |
+--------------------------+
|                        4 |
+--------------------------+
1 row in set (0.00 sec)

