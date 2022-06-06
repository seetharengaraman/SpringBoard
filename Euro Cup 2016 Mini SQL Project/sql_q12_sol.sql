/*Write a SQL query that returns the total number of goals scored by each position on
each country’s team. Do not include positions which scored no goals*/

mysql>    SELECT pp.position_desc AS position,sc.country_name AS team,COUNT(goal_id)
    ->      FROM goal_details gd
    ->INNER JOIN player_mast pm 
    ->        ON gd.player_id = pm.player_id 
    ->       AND gd.goal_type != 'O'
    ->INNER JOIN playing_position pp ON pm.posi_to_play = pp.position_id 
    ->INNER JOIN soccer_country sc ON pm.team_id = sc.country_id
    ->  GROUP BY pp.position_desc,sc.country_name
    ->    HAVING COUNT(goal_id) > 0
    ->  ORDER BY team;
+-------------+---------------------+----------------+
| position    | team                | count(goal_id) |
+-------------+---------------------+----------------+
| Defenders   | Albania             |              1 |
| Midfielders | Austria             |              1 |
| Defenders   | Belgium             |              4 |
| Midfielders | Belgium             |              5 |
| Midfielders | Croatia             |              4 |
| Defenders   | Croatia             |              1 |
| Defenders   | Czech Republic      |              2 |
| Midfielders | England             |              1 |
| Defenders   | England             |              3 |
| Defenders   | France              |              9 |
| Midfielders | France              |              4 |
| Defenders   | Germany             |              4 |
| Midfielders | Germany             |              3 |
| Defenders   | Hungary             |              4 |
| Midfielders | Hungary             |              1 |
| Midfielders | Iceland             |              3 |
| Defenders   | Iceland             |              5 |
| Midfielders | Italy               |              1 |
| Defenders   | Italy               |              5 |
| Defenders   | Northern Ireland    |              2 |
| Defenders   | Poland              |              2 |
| Midfielders | Poland              |              2 |
| Defenders   | Portugal            |              8 |
| Midfielders | Portugal            |              1 |
| Midfielders | Republic of Ireland |              3 |
| Defenders   | Romania             |              2 |
| Defenders   | Russia              |              1 |
| Midfielders | Russia              |              1 |
| Midfielders | Slovakia            |              3 |
| Defenders   | Spain               |              4 |
+-------------+---------------------+----------------+
30 rows in set (0.00 sec)
