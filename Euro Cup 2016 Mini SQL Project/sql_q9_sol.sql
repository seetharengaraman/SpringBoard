/*Write a SQL query to find the goalkeeper’s name and jersey number, playing for
Germany, who played in Germany’s group stage matches.*/
mysql>    SELECT DISTINCT pm.player_name,pm.jersey_no 
    ->      FROM player_mast pm 
    ->INNER JOIN match_details md
    ->        ON pm.player_id = md.player_gk
    ->       AND md.team_id = (SELECT country_id
    ->                           FROM soccer_country
    ->                          WHERE country_name ='Germany')
    ->       AND md.play_stage ='G';
+--------------+-----------+
| player_name  | jersey_no |
+--------------+-----------+
| Manuel Neuer |         1 |
+--------------+-----------+
1 row in set (0.00 sec)
