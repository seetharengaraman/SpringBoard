/*Write a SQL query to find all available information about the players under contract to
Liverpool F.C. playing for England in EURO Cup 2016.*/
mysql>    SELECT pm.* 
    ->      FROM player_mast pm
    ->INNER JOIN player_booked pb 
    ->        ON pm.player_id = pb.player_id 
    ->       AND pm.team_id = pb.team_id
    ->       AND pb.team_id = 
    ->                     (SELECT country_id 
    ->                        FROM soccer_country 
    ->                       WHERE country_name ='England')
    ->     WHERE pm.playing_club LIKE 'Liverpool%';
+-----------+---------+-----------+------------------+--------------+------------+------+--------------+
| player_id | team_id | jersey_no | player_name      | posi_to_play | dt_of_bir  | age  | playing_club |
+-----------+---------+-----------+------------------+--------------+------------+------+--------------+
|    160137 |    1206 |        15 | Daniel Sturridge | FD           | 1989-09-01 |   26 | Liverpool    |
+-----------+---------+-----------+------------------+--------------+------------+------+--------------+
1 row in set (0.01 sec)
