/*Write a SQL query to find the players, their jersey number, and playing club who were
the goalkeepers for England in EURO Cup 2016.*/

mysql>    SELECT pm.player_name,pm.jersey_no,pm.playing_club
    ->      FROM player_mast pm
    ->INNER JOIN playing_position pp 
    ->        ON pm.posi_to_play = pp.position_id 
    ->       AND pp.position_desc = 'Goalkeepers'
    ->INNER JOIN soccer_country sc 
    ->        ON pm.team_id = sc.country_id
    ->       AND sc.country_name ='England';
+----------------+-----------+--------------+
| player_name    | jersey_no | playing_club |
+----------------+-----------+--------------+
| Joe Hart       |         1 | Man. City    |
| Fraser Forster |        13 | Southampton  |
| Tom Heaton     |        23 | Burnley      |
+----------------+-----------+--------------+
3 rows in set (0.00 sec)
