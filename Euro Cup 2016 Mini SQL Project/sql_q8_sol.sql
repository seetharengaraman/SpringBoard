/*Write a SQL query to find the match number for the game with the highest number of
penalty shots, and which countries played that match.*/
mysql>    SELECT ps.match_no, sc.country_name
    ->      FROM penalty_shootout ps 
    ->INNER JOIN soccer_country sc 
    ->        ON sc.country_id = ps.team_id
    ->  GROUP BY ps.match_no, sc.country_name
    ->    HAVING COUNT(ps.kick_id) = 
    ->                   (SELECT MAX(pc.penalty_kick_count) AS kick_count
    ->                      FROM
    ->                           (SELECT COUNT(*) AS penalty_kick_count,ps.team_id,ps.match_no
    ->                              FROM penalty_shootout ps
    ->                          GROUP BY ps.team_id,ps.match_no) pc);
+----------+--------------+
| match_no | country_name |
+----------+--------------+
|       47 | Italy        |
|       47 | Germany      |
+----------+--------------+
2 rows in set (0.01 sec)
