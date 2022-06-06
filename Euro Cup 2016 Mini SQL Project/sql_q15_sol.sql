/*Write a SQL query to find the referees who booked the most number of players.*/

mysql> SELECT max_booking.referee_name
    ->   FROM
    ->   (SELECT referee_name, COUNT(DISTINCT player_id) AS booking_count
    ->      FROM referee_mast rm
    ->INNER JOIN match_mast mm 
    ->        ON rm.referee_id = rm.referee_id
    ->INNER JOIN player_booked pb
    ->        ON pb.match_no = mm.match_no 
    ->       AND pb.team_id = rm.country_id
    ->  GROUP BY rm.referee_name
    ->  ORDER BY booking_count DESC) max_booking
    ->   LIMIT 1;
+----------------+
| referee_name   |
+----------------+
| Nicola Rizzoli |
+----------------+
1 row in set (0.00 sec)
