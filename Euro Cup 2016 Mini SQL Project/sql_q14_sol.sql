/*Write a SQL query to find referees and the number of bookings they made for the
entire tournament. Sort your answer by the number of bookings in descending order.*/

mysql>    SELECT rm.referee_name, COUNT(player_id) AS booking_count
    ->      FROM referee_mast rm
    ->INNER JOIN match_mast mm 
    ->        ON rm.referee_id = rm.referee_id
    ->INNER JOIN player_booked pb
    ->        ON pb.match_no = mm.match_no 
    ->       AND pb.team_id = rm.country_id
    ->  GROUP BY rm.referee_name
    ->  ORDER BY booking_count DESC;
+-------------------------+---------------+
| referee_name            | booking_count |
+-------------------------+---------------+
| Nicola Rizzoli          |            16 |
| Clement Turpin          |            13 |
| Viktor Kassai           |            12 |
| Felix Brych             |            11 |
| Ovidiu Hategan          |            10 |
| Szymon Marciniak        |            10 |
| Cuneyt Cakir            |             7 |
| Pavel Kralovec          |             5 |
| Carlos Velasco Carballo |             5 |
| Martin Atkinson         |             3 |
| Mark Clattenburg        |             3 |
| Jonas Eriksson          |             3 |
| Sergei Karasev          |             2 |
+-------------------------+---------------+
13 rows in set (0.00 sec)
