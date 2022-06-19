/*Write a SQL query to find referees and the number of bookings they made for the
entire tournament. Sort your answer by the number of bookings in descending order.*/

mysql>    SELECT rm.referee_name, COUNT(player_id) AS booking_count
    ->      FROM referee_mast rm
    ->INNER JOIN match_mast mm 
    ->        ON rm.referee_id = mm.referee_id
    ->INNER JOIN player_booked pb
    ->        ON pb.match_no = mm.match_no 
    ->  GROUP BY rm.referee_name
    ->  ORDER BY booking_count DESC;
+-------------------------+---------------+
| referee_name            | booking_count |
+-------------------------+---------------+
| Mark Clattenburg        |            21 |
| Nicola Rizzoli          |            20 |
| Milorad Mazic           |            13 |
| Viktor Kassai           |            12 |
| Damir Skomina           |            12 |
| Sergei Karasev          |            12 |
| Bjorn Kuipers           |            12 |
| Jonas Eriksson          |            11 |
| Cuneyt Cakir            |            11 |
| Pavel Kralovec          |            11 |
| Carlos Velasco Carballo |            10 |
| Szymon Marciniak        |            10 |
| Ovidiu Hategan          |             9 |
| Martin Atkinson         |             9 |
| Felix Brych             |             9 |
| Svein Oddvar Moen       |             8 |
| William Collum          |             8 |
| Clement Turpin          |             3 |
+-------------------------+---------------+
18 rows in set (0.00 sec)
