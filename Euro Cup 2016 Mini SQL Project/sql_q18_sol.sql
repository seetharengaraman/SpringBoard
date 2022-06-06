/*Write a SQL query to find the highest number of foul cards given in one match*/

mysql>   SELECT COUNT(player_id) AS foul_card_player_count_per_match
    ->     FROM player_booked 
    ->    WHERE sent_off ='Y'
    -> GROUP BY match_no
    -> ORDER BY 1 DESC
    ->    LIMIT 1;
+----------------------------------+
| foul_card_player_count_per_match |
+----------------------------------+
|                                1 |
+----------------------------------+
1 row in set (0.00 sec)

