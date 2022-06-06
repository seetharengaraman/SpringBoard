/*Write a SQL query to find the substitute players who came into the field in the first
half of play, within a normal play schedule.*/

mysql>    SELECT DISTINCT pm.player_name
    ->      FROM player_mast pm
    ->INNER JOIN player_in_out pio
    ->        ON pm.player_id = pio.player_id
    ->       AND pm.team_id = pio.team_id
    ->       AND pio.play_schedule ='NT'
    ->       AND pio.in_out ='I'
    ->       AND play_half = 1;
+------------------------+
| player_name            |
+------------------------+
| Bastian Schweinsteiger |
| Ricardo Quaresma       |
+------------------------+
2 rows in set (0.00 sec)

