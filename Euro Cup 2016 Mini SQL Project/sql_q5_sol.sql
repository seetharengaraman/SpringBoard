/*Write a SQL query to find the number of bookings that happened in stoppage time*/
mysql> SELECT COUNT(*) AS player_booked_at_stop_time 
    ->   FROM player_booked 
    ->  WHERE play_schedule = 'ST';
+----------------------------+
| player_booked_at_stop_time |
+----------------------------+
|                         10 |
+----------------------------+
1 row in set (0.00 sec)
