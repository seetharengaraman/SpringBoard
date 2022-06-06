/*Write a SQL query to find the date EURO Cup 2016 started on*/
mysql> SELECT MIN(play_date) AS euro_cup_start_date 
    ->   FROM match_mast;
+---------------------+
| euro_cup_start_date |
+---------------------+
| 2016-06-11          |
+---------------------+
1 row in set (0.00 sec)
