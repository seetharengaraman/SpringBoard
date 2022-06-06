/*Write a SQL query to find the match number, date, and score for matches in which no
stoppage time was added in the 1st half.*/
mysql> SELECT match_no,play_date,goal_score 
    ->   FROM match_mast 
    ->  WHERE stop1_sec =0;
+----------+-----------+------------+
| match_no | play_date | goal_score |
+----------+-----------+------------+
|        4 | 6/12/16   | 1-1        |
+----------+-----------+------------+
1 row in set (0.00 sec)