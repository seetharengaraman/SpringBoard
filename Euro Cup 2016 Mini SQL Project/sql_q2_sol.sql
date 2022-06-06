/*Write a SQL query to find the number of matches that were won by penalty shootout*/
mysql> SELECT COUNT(*) AS match_won_by_penalty_shootout 
    ->   FROM match_mast 
    ->  WHERE decided_by ='P';
+-------------------------------+
| match_won_by_penalty_shootout |
+-------------------------------+
|                             3 |
+-------------------------------+
1 row in set (0.00 sec)