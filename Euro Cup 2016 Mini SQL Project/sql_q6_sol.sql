/*Write a SQL query to find the number of matches that were won by a single point, but
do not include matches decided by penalty shootout.*/
mysql> SELECT COUNT(*) AS number_of_matches_won_by_single_point
    ->   FROM match_mast
    ->  WHERE ABS(substr(goal_score,1,instr(goal_score,'-')-1)  -
    ->            substr(goal_score,instr(goal_score,'-')+1)) = 1
    ->    AND decided_by != 'P';
+---------------------------------------+
| number_of_matches_won_by_single_point |
+---------------------------------------+
|                                    21 |
+---------------------------------------+
1 row in set (0.00 sec)
