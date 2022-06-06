/*Write a SQL query to compute a list showing the number of substitutions that
happened in various stages of play for the entire tournament.*/
mysql>    SELECT COUNT(pio.player_id) AS player_count,(CASE WHEN mm.play_stage = 'G' THEN 'Group'
    -> 								   	    WHEN mm.play_stage = 'R' THEN 'Round of 16'
    ->                                      WHEN mm.play_stage = 'Q' THEN 'Quarter Final'
    ->                                      WHEN mm.play_stage = 'S' THEN 'Semi Final'
    ->                                      WHEN mm.play_stage = 'F' THEN 'Finals'
    ->                                  END) AS play_stage
    ->      FROM  match_mast mm 
    ->INNER JOIN player_in_out pio 
    ->        ON mm.match_no = pio.match_no 
    ->       AND pio.in_out ='I'
    ->  GROUP BY mm.play_stage;
+----------------------+---------------+
| player_count         | play_stage    |
+----------------------+---------------+
|                  208 | Group         |
|                   45 | Round of 16   |
|                   22 | Quarter Final |
|                   12 | Semi Final    |
|                    6 | Finals        |
+----------------------+---------------+
5 rows in set (0.00 sec)
