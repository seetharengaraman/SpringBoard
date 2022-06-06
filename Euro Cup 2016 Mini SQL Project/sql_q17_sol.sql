/*Write a SQL query to find the country where the most assistant referees come from,
and the count of the assistant referees.*/

mysql> WITH result AS
    ->        (SELECT COUNT(ass_ref_id) AS assistant_referee_count,country_id
    ->           FROM asst_referee_mast arm
    ->       GROUP BY country_id
    ->       ORDER BY 1 DESC
    ->          LIMIT 1)
    ->     SELECT sc.country_name,r.assistant_referee_count
    ->       FROM result r
    -> INNER JOIN soccer_country sc
    ->         ON sc.country_id = r.country_id;
+--------------+-------------------------+
| country_name | assistant_referee_count |
+--------------+-------------------------+
| England      |                       4 |
+--------------+-------------------------+
1 row in set (0.00 sec)

