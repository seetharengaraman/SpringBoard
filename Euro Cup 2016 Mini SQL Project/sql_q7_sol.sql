/*Write a SQL query to find all the venues where matches with penalty shootouts were
played.*/
mysql>    SELECT DISTINCT sv.venue_name
    ->      FROM soccer_venue sv
    ->INNER JOIN match_mast mm
    ->        ON sv.venue_id = mm.venue_id
    ->INNER JOIN penalty_shootout ps
    ->        ON ps.match_no = mm.match_no;
+-------------------------+
| venue_name              |
+-------------------------+
| Stade Geoffroy Guichard |
| Stade VElodrome         |
| Stade de Bordeaux       |
+-------------------------+
3 rows in set (0.00 sec)
