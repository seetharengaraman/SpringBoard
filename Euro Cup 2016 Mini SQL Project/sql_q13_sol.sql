/*Write a SQL query to find all the defenders who scored a goal for their teams.*/

mysql>    SELECT DISTINCT pm.player_name
    ->      FROM  player_mast pm
    ->INNER JOIN goal_details gd 
    ->        ON gd.player_id = pm.player_id 
    ->       AND gd.goal_type != 'O'
    ->INNER JOIN playing_position pp 
    ->        ON pm.posi_to_play = pp.position_id 
    ->       AND pp.position_desc = 'Defenders'
    ->  GROUP BY pm.player_name
    ->    HAVING COUNT(gd.goal_id) > 0;
+------------------------+
| player_name            |
+------------------------+
| Olivier Giroud         |
| Bogdan Stancu          |
| Vasili Berezutski      |
| Arkadiusz Milik        |
| Thomas Muller          |
| Gerard Pique           |
| Graziano Pelle         |
| Adam Szalai            |
| Nani                   |
| Antoine Griezmann      |
| Jamie Vardy            |
| Daniel Sturridge       |
| Gareth McAuley         |
| Niall McGinn           |
| Eder                   |
| Milan Skoda            |
| TomasNecid             |
| Alvaro Morata          |
| Romelu Lukaku          |
| Armando Sadiku         |
| Mario Gomez            |
| Nikola Kalinic         |
| Jon Dadi Bodvarsson    |
| Zoltan Gera            |
| Balazs Dzsudzsak       |
| Cristiano Ronaldo      |
| Ricardo Quaresma       |
| Jerome Boateng         |
| Toby Alderweireld      |
| Michy Batshuayi        |
| Giorgio Chiellini      |
| Wayne Rooney           |
| Arnor Ingvi Traustason |
| Kolbeinn Sigthorsson   |
| Robert Lewandowski     |
| Leonardo Bonucci       |
+------------------------+
36 rows in set (0.00 sec)
