USE springboardopt;

-- -------------------------------------
SET @v1 = 1612521;
SET @v2 = 1145072;
SET @v3 = 1828467;
SET @v4 = 'MGT382';
SET @v5 = 'Amber Hill';
SET @v6 = 'MGT';
SET @v7 = 'EE';			  
SET @v8 = 'MAT';

-- 2. List the names of students with id in the range of v2 (id) to v3 (inclusive).
EXPLAIN ANALYZE SELECT name FROM Student WHERE id BETWEEN @v2 AND @v3;

--Above produced below result when ID column was the primary key of the table
-> Filter: (student.id between <cache>((@v2)) and <cache>((@v3)))  (cost=56.47 rows=278) (actual time=0.030..0.305 rows=278 loops=1)
    -> Index range scan on Student using PRIMARY over (1145072 <= id <= 1828467)  (cost=56.47 rows=278) (actual time=0.027..0.230 rows=278 loops=1)

    
ALTER TABLE STUDENT DROP PRIMARY KEY;
--After dropping primary key, cost was much better and same as time taken to execute query


-> Filter: (student.id between <cache>((@v2)) and <cache>((@v3)))  (cost=5.44 rows=44) (actual time=0.026..0.294 rows=278 loops=1)
    -> Table scan on Student  (cost=5.44 rows=400) (actual time=0.024..0.239 rows=400 loops=1)

CREATE UNIQUE INDEX ID_1 ON STUDENT(ID);
--Once a unique index was added, time taken got little better but cost was worse than no index.

-> Filter: (student.id between <cache>((@v2)) and <cache>((@v3)))  (cost=56.47 rows=278) (actual time=0.018..0.189 rows=278 loops=1)
    -> Index range scan on Student using ID_1 over (1145072 <= id <= 1828467)  (cost=56.47 rows=278) (actual time=0.016..0.144 rows=278 loops=1)

--Considering above, no primary key or index seems better for this range. However, as number of records increase adding index should ideally be preferred   
    


