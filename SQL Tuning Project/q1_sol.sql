
-- -------------------------------------
SET @v1 = 1612521;
SET @v2 = 1145072;
SET @v3 = 1828467;
SET @v4 = 'MGT382';
SET @v5 = 'Amber Hill';
SET @v6 = 'MGT';
SET @v7 = 'EE';			  
SET @v8 = 'MAT';

-- 1. List the name of the student with id equal to v1 (id).
EXPLAIN ANALYZE SELECT name FROM Student WHERE id = @v1;

--Query cost before optimization:

-> Filter: (student.id = <cache>((@v1)))  (cost=41.00 rows=40) (actual time=0.088..0.317 rows=1 loops=1)
    -> Table scan on Student  (cost=41.00 rows=400) (actual time=0.021..0.268 rows=400 loops=1)
    
--Add primary key constraint
ALTER TABLE STUDENT ADD CONSTRAINT PRIMARY KEY(id);


--Query cost after optimization    
    -> Rows fetched before execution  (cost=0.00..0.00 rows=1) (actual time=0.000..0.000 rows=1 loops=1)

