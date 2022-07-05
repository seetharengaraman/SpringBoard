SET @v1 = 1612521;
SET @v2 = 1145072;
SET @v3 = 1828467;
SET @v4 = 'MGT382';
SET @v5 = 'Amber Hill';
SET @v6 = 'MGT';
SET @v7 = 'EE';			  
SET @v8 = 'MAT';

-- 3. List the names of students who have taken course v4 (crsCode).
EXPLAIN ANALYZE SELECT name FROM Student WHERE id IN (SELECT studId FROM Transcript WHERE crsCode = @v4);

--Cost with only primary key on Student.Id is below. 

-> Nested loop inner join  (cost=3.63 rows=10) (actual time=0.188..0.193 rows=2 loops=1)
    -> Filter: (`<subquery2>`.studId is not null)  (cost=10.33..2.00 rows=10) (actual time=0.148..0.149 rows=2 loops=1)
        -> Table scan on <subquery2>  (cost=0.26..2.62 rows=10) (actual time=0.001..0.001 rows=2 loops=1)
            -> Materialize with deduplication  (cost=11.51..13.88 rows=10) (actual time=0.121..0.121 rows=2 loops=1)
                -> Filter: (transcript.studId is not null)  (cost=10.25 rows=10) (actual time=0.049..0.114 rows=2 loops=1)
                    -> Filter: (transcript.crsCode = <cache>((@v4)))  (cost=10.25 rows=10) (actual time=0.049..0.113 rows=2 loops=1)
                        -> Table scan on Transcript  (cost=10.25 rows=100) (actual time=0.019..0.086 rows=100 loops=1)
    -> Single-row index lookup on Student using ID_1 (id=`<subquery2>`.studId)  (cost=0.72 rows=1) (actual time=0.007..0.007 rows=1 loops=2)
    
EXPLAIN ANALYZE SELECT name FROM Student INNER JOIN Transcript ON Student.id = Transcript.studId AND crsCode = @v4;

--Changed query to inner join and saw below result with higher cost.

    -> Nested loop inner join  (cost=17.50 rows=10) (actual time=0.068..0.147 rows=2 loops=1)
    -> Filter: ((transcript.crsCode = <cache>((@v4))) and (transcript.studId is not null))  (cost=10.25 rows=10) (actual time=0.053..0.127 rows=2 loops=1)
        -> Table scan on Transcript  (cost=10.25 rows=100) (actual time=0.020..0.095 rows=100 loops=1)
    -> Single-row index lookup on Student using ID_1 (id=transcript.studId)  (cost=0.63 rows=1) (actual time=0.009..0.009 rows=1 loops=2)


CREATE INDEX ID_2 ON Transcript(crsCode);
EXPLAIN ANALYZE SELECT name FROM Student INNER JOIN Transcript ON Student.id = Transcript.studId AND crsCode = @v4;


--After creating index on crsCode for Transcript, cost is significantly reduced for the inner join query

-> Nested loop inner join  (cost=2.15 rows=2) (actual time=0.057..0.074 rows=2 loops=1)
    -> Filter: (transcript.studId is not null)  (cost=0.70 rows=2) (actual time=0.032..0.043 rows=2 loops=1)
        -> Index lookup on Transcript using ID_2 (crsCode=(@v4))  (cost=0.70 rows=2) (actual time=0.031..0.041 rows=2 loops=1)
    -> Single-row index lookup on Student using ID_1 (id=transcript.studId)  (cost=0.68 rows=1) (actual time=0.013..0.014 rows=1 loops=2)
