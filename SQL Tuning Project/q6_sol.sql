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

-- 6. List the names of students who have taken all courses offered by department v8 (deptId).
EXPLAIN ANALYZE SELECT name FROM Student,
	(SELECT studId
	FROM Transcript
		WHERE crsCode IN
		(SELECT crsCode FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching))
		GROUP BY studId
		HAVING COUNT(*) = 
			(SELECT COUNT(*) FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching))) as alias
WHERE id = alias.studId;

--Without any modifications, cost is high

-> Nested loop inner join  (cost=1041.00 rows=0) (actual time=6.244..6.244 rows=0 loops=1)
    -> Filter: (student.id is not null)  (cost=41.00 rows=400) (actual time=0.024..0.403 rows=400 loops=1)
        -> Table scan on Student  (cost=41.00 rows=400) (actual time=0.023..0.351 rows=400 loops=1)
    -> Covering index lookup on alias using <auto_key0> (studId=student.id)  (actual time=0.000..0.000 rows=0 loops=400)
        -> Materialize  (cost=0.00..0.00 rows=0) (actual time=5.742..5.742 rows=0 loops=1)
            -> Filter: (count(0) = (select #5))  (actual time=5.531..5.531 rows=0 loops=1)
                -> Table scan on <temporary>  (actual time=0.001..0.002 rows=19 loops=1)
                    -> Aggregate using temporary table  (actual time=5.524..5.528 rows=19 loops=1)
                        -> Nested loop inner join  (cost=1020.25 rows=10000) (actual time=0.244..0.485 rows=19 loops=1)
                            -> Filter: (transcript.crsCode is not null)  (cost=10.25 rows=100) (actual time=0.004..0.106 rows=100 loops=1)
                                -> Table scan on Transcript  (cost=10.25 rows=100) (actual time=0.004..0.090 rows=100 loops=1)
                            -> Single-row index lookup on <subquery3> using <auto_distinct_key> (crsCode=transcript.crsCode)  (actual time=0.001..0.001 rows=0 loops=100)
                                -> Materialize with deduplication  (cost=120.52..120.52 rows=100) (actual time=0.351..0.355 rows=19 loops=1)
                                    -> Filter: (course.crsCode is not null)  (cost=110.52 rows=100) (actual time=0.127..0.219 rows=19 loops=1)
                                        -> Filter: (teaching.crsCode = course.crsCode)  (cost=110.52 rows=100) (actual time=0.127..0.216 rows=19 loops=1)
                                            -> Inner hash join (<hash>(teaching.crsCode)=<hash>(course.crsCode))  (cost=110.52 rows=100) (actual time=0.126..0.209 rows=19 loops=1)
                                                -> Table scan on Teaching  (cost=0.13 rows=100) (actual time=0.004..0.064 rows=100 loops=1)
                                                -> Hash
                                                    -> Filter: (course.deptId = <cache>((@v8)))  (cost=10.25 rows=10) (actual time=0.012..0.101 rows=19 loops=1)
                                                        -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.004..0.073 rows=100 loops=1)
                -> Select #5 (subquery in condition; uncacheable)
                    -> Aggregate: count(0)  (cost=211.25 rows=1000) (actual time=0.259..0.259 rows=1 loops=19)
                        -> Nested loop inner join  (cost=111.25 rows=1000) (actual time=0.134..0.255 rows=19 loops=19)
                            -> Filter: ((course.deptId = <cache>((@v8))) and (course.crsCode is not null))  (cost=10.25 rows=10) (actual time=0.006..0.102 rows=19 loops=19)
                                -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.004..0.075 rows=100 loops=19)
                            -> Single-row index lookup on <subquery6> using <auto_distinct_key> (crsCode=course.crsCode)  (actual time=0.001..0.001 rows=1 loops=361)
                                -> Materialize with deduplication  (cost=20.25..20.25 rows=100) (actual time=0.141..0.144 rows=97 loops=19)
                                    -> Filter: (teaching.crsCode is not null)  (cost=10.25 rows=100) (actual time=0.002..0.081 rows=100 loops=19)
                                        -> Table scan on Teaching  (cost=10.25 rows=100) (actual time=0.002..0.066 rows=100 loops=19)
            -> Select #5 (subquery in projection; uncacheable)
                -> Aggregate: count(0)  (cost=211.25 rows=1000) (actual time=0.259..0.259 rows=1 loops=19)
                    -> Nested loop inner join  (cost=111.25 rows=1000) (actual time=0.134..0.255 rows=19 loops=19)
                        -> Filter: ((course.deptId = <cache>((@v8))) and (course.crsCode is not null))  (cost=10.25 rows=10) (actual time=0.006..0.102 rows=19 loops=19)
                            -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.004..0.075 rows=100 loops=19)
                        -> Single-row index lookup on <subquery6> using <auto_distinct_key> (crsCode=course.crsCode)  (actual time=0.001..0.001 rows=1 loops=361)
                            -> Materialize with deduplication  (cost=20.25..20.25 rows=100) (actual time=0.141..0.144 rows=97 loops=19)
                                -> Filter: (teaching.crsCode is not null)  (cost=10.25 rows=100) (actual time=0.002..0.081 rows=100 loops=19)
                                    -> Table scan on Teaching  (cost=10.25 rows=100) (actual time=0.002..0.066 rows=100 loops=19)


--Making it as a CTE significantly improves performance
EXPLAIN ANALYZE WITH courses AS
(SELECT crsCode FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching)),
course_count AS
(SELECT count(crsCode) ccount FROM Course WHERE deptId = @v8 AND crsCode IN (SELECT crsCode FROM Teaching)),
studentIds AS
(SELECT count(*) ccount,studId
   FROM Transcript t INNER JOIN courses ON t.crsCode =courses.crsCode
   GROUP BY studId)
SELECT name FROM student INNER JOIN studentIds ON student.id = studentIds.studId
INNER JOIN course_count ON course_count.ccount = studentIds.ccount;

-> Inner hash join (student.id = studentids.studId)  (cost=1.00 rows=0) (actual time=0.427..0.427 rows=0 loops=1)
    -> Table scan on student  (cost=0.50 rows=400) (never executed)
    -> Hash
        -> Index lookup on studentIds using <auto_key2> (ccount='19')  (actual time=0.002..0.002 rows=0 loops=1)
            -> Materialize CTE studentids  (cost=0.00..0.00 rows=0) (actual time=0.421..0.421 rows=19 loops=1)
                -> Table scan on <temporary>  (actual time=0.000..0.002 rows=19 loops=1)
                    -> Aggregate using temporary table  (actual time=0.401..0.405 rows=19 loops=1)
                        -> Filter: (t.crsCode = course.crsCode)  (cost=10111.53 rows=10000) (actual time=0.293..0.385 rows=19 loops=1)
                            -> Inner hash join (<hash>(t.crsCode)=<hash>(course.crsCode))  (cost=10111.53 rows=10000) (actual time=0.292..0.377 rows=19 loops=1)
                                -> Table scan on t  (cost=0.13 rows=100) (actual time=0.004..0.066 rows=100 loops=1)
                                -> Hash
                                    -> Nested loop inner join  (cost=111.25 rows=1000) (actual time=0.140..0.266 rows=19 loops=1)
                                        -> Filter: ((course.deptId = <cache>((@v8))) and (course.crsCode is not null))  (cost=10.25 rows=10) (actual time=0.010..0.111 rows=19 loops=1)
                                            -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.006..0.081 rows=100 loops=1)
                                        -> Single-row index lookup on <subquery4> using <auto_distinct_key> (crsCode=course.crsCode)  (actual time=0.001..0.001 rows=1 loops=19)
                                            -> Materialize with deduplication  (cost=20.25..20.25 rows=100) (actual time=0.146..0.149 rows=97 loops=1)
                                                -> Filter: (teaching.crsCode is not null)  (cost=10.25 rows=100) (actual time=0.003..0.086 rows=100 loops=1)
                                                    -> Table scan on Teaching  (cost=10.25 rows=100) (actual time=0.002..0.071 rows=100 loops=1)


--Adding index on top of CTE doesn't benefit much as number of records are very less. When records increase, these indexes might help

CREATE INDEX COURSE_DEPT_IDX ON Course(deptId,crsCode);
CREATE INDEX COURSE_IDX ON Transcript(crsCode);
CREATE UNIQUE INDEX STUDENT_ID_IDX ON Student(Id);

-> Nested loop inner join  (cost=1.33 rows=2) (actual time=0.528..0.528 rows=0 loops=1)
    -> Filter: (studentids.studId is not null)  (cost=0.35..0.67 rows=2) (actual time=0.527..0.527 rows=0 loops=1)
        -> Index lookup on studentIds using <auto_key2> (ccount='19')  (actual time=0.002..0.002 rows=0 loops=1)
            -> Materialize CTE studentids  (cost=0.00..0.00 rows=0) (actual time=0.527..0.527 rows=19 loops=1)
                -> Table scan on <temporary>  (actual time=0.001..0.003 rows=19 loops=1)
                    -> Aggregate using temporary table  (actual time=0.494..0.501 rows=19 loops=1)
                        -> Nested loop inner join  (cost=395.95 rows=1959) (actual time=0.254..0.467 rows=19 loops=1)
                            -> Nested loop inner join  (cost=195.18 rows=1900) (actual time=0.235..0.314 rows=19 loops=1)
                                -> Filter: (course.crsCode is not null)  (cost=3.28 rows=19) (actual time=0.016..0.050 rows=19 loops=1)
                                    -> Covering index lookup on Course using COURSE_DEPT_IDX (deptId=(@v8))  (cost=3.28 rows=19) (actual time=0.015..0.044 rows=19 loops=1)
                                -> Single-row index lookup on <subquery4> using <auto_distinct_key> (crsCode=course.crsCode)  (actual time=0.001..0.001 rows=1 loops=19)
                                    -> Materialize with deduplication  (cost=20.25..20.25 rows=100) (actual time=0.246..0.252 rows=97 loops=1)
                                        -> Filter: (teaching.crsCode is not null)  (cost=10.25 rows=100) (actual time=0.006..0.142 rows=100 loops=1)
                                            -> Table scan on Teaching  (cost=10.25 rows=100) (actual time=0.005..0.117 rows=100 loops=1)
                            -> Index lookup on t using COURSE_IDX (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.006..0.007 rows=1 loops=19)
    -> Single-row index lookup on student using STUDENT_ID_IDX (id=studentids.studId)  (cost=0.30 rows=1) (never executed)

--Removed unused index but will be useful when more number of rows are added to the table
DROP INDEX STUDENT_ID_IDX ON Student;

-> Inner hash join (student.id = studentids.studId)  (cost=1.00 rows=0) (actual time=0.531..0.531 rows=0 loops=1)
    -> Table scan on student  (cost=2.63 rows=400) (never executed)
    -> Hash
        -> Index lookup on studentIds using <auto_key2> (ccount='19')  (actual time=0.002..0.002 rows=0 loops=1)
            -> Materialize CTE studentids  (cost=0.00..0.00 rows=0) (actual time=0.521..0.521 rows=19 loops=1)
                -> Table scan on <temporary>  (actual time=0.001..0.003 rows=19 loops=1)
                    -> Aggregate using temporary table  (actual time=0.491..0.497 rows=19 loops=1)
                        -> Nested loop inner join  (cost=395.95 rows=1959) (actual time=0.259..0.436 rows=19 loops=1)
                            -> Nested loop inner join  (cost=195.18 rows=1900) (actual time=0.240..0.311 rows=19 loops=1)
                                -> Filter: (course.crsCode is not null)  (cost=3.28 rows=19) (actual time=0.015..0.048 rows=19 loops=1)
                                    -> Covering index lookup on Course using COURSE_DEPT_IDX (deptId=(@v8))  (cost=3.28 rows=19) (actual time=0.014..0.043 rows=19 loops=1)
                                -> Single-row index lookup on <subquery4> using <auto_distinct_key> (crsCode=course.crsCode)  (actual time=0.001..0.001 rows=1 loops=19)
                                    -> Materialize with deduplication  (cost=20.25..20.25 rows=100) (actual time=0.248..0.253 rows=97 loops=1)
                                        -> Filter: (teaching.crsCode is not null)  (cost=10.25 rows=100) (actual time=0.005..0.150 rows=100 loops=1)
                                            -> Table scan on Teaching  (cost=10.25 rows=100) (actual time=0.005..0.126 rows=100 loops=1)
                            -> Index lookup on t using COURSE_IDX (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.005..0.006 rows=1 loops=19)
