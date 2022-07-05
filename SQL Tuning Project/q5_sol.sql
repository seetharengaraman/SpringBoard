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

-- 5. List the names of students who have taken a course from department v6 (deptId), but not v7.
EXPLAIN ANALYZE SELECT * FROM Student, 
	(SELECT studId FROM Transcript, Course WHERE deptId = @v6 AND Course.crsCode = Transcript.crsCode
	AND studId NOT IN
	(SELECT studId FROM Transcript, Course WHERE deptId = @v7 AND Course.crsCode = Transcript.crsCode)) as alias
WHERE Student.id = alias.studId;

--Without any modification, below is the cost which is very high.

-> Filter: <in_optimizer>(transcript.studId,<exists>(select #3) is false)  (cost=4112.69 rows=4000) (actual time=0.888..10.290 rows=30 loops=1)
    -> Inner hash join (student.id = transcript.studId)  (cost=4112.69 rows=4000) (actual time=0.481..0.918 rows=30 loops=1)
        -> Table scan on Student  (cost=0.06 rows=400) (actual time=0.009..0.357 rows=400 loops=1)
        -> Hash
            -> Filter: (transcript.crsCode = course.crsCode)  (cost=110.52 rows=100) (actual time=0.241..0.429 rows=30 loops=1)
                -> Inner hash join (<hash>(transcript.crsCode)=<hash>(course.crsCode))  (cost=110.52 rows=100) (actual time=0.240..0.411 rows=30 loops=1)
                    -> Table scan on Transcript  (cost=0.13 rows=100) (actual time=0.006..0.134 rows=100 loops=1)
                    -> Hash
                        -> Filter: (course.deptId = <cache>((@v6)))  (cost=10.25 rows=10) (actual time=0.036..0.194 rows=26 loops=1)
                            -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.028..0.148 rows=100 loops=1)
    -> Select #3 (subquery in condition; dependent)
        -> Limit: 1 row(s)  (cost=110.52 rows=1) (actual time=0.308..0.308 rows=0 loops=30)
            -> Filter: <if>(outer_field_is_not_null, <is_not_null_test>(transcript.studId), true)  (cost=110.52 rows=100) (actual time=0.307..0.307 rows=0 loops=30)
                -> Filter: (<if>(outer_field_is_not_null, ((<cache>(transcript.studId) = transcript.studId) or (transcript.studId is null)), true) and (transcript.crsCode = course.crsCode))  (cost=110.52 rows=100) (actual time=0.307..0.307 rows=0 loops=30)
                    -> Inner hash join (<hash>(transcript.crsCode)=<hash>(course.crsCode))  (cost=110.52 rows=100) (actual time=0.167..0.298 rows=34 loops=30)
                        -> Table scan on Transcript  (cost=0.13 rows=100) (actual time=0.003..0.097 rows=100 loops=30)
                        -> Hash
                            -> Filter: (course.deptId = <cache>((@v7)))  (cost=10.25 rows=10) (actual time=0.010..0.132 rows=32 loops=30)
                                -> Table scan on Course  (cost=10.25 rows=100) (actual time=0.004..0.100 rows=100 loops=30)


--Added below indexes based on the where conditions and tables involved. All indexes used, produces better cost and better execution time

CREATE UNIQUE INDEX STUDENT_ID_IDX ON Student(Id);
CREATE INDEX COURSE_DEPT_IDX ON Course(deptId,crsCode);
CREATE INDEX COURSE_IDX ON Transcript(crsCode);

-> Nested loop inner join  (cost=23.18 rows=27) (actual time=0.426..8.800 rows=30 loops=1)
    -> Nested loop inner join  (cost=13.79 rows=27) (actual time=0.047..0.286 rows=30 loops=1)
        -> Filter: (course.crsCode is not null)  (cost=4.41 rows=26) (actual time=0.027..0.074 rows=26 loops=1)
            -> Covering index lookup on Course using COURSE_DEPT_IDX (deptId=(@v6))  (cost=4.41 rows=26) (actual time=0.025..0.063 rows=26 loops=1)
        -> Filter: (transcript.studId is not null)  (cost=0.26 rows=1) (actual time=0.005..0.008 rows=1 loops=26)
            -> Index lookup on Transcript using COURSE_IDX (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.005..0.007 rows=1 loops=26)
    -> Filter: <in_optimizer>(transcript.studId,<exists>(select #3) is false)  (cost=0.25 rows=1) (actual time=0.283..0.283 rows=1 loops=30)
        -> Single-row index lookup on Student using STUDENT_ID_IDX (id=transcript.studId)  (cost=0.25 rows=1) (actual time=0.008..0.008 rows=1 loops=30)
        -> Select #3 (subquery in condition; dependent)
            -> Limit: 1 row(s)  (cost=16.93 rows=1) (actual time=0.271..0.271 rows=0 loops=30)
                -> Filter: <if>(outer_field_is_not_null, <is_not_null_test>(transcript.studId), true)  (cost=16.93 rows=33) (actual time=0.271..0.271 rows=0 loops=30)
                    -> Nested loop inner join  (cost=16.93 rows=33) (actual time=0.271..0.271 rows=0 loops=30)
                        -> Filter: (course.crsCode is not null)  (cost=5.39 rows=32) (actual time=0.006..0.054 rows=32 loops=30)
                            -> Covering index lookup on Course using COURSE_DEPT_IDX (deptId=(@v7))  (cost=5.39 rows=32) (actual time=0.005..0.047 rows=32 loops=30)
                        -> Filter: <if>(outer_field_is_not_null, ((<cache>(transcript.studId) = transcript.studId) or (transcript.studId is null)), true)  (cost=0.26 rows=1) (actual time=0.006..0.006 rows=0 loops=960)
                            -> Index lookup on Transcript using COURSE_IDX (crsCode=course.crsCode)  (cost=0.26 rows=1) (actual time=0.004..0.006 rows=1 loops=960)



