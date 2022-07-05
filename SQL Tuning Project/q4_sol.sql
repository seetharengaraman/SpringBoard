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

-- 4. List the names of students who have taken a course taught by professor v5 (name).
EXPLAIN ANALYZE SELECT name FROM Student,
	(SELECT studId FROM Transcript,
		(SELECT crsCode, semester FROM Professor
			JOIN Teaching
			WHERE Professor.name = @v5 AND Professor.id = Teaching.profId) as alias1
	WHERE Transcript.crsCode = alias1.crsCode AND Transcript.semester = alias1.semester) as alias2
WHERE Student.id = alias2.studId;

--Without any changes, cost is high

-> Inner hash join (student.id = transcript.studId)  (cost=1313.72 rows=160) (actual time=0.266..0.266 rows=0 loops=1)
    -> Table scan on Student  (cost=0.03 rows=400) (never executed)
    -> Hash
        -> Inner hash join (professor.id = teaching.profId)  (cost=1144.90 rows=4) (actual time=0.259..0.259 rows=0 loops=1)
            -> Filter: (professor.`name` = <cache>((@v5)))  (cost=0.95 rows=4) (never executed)
                -> Table scan on Professor  (cost=0.95 rows=400) (never executed)
            -> Hash
                -> Filter: ((teaching.semester = transcript.semester) and (teaching.crsCode = transcript.crsCode))  (cost=1010.70 rows=100) (actual time=0.238..0.238 rows=0 loops=1)
                    -> Inner hash join (<hash>(teaching.semester)=<hash>(transcript.semester)), (<hash>(teaching.crsCode)=<hash>(transcript.crsCode))  (cost=1010.70 rows=100) (actual time=0.237..0.237 rows=0 loops=1)
                        -> Table scan on Teaching  (cost=0.01 rows=100) (actual time=0.005..0.066 rows=100 loops=1)
                        -> Hash
                            -> Table scan on Transcript  (cost=10.25 rows=100) (actual time=0.022..0.088 rows=100 loops=1)

--Tried Inner Join instead of subquery and cost is higher since its better to filter tables like Professor and Teaching upfront before using the result to fetch
--data from transcript and student.

EXPLAIN ANALYZE SELECT s.name 
FROM Student s 
INNER JOIN Transcript t ON s.id = t.studId
INNER JOIN Teaching te ON t.crsCode = te.crsCode AND t.semester = te.semester
INNER JOIN Professor p ON te.profId = p.id AND p.name =@v5;

-> Inner hash join (s.id = t.studId)  (cost=21113.72 rows=16000) (actual time=0.291..0.291 rows=0 loops=1)
    -> Table scan on s  (cost=0.03 rows=400) (never executed)
    -> Hash
        -> Filter: (p.`name` = <cache>((@v5)))  (cost=5104.90 rows=400) (actual time=0.256..0.256 rows=0 loops=1)
            -> Inner hash join (p.id = te.profId)  (cost=5104.90 rows=400) (actual time=0.256..0.256 rows=0 loops=1)
                -> Table scan on p  (cost=0.95 rows=400) (never executed)
                -> Hash
                    -> Filter: ((te.semester = t.semester) and (te.crsCode = t.crsCode))  (cost=1010.70 rows=100) (actual time=0.249..0.249 rows=0 loops=1)
                        -> Inner hash join (<hash>(te.semester)=<hash>(t.semester)), (<hash>(te.crsCode)=<hash>(t.crsCode))  (cost=1010.70 rows=100) (actual time=0.248..0.248 rows=0 loops=1)
                            -> Table scan on te  (cost=0.01 rows=100) (actual time=0.004..0.066 rows=100 loops=1)
                            -> Hash
                                -> Table scan on t  (cost=10.25 rows=100) (actual time=0.022..0.094 rows=100 loops=1)

--Instead of subquery, tried CTE which is similar to subquery

EXPLAIN ANALYZE WITH alias1 AS
(SELECT crsCode, semester FROM Professor
			JOIN Teaching
			WHERE Professor.name = @v5 AND Professor.id = Teaching.profId),
alias2 AS
(SELECT studId FROM transcript INNER JOIN alias1 ON Transcript.crsCode = alias1.crsCode AND Transcript.semester = alias1.semester)
SELECT name
  FROM student INNER JOIN alias2 ON student.id = alias2.studId;
  
  -> Inner hash join (student.id = transcript.studId)  (cost=1313.72 rows=160) (actual time=0.255..0.255 rows=0 loops=1)
    -> Table scan on student  (cost=0.03 rows=400) (never executed)
    -> Hash
        -> Inner hash join (professor.id = teaching.profId)  (cost=1144.90 rows=4) (actual time=0.250..0.250 rows=0 loops=1)
            -> Filter: (professor.`name` = <cache>((@v5)))  (cost=0.95 rows=4) (never executed)
                -> Table scan on Professor  (cost=0.95 rows=400) (never executed)
            -> Hash
                -> Filter: ((teaching.semester = transcript.semester) and (teaching.crsCode = transcript.crsCode))  (cost=1010.70 rows=100) (actual time=0.243..0.243 rows=0 loops=1)
                    -> Inner hash join (<hash>(teaching.semester)=<hash>(transcript.semester)), (<hash>(teaching.crsCode)=<hash>(transcript.crsCode))  (cost=1010.70 rows=100) (actual time=0.242..0.242 rows=0 loops=1)
                        -> Table scan on Teaching  (cost=0.01 rows=100) (actual time=0.004..0.069 rows=100 loops=1)
                        -> Hash
                            -> Table scan on transcript  (cost=10.25 rows=100) (actual time=0.022..0.090 rows=100 loops=1)



--Tried adding some index to help improve performance. All performed well and got significantly better cost except index on Transcript.

CREATE UNIQUE INDEX STUDENT_ID_IDX ON Student(Id);
CREATE UNIQUE INDEX PROFESSOR_ID_IDX ON Professor(Id);
CREATE UNIQUE INDEX COURSE_SEMESTER_IDX ON Teaching(crsCode,semester);
CREATE UNIQUE INDEX STUD_COURSE_SEM_IDX ON Transcript(studId,crsCode,semester);


-> Nested loop inner join  (cost=115.25 rows=10) (actual time=0.434..0.434 rows=0 loops=1)
    -> Nested loop inner join  (cost=80.25 rows=100) (actual time=0.434..0.434 rows=0 loops=1)
        -> Nested loop inner join  (cost=45.25 rows=100) (actual time=0.434..0.434 rows=0 loops=1)
            -> Filter: ((transcript.crsCode is not null) and (transcript.semester is not null) and (transcript.studId is not null))  (cost=10.25 rows=100) (actual time=0.023..0.117 rows=100 loops=1)
                -> Covering index scan on transcript using STUD_COURSE_SEM_IDX  (cost=10.25 rows=100) (actual time=0.021..0.098 rows=100 loops=1)
            -> Filter: (teaching.profId is not null)  (cost=0.25 rows=1) (actual time=0.003..0.003 rows=0 loops=100)
                -> Single-row index lookup on Teaching using COURSE_SEMESTER_IDX (crsCode=transcript.crsCode, semester=transcript.semester)  (cost=0.25 rows=1) (actual time=0.003..0.003 rows=0 loops=100)
        -> Single-row index lookup on student using STUDENT_ID_IDX (id=transcript.studId)  (cost=0.25 rows=1) (never executed)
    -> Filter: (professor.`name` = <cache>((@v5)))  (cost=0.25 rows=0) (never executed)
        -> Single-row index lookup on Professor using PROFESSOR_ID_IDX (id=teaching.profId)  (cost=0.25 rows=1) (never executed)

--Removed index on Transcript and there is no difference in the result.

DROP INDEX STUD_COURSE_SEM_IDX ON Transcript;
-> Nested loop inner join  (cost=115.25 rows=10) (actual time=0.767..0.767 rows=0 loops=1)
    -> Nested loop inner join  (cost=80.25 rows=100) (actual time=0.766..0.766 rows=0 loops=1)
        -> Nested loop inner join  (cost=45.25 rows=100) (actual time=0.766..0.766 rows=0 loops=1)
            -> Filter: ((transcript.crsCode is not null) and (transcript.semester is not null) and (transcript.studId is not null))  (cost=10.25 rows=100) (actual time=0.036..0.216 rows=100 loops=1)
                -> Table scan on transcript  (cost=10.25 rows=100) (actual time=0.033..0.182 rows=100 loops=1)
            -> Filter: (teaching.profId is not null)  (cost=0.25 rows=1) (actual time=0.005..0.005 rows=0 loops=100)
                -> Single-row index lookup on Teaching using COURSE_SEMESTER_IDX (crsCode=transcript.crsCode, semester=transcript.semester)  (cost=0.25 rows=1) (actual time=0.005..0.005 rows=0 loops=100)
        -> Single-row index lookup on student using STUDENT_ID_IDX (id=transcript.studId)  (cost=0.25 rows=1) (never executed)
    -> Filter: (professor.`name` = <cache>((@v5)))  (cost=0.25 rows=0) (never executed)
        -> Single-row index lookup on Professor using PROFESSOR_ID_IDX (id=teaching.profId)  (cost=0.25 rows=1) (never executed)


