CREATE EXTENSION btree_gin;
CREATE STATISTICS student_stats ON id, department FROM student;
CREATE STATISTICS section_stats ON section_id, semester, year FROM section;
CREATE STATISTICS takes_stats ON student_id, section_id FROM takes;