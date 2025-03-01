CREATE TABLE department(dep_name VARCHAR(20) PRIMARY KEY, building NUMERIC, budget NUMERIC);
CREATE TABLE instructor(ID INT PRIMARY KEY, name VARCHAR(20), salary NUMERIC, dep_name VARCHAR(20) REFERENCES department);
CREATE TABLE classroom(building INT, room_number INT, capacity NUMERIC, PRIMARY KEY(building, room_number), UNIQUE(building), UNIQUE(room_number));
CREATE TABLE time_slot(id INT PRIMARY KEY, day VARCHAR(10), start time, end_time time);
CREATE TABLE student(id INT PRIMARY KEY, name VARCHAR(20), tot_credit INT, department VARCHAR(20) REFERENCES department, advisor_id INT REFERENCES instructor);
CREATE TABLE course(course_id INT PRIMARY KEY, title VARCHAR(20), credits INT, department VARCHAR(20) REFERENCES department);
CREATE TABLE pre_requiste(course_id INT, prereq_id INT,PRIMARY KEY(course_id, prereq_id));
CREATE TABLE section(section_id INT PRIMARY KEY, semester INT, year INT, instructor_id INT REFERENCES instructor, course_id INT REFERENCES course,classroom_building INT REFERENCES classroom(building), classroom_room_no INT REFERENCES classroom(room_number));
CREATE TABLE takes(student_id INT REFERENCES student, section_id INT REFERENCES section, grade real, PRIMARY KEY(student_id, section_id));
CREATE TABLE section_time(time_slot INT REFERENCES time_slot, section_id INT REFERENCES section, PRIMARY KEY(time_slot, section_id));

CREATE SEQUENCE id_sequence START 1;