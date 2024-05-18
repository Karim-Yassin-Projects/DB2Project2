import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;

public class Schema1 {

    private static final int nBuildings = 10;
    private static final int nDepartments = 60;
    private static final int nClasses = 400;
    private static final int minInstructorsPerDept = 10;
    private static final int maxInstructorsPerDept = 35;
    private static final int minClassCapacity = 50;
    private static final int maxClassCapacity = 100;
    private static final int nTimeslots = 10000;
    private static final int minStudentsPerDept = 1100;
    private static final int maxStudentsPerDept = 3000;
    private static final int minCoursesPerDept = 20;
    private static final int maxCoursesPerDept = 45;
    private static final int minYear = 2017;
    private static final int maxYear = 2024;

    private static final int INSERT_DEPARTMENT = 0;
    private static final int INSERT_INSTRUCTOR = 1;
    private static final int INSERT_CLASSROOM = 2;
    private static final int INSERT_TIMESLOT = 3;
    private static final int INSERT_STUDENT = 4;
    private static final int INSERT_COURSE = 5;
    private static final int INSERT_PREREQUISITE = 6;
    private static final int INSERT_SECTION = 7;
    private static final int INSERT_TAKES = 8;
    private static final int INSERT_SECTION_TIME = 9;

    private final PreparedStatement[] preparedStatements = new PreparedStatement[10];

    private Schema1(Connection conn) throws SQLException {
        preparedStatements[INSERT_DEPARTMENT] = conn.prepareStatement("INSERT INTO department(dep_name,building,budget) VALUES(?,?,?);");
        preparedStatements[INSERT_INSTRUCTOR] = conn.prepareStatement("INSERT INTO instructor(ID,name,salary,dep_name) VALUES(nextval('id_sequence'),?,?,?);", Statement.RETURN_GENERATED_KEYS);
        preparedStatements[INSERT_CLASSROOM] = conn.prepareStatement("INSERT INTO classroom(building,room_number,capacity) VALUES(?,?,?);");
        preparedStatements[INSERT_TIMESLOT] = conn.prepareStatement("INSERT INTO time_slot(id,day,start,end_time) VALUES(?,?,?,?);");
        preparedStatements[INSERT_STUDENT] = conn.prepareStatement("INSERT INTO student(id,name,tot_credit,department,advisor_id) VALUES(nextval('id_sequence'),?,?,?,?);", Statement.RETURN_GENERATED_KEYS);
        preparedStatements[INSERT_COURSE] = conn.prepareStatement("INSERT INTO course(course_id,title,credits,department) VALUES(nextval('id_sequence'),?,?,?);", Statement.RETURN_GENERATED_KEYS);
        preparedStatements[INSERT_PREREQUISITE] = conn.prepareStatement("INSERT INTO pre_requiste(course_id,prereq_id) VALUES(?,?);");
        preparedStatements[INSERT_SECTION] = conn.prepareStatement("INSERT INTO section(section_id,semester,year,instructor_id,course_id,classroom_building,classroom_room_no) VALUES(nextval('id_sequence'),?,?,?,?,?,?);", Statement.RETURN_GENERATED_KEYS);
        preparedStatements[INSERT_TAKES] = conn.prepareStatement("INSERT INTO takes(student_id,section_id,grade) VALUES(?,?,?);");
        preparedStatements[INSERT_SECTION_TIME] = conn.prepareStatement("INSERT INTO section_time(time_slot,section_id) VALUES(?,?);");
    }


    // //////////////////////////////////////////// Table Insertion Methods
    // ///////////////////////////////////////////////////////////////
    private void insertDepartment(int building, String deptName, int budget) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_DEPARTMENT];

        pstmt.setString(1, deptName);
        pstmt.setInt(2, building);
        pstmt.setInt(3, budget);

        pstmt.executeUpdate();
    }

    private int insertInstructor(String name, int salary, String deptName) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_INSTRUCTOR];

        pstmt.setString(1, name);
        pstmt.setInt(2, salary);
        pstmt.setString(3, deptName);

        return AllSchemas.getGeneratedId(pstmt);
    }

    private void insertClassroom(int building, int roomNo, int capacity) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_CLASSROOM];

        pstmt.setInt(1, building);
        pstmt.setInt(2, roomNo);
        pstmt.setInt(3, capacity);

        pstmt.executeUpdate();
    }

    private void insertTimeSlot(int ID, String day, Time start, Time end) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_TIMESLOT];

        pstmt.setInt(1, ID);
        pstmt.setString(2, day);
        pstmt.setTime(3, start);
        pstmt.setTime(4, end);

        pstmt.executeUpdate();
    }

    private int insertStudent(String name, int credit,
                              String deptName, int advisorId) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_STUDENT];

        pstmt.setString(1, name);
        pstmt.setInt(2, credit);
        pstmt.setString(3, deptName);
        pstmt.setInt(4, advisorId);

        return AllSchemas.getGeneratedId(pstmt);
    }

    // CREATE TABLE course(course_id INT PRIMARY KEY, title VARCHAR(20), credits
    // INT, department VARCHAR(20) REFERENCES department);
    private int insertCourse(String title, int credit, String deptName) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_COURSE];

        pstmt.setString(1, title);
        pstmt.setInt(2, credit);
        pstmt.setString(3, deptName);
        return AllSchemas.getGeneratedId(pstmt);
    }

    // CREATE TABLE pre_requiste(course_id INT, prereq_id INT,PRIMARY
    // KEY(course_id, prereq_id));
    private void insertPrerequisite(int ID, int preID) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_PREREQUISITE];

        pstmt.setInt(2, preID);
        pstmt.setInt(1, ID);
        pstmt.executeUpdate();
    }

    // CREATE TABLE section(section_id INT PRIMARY KEY, semester INT, year INT,
    // instructor_id INT REFERENCES instructor, course_id INT REFERENCES
    // course,classroom_building INT REFERENCES classroom(building),
    // classroom_room_no INT REFERENCES classroom(room_number));

    private int insertSection(int semester, int year,
                              int instID, int courseID, int classroomBuilding,
                              int classroomRoomNo) throws SQLException {

        PreparedStatement pstmt = preparedStatements[INSERT_SECTION];

        pstmt.setInt(1, semester);
        pstmt.setInt(2, year);
        pstmt.setInt(3, instID);
        pstmt.setInt(4, courseID);
        pstmt.setInt(5, classroomBuilding);
        pstmt.setInt(6, classroomRoomNo);
        return AllSchemas.getGeneratedId(pstmt);
    }

    // CREATE TABLE takes(student_id INT REFERENCES student, section_id INT
    // REFERENCES section, grade real, PRIMARY KEY(student_id, section_id));
    private void insertTakes(int student_id, int secID, double grade
    ) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_TAKES];

        pstmt.setInt(2, secID);
        pstmt.setInt(1, student_id);
        pstmt.setDouble(3, grade);

        try {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            // The Postgresql 23505 UNIQUE VIOLATION error
            if (!e.getSQLState().equals("23505")) {
                throw e;
            }
        }
    }

    // CREATE TABLE section_time(time_slot INT REFERENCES time_slot, section_id
    // INT REFERENCES section, PRIMARY KEY(time_slot, section_id));
    private void insertSectionTime(int timeSlot, int secID) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_SECTION_TIME];
        pstmt.setInt(2, secID);
        pstmt.setInt(1, timeSlot);
        pstmt.executeUpdate();
    }

    // ///////////////////////////////////////// Data Population Method
    // //////////////////////////////////////////////////////
    private void populateDepartment() throws SQLException {
        System.out.println("populating departments");
        for (int i = 0; i < nDepartments; i++) {
            char c1 = (char) ('E' + i / 20);
            char c2 = (char) ('A' + i % 20);
            String deptName = "CS" + c1 + c2;

            insertDepartment(AllSchemas.random(1, nBuildings + 1), deptName, i);
            ArrayList<Integer> instructorIds = populateInstructor(deptName);
            ArrayList<Integer> courseIds = populateCourse(deptName);
            ArrayList<Integer> sectionIds = populateSection(instructorIds, courseIds);
            ArrayList<Integer> studentIds = populateStudent(deptName, instructorIds);
            populatePrerequisite(courseIds);
            populateTakes(studentIds, sectionIds);
            populateSectionTime(sectionIds);
        }
    }

    private ArrayList<Integer> populateInstructor(String deptName) throws SQLException {
        System.out.println("populating instructors for dept " + deptName);
        int numberOfInstructors = AllSchemas.random(minInstructorsPerDept, maxInstructorsPerDept + 1);
        ArrayList<Integer> result = new ArrayList<>(numberOfInstructors);
        for (int i = 0; i < numberOfInstructors; i++) {
            int insId = insertInstructor("Name" + i, i, deptName);
            result.add(insId);
        }
        return result;
    }

    private void populateClassroom() throws SQLException {
        System.out.println("populating classrooms ");
        for (int i = 1; i <= nClasses; i++) {
            int cap = AllSchemas.random(minClassCapacity, maxClassCapacity);
            insertClassroom(i, i, cap);
        }
    }

    private void populateTimeSlot() throws SQLException {
        System.out.println("populating timeslots");
        for (int i = 1; i <= nTimeslots; i++) {
            Time[] range = AllSchemas.randomTimeRange();
            insertTimeSlot(i, "day" + i, range[0], range[1]);
        }
    }

    private ArrayList<Integer> populateStudent(String deptName, ArrayList<Integer> instructors)
            throws SQLException {
        System.out.println("populating students dept " + deptName);

        int nStudents = AllSchemas.random(minStudentsPerDept, maxStudentsPerDept + 1);
        ArrayList<Integer> result = new ArrayList<>();
        for (int i = 0; i < nStudents; i++) {
            int advisorId = AllSchemas.randomElement(instructors);
            int studentId = insertStudent("Student " + deptName + " " + i, i, deptName, advisorId);
            result.add(studentId);
        }
        return result;
    }

    private ArrayList<Integer> populateCourse(String deptName) throws SQLException {
        System.out.println("populating courses dept " + deptName);

        int numberOfCourses = AllSchemas.random(minCoursesPerDept, maxCoursesPerDept + 1);
        ArrayList<Integer> result = new ArrayList<>(numberOfCourses);
        for (int i = 0; i < numberOfCourses; i++) {
            int credit = AllSchemas.random(1, 9);
            int courseID = insertCourse(String.format("%s%02d", deptName, i + 1), credit, deptName);
            result.add(courseID);
        }
        return result;
    }

    private void populatePrerequisite(ArrayList<Integer> courseIds) throws SQLException {
        System.out.println("populating pre_requisites");
        for (int i = 0; i < courseIds.size() / 2 - 1; i++) {
            int preCourseId = courseIds.get(i);
            int courseId = courseIds.get(i + courseIds.size() / 2);
            insertPrerequisite(courseId, preCourseId);
        }
    }

    private ArrayList<Integer> populateSection(ArrayList<Integer> instructors, ArrayList<Integer> courses) throws SQLException {
        System.out.println("populating sections");
        ArrayList<Integer> result = new ArrayList<>();

        for (int year = minYear; year <= maxYear; year++) {
            for (int semester = 1; semester < 3; semester++) {
                for (Integer courseId : courses) {
                    int insId = AllSchemas.randomElement(instructors);
                    int building = AllSchemas.random(1, nBuildings + 1);
                    int sectionId = insertSection(semester, year, insId, courseId, building, building);
                    result.add(sectionId);
                }
            }
        }
        return result;
    }

    private void populateTakes(ArrayList<Integer> studentIds, ArrayList<Integer> sectionIds)
            throws SQLException {
        System.out.println("populating takes");
        for (Integer studentId : studentIds) {
            int nCourses = AllSchemas.random(4, 8);
            for (int i = 0; i < nCourses; i++) {
                int sectionId = AllSchemas.randomElement(sectionIds);
                double grade = AllSchemas.random(0.7, 5, 1);
                insertTakes(studentId, sectionId, grade);
            }
        }
    }

    private void populateSectionTime(ArrayList<Integer> sections) throws SQLException {
        System.out.println("populating section time");
        int slot = 1;
        for (Integer sectionId : sections) {
            int nSlots = AllSchemas.random(1, 6);
            for (int i = 0; i < nSlots; i++) {
                slot++;
                insertSectionTime(slot % 10000 + 1, sectionId);
            }
        }
    }

    public static void insertSchema1(Connection connection) throws SQLException {
        Schema1 schema1 = new Schema1(connection);
        schema1.populateClassroom();
        schema1.populateTimeSlot();
        schema1.populateDepartment();
    }

}