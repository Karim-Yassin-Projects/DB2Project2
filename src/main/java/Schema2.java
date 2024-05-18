import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class Schema2 {
    private static final String[] relationships = {"child", "parent", "cousin"};
    private static final String[] genders = {"F", "M"};
    private static final int nEmployees = 16000;
    private static final int nSupervisors = 4000;
    private static final int nDepartments = 150;
    private static final int nProjects = 9200;

    private static final int INSERT_EMPLOYEE = 0;
    private static final int INSERT_DEPARTMENT = 1;
    private static final int INSERT_DEPT_LOCATIONS = 2;
    private static final int INSERT_PROJECT = 3;
    private static final int INSERT_WORKS_ON = 4;
    private static final int INSERT_DEPENDENT = 5;

    private static final PreparedStatement[] preparedStatements = new PreparedStatement[6];

    public Schema2(Connection conn) throws SQLException {
        preparedStatements[INSERT_EMPLOYEE] = conn.prepareStatement("INSERT INTO employee(fname,minit,lname,ssn,bdate,address,sex,salary,super_snn,dno) VALUES(?,?,?,?,?,?,?,?,?,?);");
        preparedStatements[INSERT_DEPARTMENT] = conn.prepareStatement("INSERT INTO department(dname,dnumber,mgr_snn,mgr_start_date) VALUES(?,?,?,?);");
        preparedStatements[INSERT_DEPT_LOCATIONS] = conn.prepareStatement("INSERT INTO dept_locations(dnumber,dlocation) VALUES(?,?);");
        preparedStatements[INSERT_PROJECT] = conn.prepareStatement("INSERT INTO project(Pname,Pnumber,Plocation,Dnumber) VALUES(?,?,?,?);");
        preparedStatements[INSERT_WORKS_ON] = conn.prepareStatement("INSERT INTO works_on(essn,pno,hours) VALUES(?,?,?);");
        preparedStatements[INSERT_DEPENDENT] = conn.prepareStatement("INSERT INTO dependent(essn,dependent_name,sex,bdate,relationship) VALUES(?,?,?,?,?);");
    }


    // CREATE TABLE Employee(Fname CHAR(20), Minit CHAR(10), Lname CHAR(20), ssn INT
    // PRIMARY KEY, Bdate date, address CHAR(20), sex CHARACTER(1), salary INT,
    // Super_snn INT REFERENCES Employee(ssn), dno INT);

    private void insertEmployee(String Fname, String Minit, String Lname, int ssn, Date Bdate, String address, String sex, int salary, Integer superSSN, int dno) throws SQLException {

        PreparedStatement pstmt = preparedStatements[INSERT_EMPLOYEE];

        pstmt.setString(1, Fname);
        pstmt.setString(2, Minit);
        pstmt.setString(3, Lname);
        pstmt.setInt(4, ssn);
        pstmt.setDate(5, Bdate);
        pstmt.setString(6, address);
        pstmt.setString(7, sex);
        pstmt.setInt(8, salary);
        if (superSSN == null) {
            pstmt.setNull(9, Types.INTEGER);
        } else {
            pstmt.setInt(9, superSSN);
        }
        pstmt.setInt(10, dno);

        pstmt.executeUpdate();
    }
    // CREATE TABLE Department(Dname CHAR(20), Dnumber INT PRIMARY KEY, Mgr_snn int
    // REFERENCES employee, Mgr_start_date date );

    private void insertDepartment(String Dname, int Dnumber, int MgrSSN, Date startDate)
            throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_DEPARTMENT];

        pstmt.setString(1, Dname);
        pstmt.setInt(2, Dnumber);
        pstmt.setInt(3, MgrSSN);
        pstmt.setDate(4, startDate);
        pstmt.executeUpdate();
    }

    // CREATE TABLE Dept_locations(Dnumber integer REFERENCES Department, Dlocation
    // CHAR(20), PRIMARY KEY(Dnumber,Dlocation));
    private void insertDeptLocations(int Dnumber, String Dlocation) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_DEPT_LOCATIONS];

        pstmt.setString(2, Dlocation);
        pstmt.setInt(1, Dnumber);
        pstmt.executeUpdate();
    }

    // CREATE TABLE Project(Pname CHAR(20), Pnumber INT PRIMARY KEY, Plocation
    // CHAR(50), Dnumber INT REFERENCES Department);
    public static void insertProject(String Pname, int Pnumber, String pLocation, int Dnumber)
            throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_PROJECT];

        pstmt.setString(1, Pname);
        pstmt.setInt(2, Pnumber);
        pstmt.setString(3, pLocation);
        pstmt.setInt(4, Dnumber);
        pstmt.executeUpdate();
    }

    // CREATE TABLE Works_on(Essn int REFERENCES Employee, Pno int REFERENCES
    // Project, Hours int, PRIMARY KEY(Essn,Pno));
    public boolean insertWorksOn(int Essn, int pNo, int hours) throws SQLException {
        boolean success = false;

        PreparedStatement pstmt = preparedStatements[INSERT_WORKS_ON];

        pstmt.setInt(2, pNo);
        pstmt.setInt(1, Essn);
        pstmt.setInt(3, hours);
        try {
            pstmt.executeUpdate();
            success = true;
        } catch (SQLException e) {
            if (!e.getSQLState().equals("23505")) {
                throw e;
            }
        }

        return success;
    }

    // CREATE TABLE Dependent(Essn INT REFERENCES Employee, Dependent_name CHAR(20),
    // sex CHARACTER(1), Bdate date, Relationship CHAR(20), PRIMARY KEY(Essn,
    // Dependent_name));
    public void insertDependent(int Essn, String dependentName, String sex, Date Bdate, String relationship) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_DEPENDENT];

        pstmt.setInt(1, Essn);
        pstmt.setString(2, dependentName);
        pstmt.setString(3, sex);
        pstmt.setDate(4, Bdate);
        pstmt.setString(5, relationship);

        pstmt.executeUpdate();
    }

    /////////////////////////////////////////////// Data Population Methods
    /////////////////////////////////////////////// //////////////////////////////////////////////////////////////
    private void populateEmployee() throws SQLException {
        System.out.println("Populating Employee");
        // inserting Supervisors
        for (int i = 1; i <= nSupervisors; i++) {
            String sex = AllSchemas.randomElement(genders);
            int random_supervisor_salary = AllSchemas.random(25000, 80001);
            int random_department = AllSchemas.random(1, 151);
            if (random_department == 5) {
                random_supervisor_salary = AllSchemas.random(10000, 20001);
            }
            insertEmployee(AllSchemas.randomElement(
                    sex.equals("M") ? AllSchemas.maleNames : AllSchemas.femaleNames
                    ),
                    Character.toString(AllSchemas.random('A', 'Z' + 1)),
                    i == 1 ? "employee1" : AllSchemas.randomElement(AllSchemas.lastNames), i,
                    AllSchemas.randomDateOfBirth(), "address" + i, sex,
                    random_supervisor_salary, null, random_department);
        }

        for (int i = nSupervisors + 1; i <= nEmployees; i++) {
            String sex = AllSchemas.randomElement(genders);
            int random_department = AllSchemas.random(1, 151);
            int random_salary;
            if (random_department == 5) {
                random_salary = AllSchemas.random(10000, 20001);
            } else {
                random_salary = AllSchemas.random(10000, 80001);
            }
            int superSNN = AllSchemas.random(1, nSupervisors + 1);
            insertEmployee(
                    AllSchemas.randomElement(sex.equals("M") ? AllSchemas.maleNames : AllSchemas.femaleNames),
                    Character.toString(AllSchemas.random('A', 'Z' + 1)),
                    AllSchemas.randomElement(AllSchemas.lastNames), i, AllSchemas.randomDateOfBirth(), "address" + i, sex,
                    random_salary, superSNN, random_department);
        }

    }

    private void populateDepartment() throws SQLException {
        System.out.println("Populating Department");
        for (int i = 1; i <= nDepartments; i++) {
            int random_manager = AllSchemas.random(1, nSupervisors + 1);
            insertDepartment("Department" + i, i, random_manager,
                    AllSchemas.randomDate());
        }
    }

    private void populateDeptLocations() throws SQLException {
        System.out.println("Populating Dept_Locations");
        for (int i = 1; i <= nDepartments; i++) {
            insertDeptLocations(i, "Location" + i);
        }
    }

    private void populateProject() throws SQLException {
        System.out.println("Populating Project");
        for (int i = 1; i <= nProjects; i++) {
            int depts = AllSchemas.random(1, nDepartments + 1);
            insertProject("Project" + i, i, "Location" + depts, depts);
        }
    }

    public void populateWorksOn() throws SQLException {
        System.out.println("Populating Works_On");
        for (int j = 1; j <= 16000; j++) {
            int projects;
            if (j == 1) {
                projects = 600;
            } else {
                projects = 2;
            }
            for (int i = 1; i <= projects; i++) {
                int randomHours = AllSchemas.random(1, 100);
                int randomProject = AllSchemas.random(1, nProjects + 1);
                if (!insertWorksOn(j, randomProject, randomHours)) {
                    i--;
                }
            }
        }
    }

    private void populateDependent() throws SQLException {
        System.out.println("Populating Dependent");
        for (int i = 1; i <= 10000; i++) {
            String sex = AllSchemas.randomElement(genders);
            String relation = AllSchemas.randomElement(relationships);
            insertDependent(i, AllSchemas.randomElement(sex.equals("M") ? AllSchemas.maleNames : AllSchemas.femaleNames), sex, AllSchemas.randomDate(), relation);
        }
    }

    public static void insertSchema2(Connection connection) throws SQLException {
        Schema2 schema2 = new Schema2(connection);
        schema2.populateEmployee();
        schema2.populateDepartment();
        schema2.populateDeptLocations();
        schema2.populateProject();
        schema2.populateWorksOn();
        schema2.populateDependent();
    }
}