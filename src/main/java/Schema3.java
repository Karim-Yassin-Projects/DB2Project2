import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

public class Schema3 {
    private static final String[] colors = {"white", "red", "green", "blue", "black", "yellow", "orange", "violet", "brown", "cyan"};
    public static final int nSailors = 19000;
    public static final int nBoats = 3000;
    public static final int nReserves = 35000;

    private static final int INSERT_SAILOR = 0;
    private static final int INSERT_BOAT = 1;
    private static final int INSERT_RESERVES = 2;

    private final PreparedStatement[] preparedStatements = new PreparedStatement[3];

    public Schema3(Connection conn) throws SQLException {
        preparedStatements[INSERT_SAILOR] = conn.prepareStatement("INSERT INTO Sailors(sid,sname,rating,age) VALUES(?,?,?,?);", PreparedStatement.RETURN_GENERATED_KEYS);
        preparedStatements[INSERT_BOAT] = conn.prepareStatement("INSERT INTO Boat(bid,bname,color) VALUES(?,?,?);", PreparedStatement.RETURN_GENERATED_KEYS);
        preparedStatements[INSERT_RESERVES] = conn.prepareStatement("INSERT INTO Reserves(sid,bid,day) VALUES(?,?,?);");
    }

    private int insertSailor(int ID, String Name, int rating, double age) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_SAILOR];

        pstmt.setInt(1, ID);
        pstmt.setString(2, Name);
        pstmt.setInt(3, rating);
        pstmt.setDouble(4, age);

        return AllSchemas.getGeneratedId(pstmt);
    }

    // CREATE TABLE Boat(bid INT PRIMARY KEY, bname CHAR(20), color CHAR(10));
    private int insertBoat(int ID, String Name, String color) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_BOAT];

        pstmt.setInt(1, ID);
        pstmt.setString(2, Name);
        pstmt.setString(3, color);

        return AllSchemas.getGeneratedId(pstmt);
    }

    // CREATE TABLE Reserves(sid INT REFERENCES Sailors, bid INT REFERENCES Boat,
    // day date, PRIMARY KEY(sid,bid));
    private boolean insertReserves(int sID, int bID, Date day) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_RESERVES];
        boolean success = false;
        pstmt.setInt(1, sID);
        pstmt.setInt(2, bID);
        pstmt.setDate(3, day);

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

    ///////////////////////////////////////////////////////// Data Population
    ///////////////////////////////////////////////////////// Methods
    ///////////////////////////////////////////////////////// //////////////////////////////////////////////////////////
    private ArrayList<Integer> populateSailor() throws SQLException {
        System.out.println("Populating sailors");
        ArrayList<Integer> result = new ArrayList<>();
        for (int i = 1; i <= nSailors; i++) {
            int age = AllSchemas.random(18, 60);
            int rating = AllSchemas.random(1, 11);
            int sailorID = insertSailor(i, AllSchemas.randomName(), rating, age);
            result.add(sailorID);
        }
        return result;
    }

    private ArrayList<Integer> populateBoat() throws SQLException {
        System.out.println("Populating boats");
        ArrayList<Integer> result = new ArrayList<>();
        for (int i = 1; i <= nBoats; i++) {
            String color = AllSchemas.randomElement(colors);
            int boatID = insertBoat(i, "Boat" + i, color);
            result.add(boatID);
        }
        return result;
    }

    private void populateReserves(ArrayList<Integer> boats, ArrayList<Integer> sailors)
            throws SQLException {
        System.out.println("Populating reserves");
        for (int i = 1; i <= nReserves; i++) {
            int boatID = AllSchemas.randomBoolean() ? 103 : AllSchemas.randomElement(boats);
            int sailorID = AllSchemas.randomElement(sailors);
            if (!insertReserves(sailorID, boatID, AllSchemas.randomDate())) {
                i--;
            }
        }
    }

    public static void insertSchema3(Connection connection) throws SQLException {
        Schema3 schema3 = new Schema3(connection);
        ArrayList<Integer> sailors = schema3.populateSailor();
        ArrayList<Integer> boats = schema3.populateBoat();
        schema3.populateReserves(boats, sailors);
    }
}