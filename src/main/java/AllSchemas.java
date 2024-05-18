import java.sql.*;
import java.util.ArrayList;
import java.util.Random;

public class AllSchemas {
    public static final Random rand = new Random(1000);
    public static final String[] names = {
            "Ahmed", "Mohamed", "Ali", "Omar", "Amr", "Khaled", "Mahmoud", "Hassan", "Hussein",
            "Kareem", "Yassin", "Youssef", "Tarek", "Sherif", "Raghad", "Hanin",
            "Abdullah", "Youssef", "Tarek", "Mina", "Nour", "Hana", "Sara", "Nada", "Nouran"
    };

    public static final String[] lastNames = {
            "Ali", "Mohamed", "Omar", "Amr", "Khaled", "Mahmoud", "Hassan", "Hussein"
    };

    public static String randomName() {
        return AllSchemas.randomElement(names) + " " + AllSchemas.randomElement(lastNames);
    }

    public static <T> T randomElement(T[] arr) {
        return arr[rand.nextInt(arr.length)];
    }

    public static <T> T randomElement(ArrayList<T> arr) {
        return arr.get(rand.nextInt(arr.size()));
    }

    public static int random(int minIncl, int maxExcl) {
        return rand.nextInt(maxExcl - minIncl) + minIncl;
    }

    public static double random(double minIncl, double maxExcl, int precision) {
        var scale = Math.pow(10, precision);
        var val = rand.nextDouble() * (maxExcl - minIncl) + minIncl;
        return Math.round(val * scale) / scale;
    }

    public static Time randomTime() {
        long hour = random(9, 17);
        long minute = (long)random(0, 4) * 15;
        var ms = (hour * 60 + minute) * 60 * 1000;
        return new Time(ms);
    }

    public static Time[] randomTimeRange() {
        var start = randomTime();
        long duration = (long)random(1, 5) * 15 * 60 * 1000;
        var end = new Time(start.getTime() + duration);
        return new Time[] {start, end};
    }

    public static Date randomDateOfBirth() {
        int daysAfter1971 = random(0, 365 * 30);
        return new Date((long)daysAfter1971 * 24 * 3600 * 1000);
    }

    public static Date randomDate() {
        int daysAfter1971 = random(365 * 50, 365 * 53);
        return new Date((long)daysAfter1971 * 24 * 3600 * 1000);
    }

    public static boolean randomBoolean() {
        return rand.nextBoolean();
    }

    public static int getGeneratedId(PreparedStatement pstmt) throws SQLException {
        int affectedRows = pstmt.executeUpdate();
        if (affectedRows == 0) {
            throw new SQLException("Insert failed, no rows affected.");
        }
        ResultSet rs = pstmt.getGeneratedKeys();
        if (rs.next()) {
            return rs.getInt(1);
        }
        throw new SQLException("Insert failed, no ID obtained.");
    }

    public static Date getDate(int year, int month, int day) {
        return Date.valueOf(year + "-" + month + "-" + day);
    }
}
