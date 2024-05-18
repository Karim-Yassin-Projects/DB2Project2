import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Collectors;

public class Program {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        System.out.println("-------- Postgres JDBC Connection Testing ------------");
        Class.forName("org.postgresql.Driver");
        System.out.println("Postgres JDBC Driver Registered!");


        //createTables("schema1");
        //createTables("schema2");
        //createTables("schema3");
        createTables("schema4");

        // wait for enter to be pressed
        System.out.println("Press enter to populate schemas...");
        int result;
        do {
            result = System.in.read();
        } while (result != '\n');

        //insertSchema("schema1", Schema1::insertSchema1);
        //insertSchema("schema2", Schema2::insertSchema2);
        //insertSchema("schema3", Schema3::insertSchema3);
        insertSchema("schema4", Schema4::insertSchema4);
    }

    private static void insertSchema(String name, SchemaPopulator runnable) throws SQLException, IOException {
        try(Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/" + name, "postgres", "postgres")) {
            runnable.populate(connection);
        }
    }

    private static void createTables(String name) throws SQLException, IOException {
        try(Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/postgres", "postgres", "postgres")){
            dropDatabase(connection, name);
            createDatabase(connection, name);
        }
        try(Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/" + name, "postgres", "postgres")) {
            try (InputStream stream = ClassLoader.getSystemResourceAsStream(name + ".sql")) {
                if (stream == null) {
                    throw new RuntimeException("Failed to load schema file: " + name + ".sql");
                }
                System.out.println("Loading schema: " + name + " ...");
                InputStreamReader reader = new InputStreamReader(stream);
                String sql = new BufferedReader(reader).lines().collect(Collectors.joining("\n"));
                connection.createStatement().execute(sql);
                System.out.println("Done loading schema: " + name);
            }
        }
    }

    private static void dropDatabase(Connection conn, String name) throws SQLException {
        System.out.println("Dropping database: " + name + " ...");
        String SQL = "DROP DATABASE IF EXISTS " + name + ";";
        conn.createStatement().executeUpdate(SQL);
        System.out.println("Done dropping database: " + name);
    }

    private static void createDatabase(Connection conn, String name) throws SQLException {
        System.out.println("Creating database: " + name + " ...");
        String SQL = "CREATE DATABASE " + name + ";";
        conn.createStatement().executeUpdate(SQL);
        System.out.println("Done creating database: " + name);
    }
}
