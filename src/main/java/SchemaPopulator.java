import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface SchemaPopulator {
    void populate(Connection connection) throws SQLException;
}
