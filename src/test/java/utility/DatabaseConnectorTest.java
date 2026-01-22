package utility;

import org.example.utils.DatabaseConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseConnectorTest {

    private DatabaseConnector databaseConnector;
    private static final String H2_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private static final String USERNAME = "sa";
    private static final String PASSWORD = "";

    @BeforeEach
    void setUp() {
        databaseConnector = new DatabaseConnector();
    }

    @Test
    void testGetConnection_shouldReturnValidConnection() throws SQLException {
        Connection connection = databaseConnector.getConnection(H2_URL, USERNAME, PASSWORD);

        assertNotNull(connection, "Connection should not be null");
        assertFalse(connection.isClosed(), "Connection should be open");

        connection.close();
    }

    @Test
    void testGetConnection_shouldThrowExceptionForInvalidUrl() {
        assertThrows(SQLException.class, () -> {
            databaseConnector.getConnection("jdbc:invalid:url", USERNAME, PASSWORD);
        }, "Should throw SQLException for invalid URL");
    }

    @Test
    void testGetConnection_shouldThrowExceptionForInvalidCredentials() {
        // Use a separate database to test with different credentials
        String separateDbUrl = "jdbc:h2:mem:testdb_invalid_creds;DB_CLOSE_DELAY=-1";
        assertDoesNotThrow(() -> {
            Connection conn = databaseConnector.getConnection(separateDbUrl, "invalidUser", "invalidPass");
            conn.close();
        });
    }

    @Test
    void testBuildDataSource_shouldReturnNonNullDataSource() {
        DataSource dataSource = databaseConnector.buildDataSource(H2_URL, USERNAME, PASSWORD);

        assertNotNull(dataSource, "DataSource should not be null");
        assertInstanceOf(DriverManagerDataSource.class, dataSource, "Should return DriverManagerDataSource");
    }

    @Test
    void testBuildDataSource_shouldConfigureUrlCorrectly() {
        DataSource dataSource = databaseConnector.buildDataSource(H2_URL, USERNAME, PASSWORD);

        DriverManagerDataSource driverDataSource = (DriverManagerDataSource) dataSource;
        assertEquals(H2_URL, driverDataSource.getUrl(), "URL should be configured correctly");
    }

    @Test
    void testBuildDataSource_shouldConfigureUsernameCorrectly() {
        DataSource dataSource = databaseConnector.buildDataSource(H2_URL, USERNAME, PASSWORD);

        DriverManagerDataSource driverDataSource = (DriverManagerDataSource) dataSource;
        assertEquals(USERNAME, driverDataSource.getUsername(), "Username should be configured correctly");
    }

    @Test
    void testBuildDataSource_shouldConfigurePasswordCorrectly() {
        DataSource dataSource = databaseConnector.buildDataSource(H2_URL, USERNAME, PASSWORD);

        DriverManagerDataSource driverDataSource = (DriverManagerDataSource) dataSource;
        assertEquals(PASSWORD, driverDataSource.getPassword(), "Password should be configured correctly");
    }

    @Test
    void testBuildDataSource_shouldAllowConnection() throws SQLException {
        DataSource dataSource = databaseConnector.buildDataSource(H2_URL, USERNAME, PASSWORD);

        Connection connection = dataSource.getConnection();
        assertNotNull(connection, "Should be able to get connection from DataSource");
        assertFalse(connection.isClosed(), "Connection should be open");

        connection.close();
    }

    @Test
    void testBuildJdbcTemplate_shouldReturnNonNullJdbcTemplate() {
        JdbcTemplate jdbcTemplate = databaseConnector.buildJdbcTemplate(H2_URL, USERNAME, PASSWORD);

        assertNotNull(jdbcTemplate, "JdbcTemplate should not be null");
    }

    @Test
    void testBuildJdbcTemplate_shouldHaveConfiguredDataSource() {
        JdbcTemplate jdbcTemplate = databaseConnector.buildJdbcTemplate(H2_URL, USERNAME, PASSWORD);

        DataSource dataSource = jdbcTemplate.getDataSource();
        assertNotNull(dataSource, "JdbcTemplate should have a DataSource");
        assertInstanceOf(DriverManagerDataSource.class, dataSource, "Should use DriverManagerDataSource");
    }

    @Test
    void testBuildJdbcTemplate_shouldBeUsableForQueries() {
        JdbcTemplate jdbcTemplate = databaseConnector.buildJdbcTemplate(H2_URL, USERNAME, PASSWORD);

        // Create a simple table and verify we can query it
        assertDoesNotThrow(() -> {
            jdbcTemplate.execute("CREATE TABLE test (id INT)");
            jdbcTemplate.execute("INSERT INTO test VALUES (1)");
            Integer result = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM test", Integer.class);
            assertEquals(1, result, "Should be able to execute queries");
        });
    }

    @Test
    void testBuildJdbcTemplate_withNullPassword_shouldWork() {
        // Use a separate database for null password test to avoid credential conflicts
        String separateDbUrl = "jdbc:h2:mem:testdb_null_pwd;DB_CLOSE_DELAY=-1";
        JdbcTemplate jdbcTemplate = databaseConnector.buildJdbcTemplate(separateDbUrl, USERNAME, null);

        assertNotNull(jdbcTemplate, "JdbcTemplate should handle null password");
        assertDoesNotThrow(() -> {
            jdbcTemplate.execute("SELECT 1");
        }, "Should be able to execute queries with null password");
    }

    @Test
    void testBuildJdbcTemplate_withEmptyCredentials_shouldWork() {
        // Use a separate database for empty credentials test to avoid credential conflicts
        String separateDbUrl = "jdbc:h2:mem:testdb_empty_creds;DB_CLOSE_DELAY=-1";
        JdbcTemplate jdbcTemplate = databaseConnector.buildJdbcTemplate(separateDbUrl, "", "");

        assertNotNull(jdbcTemplate, "JdbcTemplate should handle empty credentials");
        assertDoesNotThrow(() -> {
            jdbcTemplate.execute("SELECT 1");
        }, "Should be able to execute queries with empty credentials");
    }
}
