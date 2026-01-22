package service.ingestion;

import org.example.models.entity.Source;
import org.example.service.ingestion.DatabaseDestinationWriter;
import org.example.utils.DatabaseConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DatabaseDestinationWriterTest {

    @Mock
    private DatabaseConnector databaseConnector;

    private DatabaseDestinationWriter writer;
    private JdbcTemplate realJdbcTemplate;
    private DataSource realDataSource;
    private String realJdbcUrl;

    @BeforeEach
    void setUp() {
        writer = new DatabaseDestinationWriter(databaseConnector);

        // Setup real H2 database for integration tests
        // DB_CLOSE_DELAY=-1 keeps the database alive between connections
        realJdbcUrl = "jdbc:h2:mem:testdb_" + System.nanoTime() + ";DB_CLOSE_DELAY=-1";
        realDataSource = new DriverManagerDataSource(realJdbcUrl, "sa", "");
        realJdbcTemplate = new JdbcTemplate(realDataSource);

        // Configure mock to always return a new DataSource with the correct URL
        when(databaseConnector.buildDataSource(any(), any(), any()))
                .thenAnswer(invocation -> new DriverManagerDataSource(realJdbcUrl, "sa", ""));
    }

    @AfterEach
    void tearDown() {
        // Clean up database
        if (realJdbcTemplate != null) {
            try {
                realJdbcTemplate.execute("SHUTDOWN");
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    @Test
    void testWrite_withEmptyRecords_shouldReturnZero() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("table", "users");

        int result = writer.write(null, config, new ArrayList<>());

        assertEquals(0, result);
    }

    @Test
    void testWrite_withNullRecords_shouldReturnZero() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("table", "users");

        int result = writer.write(null, config, null);

        assertEquals(0, result);
    }

    @Test
    void testWrite_withMissingJdbcUrl_shouldThrowException() {
        Map<String, Object> config = new HashMap<>();
        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            writer.write(null, config, records);
        });

        assertTrue(exception.getMessage().contains("jdbcUrl"));
    }

    @Test
    void testWrite_withSingleRecord_shouldInsertSuccessfully() {
        realJdbcTemplate.execute("CREATE TABLE test_users (id INT, name VARCHAR(255))");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);
        config.put("table", "test_users");

        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "name", "John")
        );

        int result = writer.write(null, config, records);

        assertEquals(1, result);

        Integer count = realJdbcTemplate.queryForObject("SELECT COUNT(*) FROM test_users", Integer.class);
        assertEquals(1, count);
    }

    @Test
    void testWrite_withMultipleRecords_shouldInsertAll() {
        realJdbcTemplate.execute("CREATE TABLE test_products (id INT, name VARCHAR(255), price DECIMAL)");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);
        config.put("table", "test_products");

        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "name", "Product A", "price", 10.99),
                Map.of("id", 2, "name", "Product B", "price", 20.99),
                Map.of("id", 3, "name", "Product C", "price", 30.99)
        );

        int result = writer.write(null, config, records);

        assertEquals(3, result);

        Integer count = realJdbcTemplate.queryForObject("SELECT COUNT(*) FROM test_products", Integer.class);
        assertEquals(3, count);
    }

    @Test
    void testWrite_withConnectionConfig_shouldResolveCredentials() {
        realJdbcTemplate.execute("CREATE TABLE test_data (id INT)");

        Map<String, Object> config = new HashMap<>();
        Map<String, Object> connection = new HashMap<>();
        connection.put("host", "localhost");
        connection.put("database", "testdb");
        connection.put("username", "user");
        connection.put("password", "pass");
        config.put("connection", connection);
        config.put("table", "test_data");

        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        int result = writer.write(null, config, records);

        assertEquals(1, result);
        verify(databaseConnector).buildDataSource(
                eq("jdbc:postgresql://localhost:5432/testdb"),
                eq("user"),
                eq("pass")
        );
    }

    @Test
    void testWrite_withColumnMapping_shouldMapColumns() {
        realJdbcTemplate.execute("CREATE TABLE test_users (user_id INT, user_name VARCHAR(255))");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);
        config.put("table", "test_users");
        config.put("columnMapping", Map.of("id", "user_id", "name", "user_name"));

        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "name", "John")
        );

        int result = writer.write(null, config, records);

        assertEquals(1, result);

        Map<String, Object> row = realJdbcTemplate.queryForMap("SELECT * FROM test_users");
        assertEquals(1, row.get("USER_ID"));
        assertEquals("John", row.get("USER_NAME"));
    }

    @Test
    void testWrite_withPerRecordDestinationTable_shouldRouteToCorrectTables() {
        realJdbcTemplate.execute("CREATE TABLE users (id INT)");
        realJdbcTemplate.execute("CREATE TABLE orders (id INT)");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);

        Map<String, Object> record1 = new HashMap<>();
        record1.put("id", 1);
        record1.put("destination_table", "users");

        Map<String, Object> record2 = new HashMap<>();
        record2.put("id", 100);
        record2.put("destination_table", "orders");

        List<Map<String, Object>> records = List.of(record1, record2);

        int result = writer.write(null, config, records);

        assertEquals(2, result);

        Integer usersCount = realJdbcTemplate.queryForObject("SELECT COUNT(*) FROM users", Integer.class);
        Integer ordersCount = realJdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Integer.class);

        assertEquals(1, usersCount);
        assertEquals(1, ordersCount);
    }

    @Test
    void testWrite_withDestinationTableInMetadata_shouldRouteCorrectly() {
        realJdbcTemplate.execute("CREATE TABLE products (id INT)");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);

        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("__meta__", Map.of("destination_table", "products"));

        List<Map<String, Object>> records = List.of(record);

        int result = writer.write(null, config, records);

        assertEquals(1, result);
    }

    @Test
    void testWrite_withSchemaQualifiedTable_shouldHandleSchema() {
        realJdbcTemplate.execute("CREATE SCHEMA IF NOT EXISTS test_schema");
        realJdbcTemplate.execute("CREATE TABLE test_schema.items (id INT)");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);
        config.put("table", "test_schema.items");

        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        int result = writer.write(null, config, records);

        assertEquals(1, result);
    }

    @Test
    void testWrite_withUnmatchedColumns_shouldOnlyInsertMatchingColumns() {
        realJdbcTemplate.execute("CREATE TABLE test_table (id INT, name VARCHAR(255))");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);
        config.put("table", "test_table");

        // Record has extra columns that don't exist in the table
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "name", "John", "extra_column", "ignored")
        );

        int result = writer.write(null, config, records);

        assertEquals(1, result);

        Map<String, Object> row = realJdbcTemplate.queryForMap("SELECT * FROM test_table");
        assertEquals(1, row.get("ID"));
        assertEquals("John", row.get("NAME"));
    }

    @Test
    void testWrite_withNoMatchingColumns_shouldReturnZero() {
        realJdbcTemplate.execute("CREATE TABLE test_table (id INT, name VARCHAR(255))");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);
        config.put("table", "test_table");

        // Record has no matching columns
        List<Map<String, Object>> records = List.of(
                Map.of("completely_different_column", "value")
        );

        int result = writer.write(null, config, records);

        assertEquals(0, result);
    }

    @Test
    void testWrite_withNullValues_shouldHandleGracefully() {
        realJdbcTemplate.execute("CREATE TABLE test_table (id INT, name VARCHAR(255), age INT)");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);
        config.put("table", "test_table");

        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("name", "John");
        record.put("age", null);

        List<Map<String, Object>> records = List.of(record);

        int result = writer.write(null, config, records);

        assertEquals(1, result);

        Map<String, Object> row = realJdbcTemplate.queryForMap("SELECT * FROM test_table");
        assertEquals(1, row.get("ID"));
        assertEquals("John", row.get("NAME"));
        assertNull(row.get("AGE"));
    }

    @Test
    void testWrite_withCaseInsensitiveColumns_shouldMatch() {
        realJdbcTemplate.execute("CREATE TABLE test_table (UserId INT, UserName VARCHAR(255))");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);
        config.put("table", "test_table");

        // Record uses lowercase
        List<Map<String, Object>> records = List.of(
                Map.of("userid", 1, "username", "John")
        );

        int result = writer.write(null, config, records);

        assertEquals(1, result);
    }

    @Test
    void testWrite_withMissingTableName_shouldThrowException() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);

        // Records without destination_table metadata
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1)
        );

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            writer.write(null, config, records);
        });

        assertTrue(exception.getMessage().contains("table name"));
    }

    @Test
    void testWrite_withTableField_shouldReadFromRecordField() {
        realJdbcTemplate.execute("CREATE TABLE dynamic_table (id INT)");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);
        config.put("tableField", "target");

        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("target", "dynamic_table");

        List<Map<String, Object>> records = List.of(record);

        int result = writer.write(null, config, records);

        assertEquals(1, result);
    }

    @Test
    void testWrite_withDefaultTable_shouldUseAsFallback() {
        realJdbcTemplate.execute("CREATE TABLE default_table (id INT)");

        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", realJdbcUrl);
        config.put("defaultTable", "default_table");

        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        int result = writer.write(null, config, records);

        assertEquals(1, result);
    }

    @Test
    void testWrite_withHostAndDatabase_shouldBuildJdbcUrl() {
        realJdbcTemplate.execute("CREATE TABLE test_table (id INT)");

        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("database", "testdb");
        config.put("table", "test_table");

        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        int result = writer.write(null, config, records);

        assertEquals(1, result);
        verify(databaseConnector).buildDataSource(
                eq("jdbc:postgresql://localhost:5432/testdb"),
                any(),
                any()
        );
    }

    @Test
    void testWrite_withCustomPort_shouldUseCustomPort() {
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("port", "3306");
        config.put("database", "testdb");
        config.put("table", "test_table");

        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        try {
            writer.write(null, config, records);
        } catch (Exception e) {
            // Table might not exist, but we're just testing URL construction
        }

        verify(databaseConnector).buildDataSource(
                eq("jdbc:postgresql://localhost:3306/testdb"),
                any(),
                any()
        );
    }
}
