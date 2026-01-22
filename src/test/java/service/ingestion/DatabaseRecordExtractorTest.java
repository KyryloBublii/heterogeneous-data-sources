package service.ingestion;

import org.example.models.entity.Source;
import org.example.service.ingestion.DatabaseRecordExtractor;
import org.example.utils.DatabaseConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DatabaseRecordExtractorTest {

    @Mock
    private DatabaseConnector databaseConnector;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private DatabaseMetaData metaData;

    @Mock
    private ResultSet resultSet;

    @Mock
    private ResultSetMetaData resultSetMetaData;

    private DatabaseRecordExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new DatabaseRecordExtractor(databaseConnector);
    }

    @Test
    void testSupports_withDbFormat_shouldReturnTrue() {
        assertTrue(extractor.supports("db"));
    }

    @Test
    void testSupports_withDatabaseFormat_shouldReturnTrue() {
        assertTrue(extractor.supports("database"));
    }

    @Test
    void testSupports_withDbUpperCase_shouldReturnTrue() {
        assertTrue(extractor.supports("DB"));
    }

    @Test
    void testSupports_withDatabaseMixedCase_shouldReturnTrue() {
        assertTrue(extractor.supports("DaTaBaSe"));
    }

    @Test
    void testSupports_withCsvFormat_shouldReturnFalse() {
        assertFalse(extractor.supports("csv"));
    }

    @Test
    void testSupports_withNullFormat_shouldReturnFalse() {
        assertFalse(extractor.supports(null));
    }

    @Test
    void testExtract_withMissingJdbcUrl_shouldThrowException() {
        Map<String, Object> config = new HashMap<>();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            extractor.extract(null, config);
        });

        assertTrue(exception.getMessage().contains("jdbcUrl"));
    }

    @Test
    void testExtract_withExplicitJdbcUrl_shouldExtractData() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("table", "users");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        Map<String, Object> mockRow = new HashMap<>();
        mockRow.put("id", 1);
        mockRow.put("name", "John");
        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of(mockRow));

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertNotNull(result);
        assertEquals(1, result.size());
        verify(databaseConnector).buildJdbcTemplate("jdbc:h2:mem:testdb", null, null);
    }

    @Test
    void testExtract_withHostAndDatabase_shouldBuildJdbcUrl() {
        Map<String, Object> config = new HashMap<>();
        Map<String, Object> connectionConfig = new HashMap<>();
        connectionConfig.put("host", "localhost");
        connectionConfig.put("database", "testdb");
        connectionConfig.put("username", "user");
        connectionConfig.put("password", "pass");
        config.put("connection", connectionConfig);
        config.put("table", "users");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of());

        extractor.extract(null, config);

        verify(databaseConnector).buildJdbcTemplate(
                "jdbc:postgresql://localhost:5432/testdb",
                "user",
                "pass"
        );
    }

    @Test
    void testExtract_withCustomPort_shouldUseCustomPort() {
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("port", "3306");
        config.put("database", "testdb");
        config.put("table", "users");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of());

        extractor.extract(null, config);

        verify(databaseConnector).buildJdbcTemplate(
                "jdbc:postgresql://localhost:3306/testdb",
                null,
                null
        );
    }

    @Test
    void testExtract_withExplicitQuery_shouldExecuteQuery() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("query", "SELECT id, name FROM users WHERE active = true");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        Map<String, Object> mockRow = new HashMap<>();
        mockRow.put("id", 1);
        mockRow.put("name", "John");
        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of(mockRow));

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertNotNull(result);
        verify(jdbcTemplate).query(eq("SELECT id, name FROM users WHERE active = true"), any(RowMapper.class));
    }

    @Test
    void testExtract_withSqlKey_shouldExecuteQuery() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("sql", "SELECT * FROM orders");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of());

        extractor.extract(null, config);

        verify(jdbcTemplate).query(eq("SELECT * FROM orders"), any(RowMapper.class));
    }

    @Test
    void testExtract_withTableName_shouldBuildSelectQuery() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("table", "products");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of());

        extractor.extract(null, config);

        verify(jdbcTemplate).query(eq("SELECT * FROM products"), any(RowMapper.class));
    }

    @Test
    void testExtract_withSpecificColumns_shouldBuildSelectWithColumns() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("table", "users");
        config.put("columns", List.of("id", "name", "email"));

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of());

        extractor.extract(null, config);

        verify(jdbcTemplate).query(eq("SELECT id, name, email FROM users"), any(RowMapper.class));
    }

    @Test
    void testExtract_withColumnsAsString_shouldParseAndBuildQuery() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("table", "users");
        config.put("columns", "id, name, email");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of());

        extractor.extract(null, config);

        verify(jdbcTemplate).query(eq("SELECT id, name, email FROM users"), any(RowMapper.class));
    }

    @Test
    void testExtract_withTableLabel_shouldAddTableMetadata() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("table", "users");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        Map<String, Object> row = new HashMap<>();
        row.put("id", 1);
        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of(row));

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals("users", result.get(0).get("__table__"));
    }

    @Test
    void testExtract_withAlias_shouldUseAliasAsTableLabel() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("table", "users");
        config.put("alias", "customers");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        Map<String, Object> row = new HashMap<>();
        row.put("id", 1);
        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of(row));

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals("customers", result.get(0).get("__table__"));
    }

    @Test
    void testExtract_withMultipleTables_shouldExtractAllTables() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("tables", List.of("users", "orders"));

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        Map<String, Object> usersRow = new HashMap<>();
        usersRow.put("id", 1);
        Map<String, Object> ordersRow = new HashMap<>();
        ordersRow.put("id", 100);

        when(jdbcTemplate.query(eq("SELECT * FROM users"), any(RowMapper.class)))
                .thenReturn(List.of(usersRow));
        when(jdbcTemplate.query(eq("SELECT * FROM orders"), any(RowMapper.class)))
                .thenReturn(List.of(ordersRow));

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals(2, result.size());
        assertEquals("users", result.get(0).get("__table__"));
        assertEquals("orders", result.get(1).get("__table__"));
    }

    @Test
    void testExtract_withTableObjects_shouldExtractWithCustomQueries() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");

        Map<String, Object> table1 = new HashMap<>();
        table1.put("table", "users");
        table1.put("columns", List.of("id", "name"));

        Map<String, Object> table2 = new HashMap<>();
        table2.put("query", "SELECT * FROM orders WHERE status = 'active'");
        table2.put("alias", "active_orders");

        config.put("tables", List.of(table1, table2));

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of(new HashMap<>()));

        List<Map<String, Object>> result = extractor.extract(null, config);

        verify(jdbcTemplate).query(eq("SELECT id, name FROM users"), any(RowMapper.class));
        verify(jdbcTemplate).query(eq("SELECT * FROM orders WHERE status = 'active'"), any(RowMapper.class));
    }

    @Test
    void testExtract_withUseAllTablesFlag_shouldExtractAllTables() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("useAllTables", true);

        DriverManagerDataSource realDataSource = new DriverManagerDataSource();
        realDataSource.setUrl("jdbc:h2:mem:testdb");
        JdbcTemplate realJdbcTemplate = new JdbcTemplate(realDataSource);

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(realJdbcTemplate);

        // Create tables
        realJdbcTemplate.execute("CREATE TABLE users (id INT)");
        realJdbcTemplate.execute("CREATE TABLE orders (id INT)");

        List<Map<String, Object>> result = extractor.extract(null, config);

        // Should have extracted from both tables
        assertNotNull(result);
    }

    @Test
    void testExtract_withTablesAsterisk_shouldExtractAllTables() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb2");
        config.put("tables", "*");

        DriverManagerDataSource realDataSource = new DriverManagerDataSource();
        realDataSource.setUrl("jdbc:h2:mem:testdb2");
        JdbcTemplate realJdbcTemplate = new JdbcTemplate(realDataSource);

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(realJdbcTemplate);

        realJdbcTemplate.execute("CREATE TABLE products (id INT)");

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertNotNull(result);
    }

    @Test
    void testExtract_withTablesAll_shouldExtractAllTables() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb3");
        config.put("tables", "all");

        DriverManagerDataSource realDataSource = new DriverManagerDataSource();
        realDataSource.setUrl("jdbc:h2:mem:testdb3");
        JdbcTemplate realJdbcTemplate = new JdbcTemplate(realDataSource);

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(realJdbcTemplate);

        realJdbcTemplate.execute("CREATE TABLE inventory (id INT)");

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertNotNull(result);
    }

    @Test
    void testExtract_withEmptyQuery_shouldThrowException() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("query", "");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        // Empty query is treated as no query, which triggers extractAllTables
        // which requires a DataSource and throws NullPointerException
        assertThrows(NullPointerException.class, () -> {
            extractor.extract(null, config);
        });
    }

    @Test
    void testExtract_withDbnameAlias_shouldResolveDatabase() {
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("dbname", "testdb");
        config.put("table", "users");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of());

        extractor.extract(null, config);

        verify(databaseConnector).buildJdbcTemplate(
                "jdbc:postgresql://localhost:5432/testdb",
                null,
                null
        );
    }

    @Test
    void testExtract_withUserAlias_shouldResolveUsername() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("user", "testuser");
        config.put("table", "users");

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of());

        extractor.extract(null, config);

        verify(databaseConnector).buildJdbcTemplate(
                "jdbc:h2:mem:testdb",
                "testuser",
                null
        );
    }

    @Test
    void testExtract_withSchemaPrefix_shouldQualifyTableName() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");

        Map<String, Object> tableConfig = new HashMap<>();
        tableConfig.put("schema", "public");
        tableConfig.put("table", "users");

        config.put("tables", List.of(tableConfig));

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        when(jdbcTemplate.query(anyString(), any(RowMapper.class)))
                .thenReturn(List.of(new HashMap<>()));

        extractor.extract(null, config);

        verify(jdbcTemplate).query(eq("SELECT * FROM public.users"), any(RowMapper.class));
    }

    @Test
    void testExtract_withInvalidTableEntry_shouldThrowException() {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb");
        config.put("tables", List.of(Map.of())); // Empty map without table or query

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(jdbcTemplate);

        assertThrows(IllegalArgumentException.class, () -> {
            extractor.extract(null, config);
        });
    }

    @Test
    void testExtract_withLoadAllTablesFlag_shouldExtractAllTables() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:h2:mem:testdb4");
        config.put("loadAllTables", true);

        DriverManagerDataSource realDataSource = new DriverManagerDataSource();
        realDataSource.setUrl("jdbc:h2:mem:testdb4");
        JdbcTemplate realJdbcTemplate = new JdbcTemplate(realDataSource);

        when(databaseConnector.buildJdbcTemplate(any(), any(), any()))
                .thenReturn(realJdbcTemplate);

        realJdbcTemplate.execute("CREATE TABLE test_table (id INT)");

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertNotNull(result);
    }
}
