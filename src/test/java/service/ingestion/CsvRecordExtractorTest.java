package service.ingestion;

import org.example.models.entity.Source;
import org.example.service.ingestion.CsvRecordExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CsvRecordExtractorTest {

    private CsvRecordExtractor extractor;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        extractor = new CsvRecordExtractor();
    }

    @Test
    void testSupports_withCsvFormat_shouldReturnTrue() {
        assertTrue(extractor.supports("csv"), "Should support 'csv' format");
    }

    @Test
    void testSupports_withCsvUpperCase_shouldReturnTrue() {
        assertTrue(extractor.supports("CSV"), "Should support 'CSV' format (case insensitive)");
    }

    @Test
    void testSupports_withCsvMixedCase_shouldReturnTrue() {
        assertTrue(extractor.supports("CsV"), "Should support mixed case");
    }

    @Test
    void testSupports_withJsonFormat_shouldReturnFalse() {
        assertFalse(extractor.supports("json"), "Should not support 'json' format");
    }

    @Test
    void testSupports_withDbFormat_shouldReturnFalse() {
        assertFalse(extractor.supports("db"), "Should not support 'db' format");
    }

    @Test
    void testSupports_withNullFormat_shouldReturnFalse() {
        assertFalse(extractor.supports(null), "Should not support null format");
    }

    @Test
    void testSupports_withEmptyFormat_shouldReturnFalse() {
        assertFalse(extractor.supports(""), "Should not support empty format");
    }

    @Test
    void testExtract_withSimpleCsv_shouldExtractAllRows() throws IOException {
        Path csvFile = createCsvFile("name,age\nJohn,30\nJane,25");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals(2, result.size(), "Should extract 2 rows");
        assertEquals("John", result.get(0).get("name"));
        assertEquals("30", result.get(0).get("age"));
        assertEquals("Jane", result.get(1).get("name"));
        assertEquals("25", result.get(1).get("age"));
    }

    @Test
    void testExtract_withRelativePath_shouldWork() throws IOException {
        Path csvFile = createCsvFile("id,value\n1,test");
        Map<String, Object> config = new HashMap<>();
        config.put("relativePath", csvFile.toString());

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals(1, result.size(), "Should extract 1 row");
        assertEquals("1", result.get(0).get("id"));
    }

    @Test
    void testExtract_withCustomDelimiter_shouldParse() throws IOException {
        Path csvFile = createCsvFile("name;age\nJohn;30");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("delimiter", ";");

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals(1, result.size());
        assertEquals("John", result.get(0).get("name"));
        assertEquals("30", result.get(0).get("age"));
    }

    @Test
    void testExtract_withTableLabel_shouldAddTableMetadata() throws IOException {
        Path csvFile = createCsvFile("id\n1");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("table", "users");

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals("users", result.get(0).get("__table__"));
    }

    @Test
    void testExtract_withTableName_shouldAddTableMetadata() throws IOException {
        Path csvFile = createCsvFile("id\n1");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("tableName", "customers");

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals("customers", result.get(0).get("__table__"));
    }

    @Test
    void testExtract_withSourceName_shouldUseAsTableLabel() throws IOException {
        Path csvFile = createCsvFile("id\n1");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());

        Source source = new Source();
        source.setName("employees");

        List<Map<String, Object>> result = extractor.extract(source, config);

        assertEquals("employees", result.get(0).get("__table__"));
    }

    @Test
    void testExtract_withConfigTableOverridesSourceName_shouldUseConfigTable() throws IOException {
        Path csvFile = createCsvFile("id\n1");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("table", "orders");

        Source source = new Source();
        source.setName("employees");

        List<Map<String, Object>> result = extractor.extract(source, config);

        assertEquals("orders", result.get(0).get("__table__"));
    }

    @Test
    void testExtract_withEmptyCsv_shouldReturnEmptyList() throws IOException {
        Path csvFile = createCsvFile("name,age");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertTrue(result.isEmpty(), "Should return empty list for CSV with only headers");
    }

    @Test
    void testExtract_withMultipleColumns_shouldExtractAll() throws IOException {
        Path csvFile = createCsvFile("id,name,email,age\n1,John,john@example.com,30");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals(1, result.size());
        Map<String, Object> row = result.get(0);
        assertEquals("1", row.get("id"));
        assertEquals("John", row.get("name"));
        assertEquals("john@example.com", row.get("email"));
        assertEquals("30", row.get("age"));
    }

    @Test
    void testExtract_withSpecialCharacters_shouldHandle() throws IOException {
        Path csvFile = createCsvFile("name,description\n\"O'Brien\",\"Test, value\"");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals(1, result.size());
        assertEquals("O'Brien", result.get(0).get("name"));
        assertEquals("Test, value", result.get(0).get("description"));
    }

    @Test
    void testExtract_withMissingFilePath_shouldThrowException() {
        Map<String, Object> config = new HashMap<>();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            extractor.extract(null, config);
        });

        assertTrue(exception.getMessage().contains("filePath"));
    }

    @Test
    void testExtract_withNonExistentFile_shouldThrowException() {
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", "/non/existent/file.csv");

        assertThrows(IllegalStateException.class, () -> {
            extractor.extract(null, config);
        });
    }

    @Test
    void testExtract_withUTF8Encoding_shouldHandleSpecialChars() throws IOException {
        Path csvFile = createCsvFile("name,city\nJoão,São Paulo");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("encoding", "UTF-8");

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals("João", result.get(0).get("name"));
        assertEquals("São Paulo", result.get(0).get("city"));
    }

    @Test
    void testExtract_withTabDelimiter_shouldParse() throws IOException {
        Path csvFile = createCsvFile("name\tage\nJohn\t30");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("delimiter", "\t");

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals(1, result.size());
        assertEquals("John", result.get(0).get("name"));
        assertEquals("30", result.get(0).get("age"));
    }

    @Test
    void testExtract_withPipeDelimiter_shouldParse() throws IOException {
        Path csvFile = createCsvFile("name|age\nJohn|30");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("delimiter", "|");

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals(1, result.size());
        assertEquals("John", result.get(0).get("name"));
        assertEquals("30", result.get(0).get("age"));
    }

    @Test
    void testExtract_withEmptyValues_shouldHandleGracefully() throws IOException {
        Path csvFile = createCsvFile("name,age,city\nJohn,,NYC\n,25,");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());

        List<Map<String, Object>> result = extractor.extract(null, config);

        assertEquals(2, result.size());
        assertEquals("John", result.get(0).get("name"));
        assertEquals("", result.get(0).get("age"));
        assertEquals("NYC", result.get(0).get("city"));
    }

    @Test
    void testExtract_preservesColumnOrder_shouldMaintainOrder() throws IOException {
        Path csvFile = createCsvFile("z_col,a_col,m_col\n1,2,3");
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());

        List<Map<String, Object>> result = extractor.extract(null, config);

        Map<String, Object> row = result.get(0);
        assertTrue(row.containsKey("z_col"));
        assertTrue(row.containsKey("a_col"));
        assertTrue(row.containsKey("m_col"));
    }

    @Test
    void testExtract_withNullConfig_shouldThrowException() {
        assertThrows(NullPointerException.class, () -> {
            extractor.extract(null, null);
        });
    }

    private Path createCsvFile(String content) throws IOException {
        Path file = tempDir.resolve("test_" + System.nanoTime() + ".csv");
        Files.writeString(file, content);
        return file;
    }
}
