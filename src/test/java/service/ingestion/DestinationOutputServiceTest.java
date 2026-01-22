package service.ingestion;

import org.example.models.entity.Source;
import org.example.models.enums.SourceType;
import org.example.service.ingestion.DatabaseDestinationWriter;
import org.example.service.ingestion.DestinationOutputService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DestinationOutputServiceTest {

    @Mock
    private DatabaseDestinationWriter databaseDestinationWriter;

    private DestinationOutputService service;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        service = new DestinationOutputService(databaseDestinationWriter);
    }

    @Test
    void testWrite_withNullDestination_shouldReturnEarly() {
        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        assertDoesNotThrow(() -> service.write(null, records));
    }

    @Test
    void testWrite_withNullConfig_shouldReturnEarly() {
        Source destination = new Source();
        destination.setConfig(null);

        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        assertDoesNotThrow(() -> service.write(destination, records));
    }

    @Test
    void testWrite_withDatabaseType_shouldDelegateToWriter() {
        Source destination = new Source();
        destination.setName("test-db");
        destination.setType(SourceType.DB);
        Map<String, Object> config = new HashMap<>();
        config.put("table", "users");
        destination.setConfig(config);

        List<Map<String, Object>> records = List.of(Map.of("id", 1, "name", "John"));

        when(databaseDestinationWriter.write(any(), any(), any())).thenReturn(1);

        service.write(destination, records);

        verify(databaseDestinationWriter).write(destination, config, records);
    }

    @Test
    void testWrite_withCsvType_shouldWriteCsvFile() throws IOException {
        Path csvPath = tempDir.resolve("output.csv");

        Source destination = new Source();
        destination.setType(SourceType.CSV);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvPath.toString());
        destination.setConfig(config);

        Map<String, Object> record1 = new LinkedHashMap<>();
        record1.put("id", 1);
        record1.put("name", "John");

        Map<String, Object> record2 = new LinkedHashMap<>();
        record2.put("id", 2);
        record2.put("name", "Jane");

        List<Map<String, Object>> records = List.of(record1, record2);

        service.write(destination, records);

        assertTrue(Files.exists(csvPath));

        String content = Files.readString(csvPath);
        assertTrue(content.contains("id,name"));
        assertTrue(content.contains("1,John"));
        assertTrue(content.contains("2,Jane"));
    }

    @Test
    void testWrite_withCsvFilePathKey_shouldWriteFile() throws IOException {
        Path csvPath = tempDir.resolve("data.csv");

        Source destination = new Source();
        destination.setType(SourceType.CSV);
        Map<String, Object> config = new HashMap<>();
        config.put("csvFilePath", csvPath.toString());
        destination.setConfig(config);

        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        service.write(destination, records);

        assertTrue(Files.exists(csvPath));
    }

    @Test
    void testWrite_withNestedDirectory_shouldCreateDirectories() throws IOException {
        Path csvPath = tempDir.resolve("nested/dir/output.csv");

        Source destination = new Source();
        destination.setType(SourceType.CSV);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvPath.toString());
        destination.setConfig(config);

        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        service.write(destination, records);

        assertTrue(Files.exists(csvPath));
        assertTrue(Files.exists(csvPath.getParent()));
    }

    @Test
    void testWrite_withSpecialCharactersInCsv_shouldEscapeCorrectly() throws IOException {
        Path csvPath = tempDir.resolve("escaped.csv");

        Source destination = new Source();
        destination.setType(SourceType.CSV);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvPath.toString());
        destination.setConfig(config);

        List<Map<String, Object>> records = List.of(
                Map.of("name", "O'Brien", "description", "Test, value with comma"),
                Map.of("name", "Smith", "description", "Value with \"quotes\"")
        );

        service.write(destination, records);

        String content = Files.readString(csvPath);
        assertTrue(content.contains("\"Test, value with comma\""));
        assertTrue(content.contains("\"Value with \"\"quotes\"\"\""));
    }

    @Test
    void testWrite_withNullValues_shouldHandleGracefully() throws IOException {
        Path csvPath = tempDir.resolve("nulls.csv");

        Source destination = new Source();
        destination.setType(SourceType.CSV);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvPath.toString());
        destination.setConfig(config);

        Map<String, Object> record = new LinkedHashMap<>();
        record.put("id", 1);
        record.put("name", null);
        record.put("age", 30);

        List<Map<String, Object>> records = List.of(record);

        service.write(destination, records);

        String content = Files.readString(csvPath);
        assertTrue(content.contains("1,,30"));
    }

    @Test
    void testWrite_withMultipleRecords_shouldCollectAllHeaders() throws IOException {
        Path csvPath = tempDir.resolve("headers.csv");

        Source destination = new Source();
        destination.setType(SourceType.CSV);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvPath.toString());
        destination.setConfig(config);

        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "name", "John"),
                Map.of("id", 2, "age", 25),
                Map.of("id", 3, "email", "test@example.com")
        );

        service.write(destination, records);

        String content = Files.readString(csvPath);
        String headerLine = content.lines().findFirst().orElse("");

        assertTrue(headerLine.contains("id"));
        assertTrue(headerLine.contains("name"));
        assertTrue(headerLine.contains("age"));
        assertTrue(headerLine.contains("email"));
    }

    @Test
    void testWrite_withNewlinesInData_shouldEscapeCorrectly() throws IOException {
        Path csvPath = tempDir.resolve("newlines.csv");

        Source destination = new Source();
        destination.setType(SourceType.CSV);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvPath.toString());
        destination.setConfig(config);

        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "description", "Line 1\nLine 2")
        );

        service.write(destination, records);

        String content = Files.readString(csvPath);
        assertTrue(content.contains("\"Line 1\nLine 2\""));
    }


    @Test
    void testWrite_withMissingFilePath_shouldLogWarning() {
        Source destination = new Source();
        destination.setType(SourceType.CSV);
        Map<String, Object> config = new HashMap<>();
        destination.setConfig(config);

        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        // Should not throw exception, just log warning
        assertDoesNotThrow(() -> service.write(destination, records));
    }

    @Test
    void testWrite_withEmptyRecords_shouldCreateEmptyFile() throws IOException {
        Path csvPath = tempDir.resolve("empty.csv");

        Source destination = new Source();
        destination.setType(SourceType.CSV);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvPath.toString());
        destination.setConfig(config);

        service.write(destination, List.of());

        assertTrue(Files.exists(csvPath));
        String content = Files.readString(csvPath);
        assertEquals("\n", content); // Just header newline, no headers
    }

    @Test
    void testWrite_shouldOverwriteExistingFile() throws IOException {
        Path csvPath = tempDir.resolve("overwrite.csv");
        Files.writeString(csvPath, "Old content");

        Source destination = new Source();
        destination.setType(SourceType.CSV);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvPath.toString());
        destination.setConfig(config);

        List<Map<String, Object>> records = List.of(Map.of("id", 1));

        service.write(destination, records);

        String content = Files.readString(csvPath);
        assertFalse(content.contains("Old content"));
        assertTrue(content.contains("id"));
    }
}
