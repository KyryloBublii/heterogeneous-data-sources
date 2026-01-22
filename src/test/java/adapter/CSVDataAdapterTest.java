package adapter;

import org.example.adapters.CSVDataAdapter;
import org.example.models.dto.UnifiedRecord;
import org.example.models.entity.ApplicationUser;
import org.example.models.entity.Source;
import org.example.models.enums.SourceRole;
import org.example.models.enums.SourceStatus;
import org.example.models.enums.SourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CSVDataAdapterTest {

private CSVDataAdapter adapter;
private Source testSource;

@TempDir
Path tempDir;

@BeforeEach
void setUp() {
    adapter = new CSVDataAdapter();

    ApplicationUser user = new ApplicationUser();
    user.setId(1L);
    user.setEmail("test@example.com");

    testSource = new Source();
    testSource.setId(1L);
    testSource.setSourceUid("csv-source-uid");
    testSource.setName("Test CSV Source");
    testSource.setType(SourceType.CSV);
    testSource.setRole(SourceRole.SOURCE);
    testSource.setStatus(SourceStatus.ACTIVE);
    testSource.setApplicationUser(user);
    testSource.setCreatedAt(Instant.now());
}

@Test
void extract_ValidCSV_ReturnsRecords() throws Exception {
    Path csvFile = tempDir.resolve("test.csv");
    String csvContent = "email,name,age\n" +
            "test1@example.com,User One,25\n" +
            "test2@example.com,User Two,30\n";
    Files.writeString(csvFile, csvContent);

    Map<String, Object> config = new HashMap<>();
    config.put("filePath", csvFile.toString());
    testSource.setConfig(config);

    List<UnifiedRecord> records = adapter.extract(testSource);

    assertNotNull(records);
    assertEquals(2, records.size());

    UnifiedRecord first = records.get(0);
    assertEquals("test1@example.com", first.getField("email"));
    assertEquals("User One", first.getField("name"));
    assertEquals("25", first.getField("age"));

    UnifiedRecord second = records.get(1);
    assertEquals("test2@example.com", second.getField("email"));
    assertEquals("User Two", second.getField("name"));
    assertEquals("30", second.getField("age"));
}

@Test
void extract_EmptyCSV_ReturnsEmptyList() throws Exception {
    Path csvFile = tempDir.resolve("empty.csv");
    Files.writeString(csvFile, "");

    Map<String, Object> config = new HashMap<>();
    config.put("filePath", csvFile.toString());
    testSource.setConfig(config);

    List<UnifiedRecord> records = adapter.extract(testSource);

    assertNotNull(records);
    assertTrue(records.isEmpty());
}

@Test
void extract_CSVWithHeadersOnly_ReturnsEmptyList() throws Exception {
    Path csvFile = tempDir.resolve("headers-only.csv");
    Files.writeString(csvFile, "email,name,age\n");

    Map<String, Object> config = new HashMap<>();
    config.put("filePath", csvFile.toString());
    testSource.setConfig(config);

    List<UnifiedRecord> records = adapter.extract(testSource);

    assertNotNull(records);
    assertTrue(records.isEmpty());
}

@Test
void extract_CSVWithMissingFields_HandlesGracefully() throws Exception {
    Path csvFile = tempDir.resolve("missing-fields.csv");
    String csvContent = "email,name,age\n" +
            "test1@example.com,User One,25\n" +
            "test2@example.com,,\n" +
            "test3@example.com,User Three,35\n";
    Files.writeString(csvFile, csvContent);

    Map<String, Object> config = new HashMap<>();
    config.put("filePath", csvFile.toString());
    testSource.setConfig(config);

    List<UnifiedRecord> records = adapter.extract(testSource);

    assertNotNull(records);
    assertEquals(3, records.size());

    UnifiedRecord second = records.get(1);
    assertEquals("test2@example.com", second.getField("email"));
}

@Test
void extract_NonExistentFile_ThrowsException() {
    Map<String, Object> config = new HashMap<>();
    config.put("filePath", "/nonexistent/file.csv");
    testSource.setConfig(config);

    assertThrows(Exception.class, () -> adapter.extract(testSource));
}

@Test
void extract_NoFilePathConfigured_ThrowsException() {
    Map<String, Object> config = new HashMap<>();
    testSource.setConfig(config);

    assertThrows(IllegalArgumentException.class, () -> adapter.extract(testSource));
}

@Test
void extract_ContentAddressablePath_UsesAlternativePath() throws Exception {
    Path csvFile = tempDir.resolve("content.csv");
    String csvContent = "id,value\n1,test\n";
    Files.writeString(csvFile, csvContent);

    Map<String, Object> config = new HashMap<>();
    config.put("contentAddressablePath", csvFile.toString());
    testSource.setConfig(config);

    List<UnifiedRecord> records = adapter.extract(testSource);

    assertNotNull(records);
    assertEquals(1, records.size());
}

@Test
void supportsSource_CSVType_ReturnsTrue() {
    testSource.setType(SourceType.CSV);

    boolean supports = adapter.supportsSource(testSource);

    assertTrue(supports);
}

@Test
void supportsSource_NonCSVType_ReturnsFalse() {
    testSource.setType(SourceType.DB);

    boolean supports = adapter.supportsSource(testSource);

    assertFalse(supports);
}

@Test
void extract_CSVWithQuotedFields_ParsesCorrectly() throws Exception {
    Path csvFile = tempDir.resolve("quoted.csv");
    String csvContent = "email,name,description\n" +
            "test@example.com,\"User, Name\",\"This is a \"\"quoted\"\" description\"\n";
    Files.writeString(csvFile, csvContent);

    Map<String, Object> config = new HashMap<>();
    config.put("filePath", csvFile.toString());
    testSource.setConfig(config);

    List<UnifiedRecord> records = adapter.extract(testSource);

    assertNotNull(records);
    assertEquals(1, records.size());

    UnifiedRecord record = records.get(0);
    assertEquals("test@example.com", record.getField("email"));
    assertEquals("User, Name", record.getField("name"));
    assertTrue(record.getField("description").toString().contains("quoted"));
}

@Test
void extract_LargeCSV_HandlesEfficiently() throws Exception {
    Path csvFile = tempDir.resolve("large.csv");
    StringBuilder csvContent = new StringBuilder("email,name,age\n");
    for (int i = 0; i < 1000; i++) {
        csvContent.append(String.format("user%d@example.com,User %d,%d\n", i, i, 20 + (i % 50)));
    }
    Files.writeString(csvFile, csvContent.toString());

    Map<String, Object> config = new HashMap<>();
    config.put("filePath", csvFile.toString());
    testSource.setConfig(config);

    long startTime = System.currentTimeMillis();
    List<UnifiedRecord> records = adapter.extract(testSource);
    long endTime = System.currentTimeMillis();

    assertNotNull(records);
    assertEquals(1000, records.size());
    assertTrue((endTime - startTime) < 5000, "Should process 1000 records in under 5 seconds");
}

@Test
void extract_CSVWithSpecialCharacters_HandlesCorrectly() throws Exception {
    Path csvFile = tempDir.resolve("special.csv");
    String csvContent = "email,name,symbol\n" +
            "test@example.com,Üser Ñame,€£¥\n";
    Files.writeString(csvFile, csvContent);

    Map<String, Object> config = new HashMap<>();
    config.put("filePath", csvFile.toString());
    testSource.setConfig(config);

    List<UnifiedRecord> records = adapter.extract(testSource);

    assertNotNull(records);
    assertEquals(1, records.size());

    UnifiedRecord record = records.get(0);
    assertEquals("test@example.com", record.getField("email"));

    assertNotNull(record.getField("name"));
    assertNotNull(record.getField("symbol"));
}
}