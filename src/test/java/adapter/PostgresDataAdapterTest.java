package adapter;

import org.example.adapters.PostgresDataAdapter;
import org.example.models.entity.ApplicationUser;
import org.example.models.entity.Source;
import org.example.models.enums.SourceRole;
import org.example.models.enums.SourceStatus;
import org.example.models.enums.SourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PostgresDataAdapterTest {

    private PostgresDataAdapter adapter;
    private Source testSource;

    @BeforeEach
    void setUp() {
        adapter = new PostgresDataAdapter();
        
        ApplicationUser user = new ApplicationUser();
        user.setId(1L);
        user.setEmail("test@example.com");
        
        testSource = new Source();
        testSource.setId(1L);
        testSource.setSourceUid("db-source-uid");
        testSource.setName("Test DB Source");
        testSource.setType(SourceType.DB);
        testSource.setRole(SourceRole.SOURCE);
        testSource.setStatus(SourceStatus.ACTIVE);
        testSource.setApplicationUser(user);
        testSource.setCreatedAt(Instant.now());
    }


    @Test
    void supportsSource_NonDBType_ReturnsFalse() {
        testSource.setType(SourceType.CSV);

        boolean supports = adapter.supportsSource(testSource);

        assertFalse(supports);
    }

    @Test
    void extract_NoTableName_ThrowsException() {
        // Arrange
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("port", 5432);
        config.put("database", "testdb");
        config.put("username", "user");
        config.put("password", "pass");
        testSource.setConfig(config);

        assertThrows(IllegalArgumentException.class, () -> adapter.extract(testSource));
    }

    @Test
    void extract_InvalidConnection_ThrowsException() {
        Map<String, Object> config = new HashMap<>();
        config.put("host", "invalid-host");
        config.put("port", 5432);
        config.put("database", "testdb");
        config.put("username", "user");
        config.put("password", "pass");
        config.put("tableName", "users");
        testSource.setConfig(config);

        assertThrows(Exception.class, () -> adapter.extract(testSource));
    }


}
