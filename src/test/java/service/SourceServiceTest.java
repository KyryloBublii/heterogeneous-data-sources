package service;

import org.example.models.dto.ConnectionTestRequest;
import org.example.models.dto.SourceDTO;
import org.example.models.dto.TableSchemaResponse;
import org.example.models.dto.TableSelectionDTO;
import org.example.models.entity.ApplicationUser;
import org.example.models.entity.Dataset;
import org.example.models.entity.IngestionRun;
import org.example.models.entity.IntegrationConnection;
import org.example.models.entity.Source;
import org.example.models.enums.RunStatus;
import org.example.models.enums.SourceRole;
import org.example.models.enums.SourceStatus;
import org.example.models.enums.SourceType;
import org.example.repository.DatasetRepository;
import org.example.repository.IngestionRunRepository;
import org.example.repository.IntegrationConnectionRepository;
import org.example.repository.SourceRepository;
import org.example.repository.UserRepository;
import org.example.service.FileStorageService;
import org.example.service.SourceService;
import org.example.service.ingestion.IngestionService;
import org.example.utils.DatabaseConnector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SourceServiceTest {

    @Mock
    private SourceRepository sourceRepository;

    @Mock
    private IngestionRunRepository ingestionRunRepository;

    @Mock
    private IngestionService ingestionService;

    @Mock
    private FileStorageService fileStorageService;

    @Mock
    private DatabaseConnector databaseConnector;

    @Mock
    private UserRepository userRepository;

    @Mock
    private IntegrationConnectionRepository connectionRepository;

    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private Connection mockConnection;

    @Mock
    private DatabaseMetaData mockMetaData;

    @Mock
    private ResultSet mockTableResultSet;

    @Mock
    private ResultSet mockColumnResultSet;

    @InjectMocks
    private SourceService sourceService;

    private ApplicationUser testUser;
    private Dataset testDataset;
    private Source testSource;
    private Source testDestination;
    private IngestionRun testIngestionRun;
    private IntegrationConnection testConnection;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        // Set up test user
        testUser = new ApplicationUser();
        testUser.setId(1L);
        testUser.setEmail("test@example.com");
        testUser.setName("Test User");

        // Set up test dataset
        testDataset = new Dataset();
        testDataset.setId(1L);
        testDataset.setDatasetUid("dataset-uid");
        testDataset.setName("Test Dataset");
        testDataset.setApplicationUser(testUser);
        testDataset.setCreatedAt(Instant.now());

        // Set up test source (CSV)
        testSource = new Source();
        testSource.setId(1L);
        testSource.setSourceUid("source-uid-123");
        testSource.setName("Test CSV Source");
        testSource.setType(SourceType.CSV);
        testSource.setRole(SourceRole.SOURCE);
        testSource.setStatus(SourceStatus.ACTIVE);
        testSource.setApplicationUser(testUser);
        testSource.setDataset(testDataset);
        testSource.setCreatedAt(Instant.now());
        testSource.setUpdatedAt(Instant.now());
        Map<String, Object> csvConfig = new HashMap<>();
        csvConfig.put("filePath", "/test/path/file.csv");
        csvConfig.put("delimiter", ",");
        testSource.setConfig(csvConfig);

        // Set up test destination
        testDestination = new Source();
        testDestination.setId(2L);
        testDestination.setSourceUid("dest-uid-456");
        testDestination.setName("Test Destination");
        testDestination.setType(SourceType.CSV);
        testDestination.setRole(SourceRole.DESTINATION);
        testDestination.setStatus(SourceStatus.ACTIVE);
        testDestination.setApplicationUser(testUser);
        testDestination.setDataset(testDataset);
        testDestination.setCreatedAt(Instant.now());
        testDestination.setUpdatedAt(Instant.now());

        // Set up test ingestion run
        testIngestionRun = new IngestionRun();
        testIngestionRun.setId(1L);
        testIngestionRun.setIngestionUid("ingestion-uid-789");
        testIngestionRun.setSource(testSource);
        testIngestionRun.setDestination(testDestination);
        testIngestionRun.setDataset(testDataset);
        testIngestionRun.setRunStatus(RunStatus.QUEUED);
        testIngestionRun.setStartedAt(Instant.now());

        // Set up test connection
        testConnection = new IntegrationConnection();
        testConnection.setId(1L);
        testConnection.setConnectionUid("connection-uid");
        testConnection.setSource(testSource);
        testConnection.setDestination(testDestination);
        testConnection.setDataset(testDataset);
        testConnection.setCreatedAt(Instant.now());
    }

    // ==================== listReusableSources Tests ====================

    @Test
    void shouldReturnReusableSourcesWhenValidUserProvided() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findAllByApplicationUser_Email("test@example.com"))
                .thenReturn(List.of(testSource));

        List<Source> result = sourceService.listReusableSources("test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("source-uid-123", result.get(0).getSourceUid());
        verify(userRepository).findByEmail("test@example.com");
        verify(sourceRepository).findAllByApplicationUser_Email("test@example.com");
    }

    @Test
    void shouldThrowExceptionWhenUserNotFoundForReusableSources() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.listReusableSources("test@example.com")
        );

        verify(sourceRepository, never()).findAllByApplicationUser_Email(anyString());
    }

    @Test
    void shouldThrowExceptionWhenUserEmailIsNullForReusableSources() {
        assertThrows(IllegalArgumentException.class, () ->
                sourceService.listReusableSources(null)
        );

        verify(userRepository, never()).findByEmail(anyString());
    }

    @Test
    void shouldThrowExceptionWhenUserEmailIsEmptyForReusableSources() {
        assertThrows(IllegalArgumentException.class, () ->
                sourceService.listReusableSources("")
        );

        verify(userRepository, never()).findByEmail(anyString());
    }

    // ==================== getAllSources Tests ====================

    @Test
    void shouldReturnAllSourcesWhenNoFiltersProvided() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findAllByApplicationUser_Email("test@example.com"))
                .thenReturn(List.of(testSource, testDestination));

        List<Source> result = sourceService.getAllSources(null, null, "test@example.com");

        assertNotNull(result);
        assertEquals(2, result.size());
        verify(sourceRepository).findAllByApplicationUser_Email("test@example.com");
    }

    @Test
    void shouldReturnSourcesFilteredByRole() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findAllByApplicationUser_EmailAndRole("test@example.com", SourceRole.SOURCE))
                .thenReturn(List.of(testSource));

        List<Source> result = sourceService.getAllSources(SourceRole.SOURCE, null, "test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(SourceRole.SOURCE, result.get(0).getRole());
        verify(sourceRepository).findAllByApplicationUser_EmailAndRole("test@example.com", SourceRole.SOURCE);
    }

    @Test
    void shouldReturnSourcesFilteredByDataset() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findAllByDataset_IdAndApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(List.of(testSource));

        List<Source> result = sourceService.getAllSources(null, 1L, "test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        verify(sourceRepository).findAllByDataset_IdAndApplicationUser_Email(1L, "test@example.com");
    }

    @Test
    void shouldReturnSourcesFilteredByDatasetAndRole() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findAllByDataset_IdAndApplicationUser_EmailAndRole(
                1L, "test@example.com", SourceRole.SOURCE))
                .thenReturn(List.of(testSource));

        List<Source> result = sourceService.getAllSources(SourceRole.SOURCE, 1L, "test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        verify(sourceRepository).findAllByDataset_IdAndApplicationUser_EmailAndRole(
                1L, "test@example.com", SourceRole.SOURCE);
    }

    // ==================== getSourceById Tests ====================

    @Test
    void shouldReturnSourceWhenValidIdAndUserProvided() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com"))
                .thenReturn(Optional.of(testSource));

        Source result = sourceService.getSourceById("source-uid-123", "test@example.com");

        assertNotNull(result);
        assertEquals("source-uid-123", result.getSourceUid());
        verify(sourceRepository).findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com");
    }

    @Test
    void shouldThrowExceptionWhenSourceNotFound() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("non-existent", "test@example.com"))
                .thenReturn(Optional.empty());

        assertThrows(RuntimeException.class, () ->
                sourceService.getSourceById("non-existent", "test@example.com")
        );
    }

    // ==================== createSource Tests ====================

    @Test
    void shouldCreateCsvSourceSuccessfully() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(Optional.of(testDataset));
        when(sourceRepository.save(any(Source.class))).thenReturn(testSource);

        Map<String, Object> config = new HashMap<>();
        config.put("filePath", "/test/file.csv");

        SourceDTO dto = new SourceDTO(
                null,                // id
                "Test Source",       // name
                SourceType.CSV,      // type
                SourceRole.SOURCE,   // role
                config,              // config
                SourceStatus.ACTIVE, // status
                null,                // createdAt
                null,                // updatedAt
                1L                   // datasetId
        );

        Source result = sourceService.createSource(dto, "test@example.com");

        assertNotNull(result);
        verify(userRepository).findByEmail("test@example.com");
        verify(datasetRepository).findByIdAndApplicationUser_Email(1L, "test@example.com");

        ArgumentCaptor<Source> sourceCaptor = ArgumentCaptor.forClass(Source.class);
        verify(sourceRepository).save(sourceCaptor.capture());

        Source savedSource = sourceCaptor.getValue();
        assertNotNull(savedSource.getSourceUid());
        assertEquals("Test Source", savedSource.getName());
        assertEquals(SourceType.CSV, savedSource.getType());
        assertEquals(SourceRole.SOURCE, savedSource.getRole());
        assertEquals(SourceStatus.ACTIVE, savedSource.getStatus());
        assertEquals(testUser, savedSource.getApplicationUser());
        assertEquals(testDataset, savedSource.getDataset());
        assertNotNull(savedSource.getCreatedAt());
        assertNotNull(savedSource.getUpdatedAt());
    }

    @Test
    void shouldCreateDatabaseSourceSuccessfullyWithJdbcUrl() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.save(any(Source.class))).thenReturn(testSource);

        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("port", 5432);
        config.put("database", "testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");

        SourceDTO dto = new SourceDTO(
                null,                // id
                "Test DB Source",    // name
                SourceType.DB,       // type
                SourceRole.SOURCE,   // role
                config,              // config
                SourceStatus.ACTIVE, // status
                null,                // createdAt
                null,                // updatedAt
                null                 // datasetId
        );

        Source result = sourceService.createSource(dto, "test@example.com");

        assertNotNull(result);

        ArgumentCaptor<Source> sourceCaptor = ArgumentCaptor.forClass(Source.class);
        verify(sourceRepository).save(sourceCaptor.capture());

        Source savedSource = sourceCaptor.getValue();
        assertNotNull(savedSource.getConfig());
        assertEquals("jdbc:postgresql://localhost:5432/testdb", savedSource.getConfig().get("jdbcUrl"));
        assertEquals("testuser", savedSource.getConfig().get("username"));
        assertEquals("testpass", savedSource.getConfig().get("password"));
    }

    @Test
    void shouldThrowExceptionWhenSourceNameIsNull() {
        SourceDTO dto = new SourceDTO(
                null,            // id
                null,            // name
                SourceType.CSV,  // type
                SourceRole.SOURCE, // role
                null,            // config
                null,            // status
                null,            // createdAt
                null,            // updatedAt
                null             // datasetId
        );

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.createSource(dto, "test@example.com")
        );

        verify(sourceRepository, never()).save(any());
    }

    @Test
    void shouldThrowExceptionWhenSourceNameIsEmpty() {
        SourceDTO dto = new SourceDTO(
                null,            // id
                "   ",           // name
                SourceType.CSV,  // type
                SourceRole.SOURCE, // role
                null,            // config
                null,            // status
                null,            // createdAt
                null,            // updatedAt
                null             // datasetId
        );

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.createSource(dto, "test@example.com")
        );

        verify(sourceRepository, never()).save(any());
    }

    @Test
    void shouldThrowExceptionWhenSourceTypeIsNull() {
        SourceDTO dto = new SourceDTO(
                null,            // id
                "Test Source",   // name
                null,            // type
                SourceRole.SOURCE, // role
                null,            // config
                null,            // status
                null,            // createdAt
                null,            // updatedAt
                null             // datasetId
        );

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.createSource(dto, "test@example.com")
        );

        verify(sourceRepository, never()).save(any());
    }

    @Test
    void shouldDefaultToSourceRoleWhenRoleIsNull() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.save(any(Source.class))).thenReturn(testSource);

        SourceDTO dto = new SourceDTO(
                null,            // id
                "Test Source",   // name
                SourceType.CSV,  // type
                null,            // role
                null,            // config
                null,            // status
                null,            // createdAt
                null,            // updatedAt
                null             // datasetId
        );

        sourceService.createSource(dto, "test@example.com");

        ArgumentCaptor<Source> sourceCaptor = ArgumentCaptor.forClass(Source.class);
        verify(sourceRepository).save(sourceCaptor.capture());

        Source savedSource = sourceCaptor.getValue();
        assertEquals(SourceRole.SOURCE, savedSource.getRole());
    }

    @Test
    void shouldThrowExceptionWhenDatasetNotFound() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Email(999L, "test@example.com"))
                .thenReturn(Optional.empty());

        SourceDTO dto = new SourceDTO(
                null,            // id
                "Test Source",   // name
                SourceType.CSV,  // type
                SourceRole.SOURCE, // role
                null,            // config
                null,            // status
                null,            // createdAt
                null,            // updatedAt
                999L             // datasetId
        );

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.createSource(dto, "test@example.com")
        );

        verify(sourceRepository, never()).save(any());
    }

    @Test
    void shouldThrowExceptionWhenDatabaseConfigIsMissingHost() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));

        Map<String, Object> config = new HashMap<>();
        config.put("database", "testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");

        SourceDTO dto = new SourceDTO(
                null,            // id
                "Test DB Source", // name
                SourceType.DB,   // type
                SourceRole.SOURCE, // role
                config,          // config
                null,            // status
                null,            // createdAt
                null,            // updatedAt
                null             // datasetId
        );

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.createSource(dto, "test@example.com")
        );

        verify(sourceRepository, never()).save(any());
    }

    @Test
    void shouldThrowExceptionWhenDatabaseConfigIsMissingDatabase() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));

        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("username", "testuser");
        config.put("password", "testpass");

        SourceDTO dto = new SourceDTO(
                null,            // id
                "Test DB Source", // name
                SourceType.DB,   // type
                SourceRole.SOURCE, // role
                config,          // config
                null,            // status
                null,            // createdAt
                null,            // updatedAt
                null             // datasetId
        );

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.createSource(dto, "test@example.com")
        );

        verify(sourceRepository, never()).save(any());
    }

    @Test
    void shouldThrowExceptionWhenDatabaseConfigIsMissingUsername() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));

        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("database", "testdb");
        config.put("password", "testpass");

        SourceDTO dto = new SourceDTO(
                null,            // id
                "Test DB Source", // name
                SourceType.DB,   // type
                SourceRole.SOURCE, // role
                config,          // config
                null,            // status
                null,            // createdAt
                null,            // updatedAt
                null             // datasetId
        );

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.createSource(dto, "test@example.com")
        );

        verify(sourceRepository, never()).save(any());
    }

    @Test
    void shouldThrowExceptionWhenDatabaseConfigIsMissingPassword() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));

        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("database", "testdb");
        config.put("username", "testuser");

        SourceDTO dto = new SourceDTO(
                null,            // id
                "Test DB Source", // name
                SourceType.DB,   // type
                SourceRole.SOURCE, // role
                config,          // config
                null,            // status
                null,            // createdAt
                null,            // updatedAt
                null             // datasetId
        );

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.createSource(dto, "test@example.com")
        );

        verify(sourceRepository, never()).save(any());
    }

    @Test
    void shouldPrepareDestinationFileForCsvDestination() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.save(any(Source.class))).thenReturn(testDestination);

        FileStorageService.StoredFileMetadata metadata = new FileStorageService.StoredFileMetadata(
                tempDir.resolve("output.csv"),
                tempDir.resolve("output.csv"),
                "output.csv",
                "output.csv",
                "output.csv",
                ".csv",
                0L,
                null,
                "destinations/dest-uid/output.csv"
        );

        when(fileStorageService.prepareDestinationFile(anyString(), anyString(), eq(SourceType.CSV)))
                .thenReturn(metadata);

        Map<String, Object> config = new HashMap<>();
        config.put("fileName", "output.csv");

        SourceDTO dto = new SourceDTO(
                null,                 // id
                "Test Destination",   // name
                SourceType.CSV,       // type
                SourceRole.DESTINATION, // role
                config,               // config
                null,                 // status
                null,                 // createdAt
                null,                 // updatedAt
                null                  // datasetId
        );

        sourceService.createSource(dto, "test@example.com");

        verify(fileStorageService).prepareDestinationFile(anyString(), eq("output.csv"), eq(SourceType.CSV));

        ArgumentCaptor<Source> sourceCaptor = ArgumentCaptor.forClass(Source.class);
        verify(sourceRepository).save(sourceCaptor.capture());

        Source savedSource = sourceCaptor.getValue();
        assertNotNull(savedSource.getConfig().get("filePath"));
        assertNotNull(savedSource.getConfig().get("relativePath"));
        assertNotNull(savedSource.getConfig().get("storedFilename"));
    }

    // ==================== updateSource Tests ====================

    @Test
    void shouldUpdateSourceSuccessfully() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com"))
                .thenReturn(Optional.of(testSource));
        when(sourceRepository.save(any(Source.class))).thenReturn(testSource);

        Map<String, Object> newConfig = new HashMap<>();
        newConfig.put("delimiter", ";");

        SourceDTO dto = new SourceDTO(
                null,                  // id
                "Updated Source Name", // name
                SourceType.CSV,        // type
                SourceRole.SOURCE,     // role
                newConfig,             // config
                SourceStatus.PAUSED,   // status
                null,                  // createdAt
                null,                  // updatedAt
                null                   // datasetId
        );

        Source result = sourceService.updateSource("source-uid-123", dto, "test@example.com");

        assertNotNull(result);

        ArgumentCaptor<Source> sourceCaptor = ArgumentCaptor.forClass(Source.class);
        verify(sourceRepository).save(sourceCaptor.capture());

        Source updatedSource = sourceCaptor.getValue();
        assertEquals("Updated Source Name", updatedSource.getName());
        assertEquals(SourceStatus.PAUSED, updatedSource.getStatus());
        assertNotNull(updatedSource.getUpdatedAt());
    }

    @Test
    void shouldUpdateOnlyProvidedFieldsInSource() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com"))
                .thenReturn(Optional.of(testSource));
        when(sourceRepository.save(any(Source.class))).thenReturn(testSource);

        SourceDTO dto = new SourceDTO(
                null,                // id
                "Updated Name Only", // name
                null,                // type
                null,                // role
                null,                // config
                null,                // status
                null,                // createdAt
                null,                // updatedAt
                null                 // datasetId
        );

        sourceService.updateSource("source-uid-123", dto, "test@example.com");

        ArgumentCaptor<Source> sourceCaptor = ArgumentCaptor.forClass(Source.class);
        verify(sourceRepository).save(sourceCaptor.capture());

        Source updatedSource = sourceCaptor.getValue();
        assertEquals("Updated Name Only", updatedSource.getName());
        assertEquals(SourceType.CSV, updatedSource.getType()); // Unchanged
        assertEquals(SourceRole.SOURCE, updatedSource.getRole()); // Unchanged
    }

    @Test
    void shouldUpdateDatasetWhenProvided() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com"))
                .thenReturn(Optional.of(testSource));

        Dataset newDataset = new Dataset();
        newDataset.setId(2L);
        newDataset.setDatasetUid("new-dataset-uid");
        newDataset.setApplicationUser(testUser);

        when(datasetRepository.findByIdAndApplicationUser_Email(2L, "test@example.com"))
                .thenReturn(Optional.of(newDataset));
        when(sourceRepository.save(any(Source.class))).thenReturn(testSource);

        SourceDTO dto = new SourceDTO(
                null,  // id
                null,  // name
                null,  // type
                null,  // role
                null,  // config
                null,  // status
                null,  // createdAt
                null,  // updatedAt
                2L     // datasetId
        );

        sourceService.updateSource("source-uid-123", dto, "test@example.com");

        ArgumentCaptor<Source> sourceCaptor = ArgumentCaptor.forClass(Source.class);
        verify(sourceRepository).save(sourceCaptor.capture());

        Source updatedSource = sourceCaptor.getValue();
        assertEquals(newDataset, updatedSource.getDataset());
    }

    // ==================== deleteSource Tests ====================

    @Test
    void shouldDeleteSourceSuccessfully() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com"))
                .thenReturn(Optional.of(testSource));

        sourceService.deleteSource("source-uid-123", "test@example.com");

        verify(sourceRepository).delete(testSource);
    }

    @Test
    void shouldThrowExceptionWhenDeletingNonExistentSource() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("non-existent", "test@example.com"))
                .thenReturn(Optional.empty());

        assertThrows(RuntimeException.class, () ->
                sourceService.deleteSource("non-existent", "test@example.com")
        );

        verify(sourceRepository, never()).delete(any());
    }

    // ==================== assignSourceToDataset Tests ====================

    @Test
    void shouldAssignSourceToDatasetSuccessfully() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com"))
                .thenReturn(Optional.of(testSource));
        when(datasetRepository.findByIdAndApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(Optional.of(testDataset));
        when(sourceRepository.save(any(Source.class))).thenReturn(testSource);

        Source result = sourceService.assignSourceToDataset("source-uid-123", 1L, "test@example.com");

        assertNotNull(result);
        verify(sourceRepository).save(testSource);
    }

    @Test
    void shouldUnassignSourceFromDatasetWhenDatasetIdIsNull() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com"))
                .thenReturn(Optional.of(testSource));
        when(sourceRepository.save(any(Source.class))).thenReturn(testSource);

        sourceService.assignSourceToDataset("source-uid-123", null, "test@example.com");

        ArgumentCaptor<Source> sourceCaptor = ArgumentCaptor.forClass(Source.class);
        verify(sourceRepository).save(sourceCaptor.capture());

        Source updatedSource = sourceCaptor.getValue();
        assertNull(updatedSource.getDataset());
    }

    @Test
    void shouldCloneSourceWhenAssigningToDifferentDataset() {
        Source sourceWithDataset = new Source();
        sourceWithDataset.setId(1L);
        sourceWithDataset.setSourceUid("source-uid-123");
        sourceWithDataset.setName("Test Source");
        sourceWithDataset.setType(SourceType.CSV);
        sourceWithDataset.setRole(SourceRole.SOURCE);
        sourceWithDataset.setStatus(SourceStatus.ACTIVE);
        sourceWithDataset.setApplicationUser(testUser);
        sourceWithDataset.setDataset(testDataset);
        sourceWithDataset.setConfig(new HashMap<>());

        Dataset differentDataset = new Dataset();
        differentDataset.setId(2L);
        differentDataset.setDatasetUid("different-dataset-uid");
        differentDataset.setApplicationUser(testUser);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com"))
                .thenReturn(Optional.of(sourceWithDataset));
        when(datasetRepository.findByIdAndApplicationUser_Email(2L, "test@example.com"))
                .thenReturn(Optional.of(differentDataset));

        Source clonedSource = new Source();
        clonedSource.setSourceUid("new-source-uid");
        when(sourceRepository.save(any(Source.class))).thenReturn(clonedSource);

        Source result = sourceService.assignSourceToDataset("source-uid-123", 2L, "test@example.com");

        assertNotNull(result);
        verify(sourceRepository).save(any(Source.class));
    }

    // ==================== triggerIngestion Tests ====================

    @Test
    void shouldTriggerIngestionSuccessfully() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com"))
                .thenReturn(Optional.of(testSource));
        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(testIngestionRun);

        IngestionRun result = sourceService.triggerIngestion("source-uid-123", "test@example.com");

        assertNotNull(result);

        ArgumentCaptor<IngestionRun> runCaptor = ArgumentCaptor.forClass(IngestionRun.class);
        verify(ingestionRunRepository).save(runCaptor.capture());

        IngestionRun savedRun = runCaptor.getValue();
        assertNotNull(savedRun.getIngestionUid());
        assertEquals(testSource, savedRun.getSource());
        assertEquals(RunStatus.QUEUED, savedRun.getRunStatus());
        assertNotNull(savedRun.getStartedAt());

        verify(ingestionService).startIngestionAsync(anyLong());
    }

    @Test
    void shouldTriggerIngestionWithTableSelectionsForDatabaseSource() {
        Source dbSource = new Source();
        dbSource.setId(1L);
        dbSource.setSourceUid("db-source-uid");
        dbSource.setType(SourceType.DB);
        dbSource.setApplicationUser(testUser);
        dbSource.setDataset(testDataset);

        List<TableSelectionDTO> selections = List.of(
                new TableSelectionDTO("users", "public", List.of("id", "name", "email"))
        );

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(testIngestionRun);

        sourceService.triggerIngestion(dbSource, null, selections);

        verify(ingestionRunRepository).save(any(IngestionRun.class));
        verify(ingestionService).startIngestionAsync(anyLong(), anyMap());
    }

    @Test
    void shouldLoadStoredSelectionsFromConnectionWhenNoSelectionsProvided() {
        Source dbSource = new Source();
        dbSource.setId(1L);
        dbSource.setSourceUid("db-source-uid");
        dbSource.setType(SourceType.DB);
        dbSource.setApplicationUser(testUser);

        List<Map<String, Object>> storedSelections = new ArrayList<>();
        Map<String, Object> tableSelection = new HashMap<>();
        tableSelection.put("tableName", "users");
        tableSelection.put("schema", "public");
        tableSelection.put("columns", List.of("id", "name"));
        storedSelections.add(tableSelection);

        testConnection.setTableSelection(storedSelections);

        when(connectionRepository.findFirstBySource_IdOrderByCreatedAtDesc(1L))
                .thenReturn(Optional.of(testConnection));
        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(testIngestionRun);

        sourceService.triggerIngestion(dbSource, null, null);

        verify(connectionRepository).findFirstBySource_IdOrderByCreatedAtDesc(1L);
        verify(ingestionRunRepository).save(any(IngestionRun.class));
        verify(ingestionService).startIngestionAsync(anyLong(), anyMap());
    }

    // ==================== getSourceRuns Tests ====================

    @Test
    void shouldReturnSourceRunsSuccessfully() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("source-uid-123", "test@example.com"))
                .thenReturn(Optional.of(testSource));
        when(ingestionRunRepository.findBySourceOrderByStartedAtDesc(testSource))
                .thenReturn(List.of(testIngestionRun));

        List<IngestionRun> result = sourceService.getSourceRuns("source-uid-123", "test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("ingestion-uid-789", result.get(0).getIngestionUid());
        verify(ingestionRunRepository).findBySourceOrderByStartedAtDesc(testSource);
    }

    // ==================== testConnection Tests ====================

    @Test
    void shouldReturnTrueForNonDatabaseConnection() throws SQLException {
        Map<String, Object> config = new HashMap<>();

        boolean result = sourceService.testConnection(SourceType.CSV, config);

        assertTrue(result);
        verify(databaseConnector, never()).getConnection(anyString(), anyString(), anyString());
    }

    @Test
    void shouldTestDatabaseConnectionSuccessfully() throws SQLException {
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("port", 5432);
        config.put("database", "testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");

        when(databaseConnector.getConnection(
                "jdbc:postgresql://localhost:5432/testdb",
                "testuser",
                "testpass"
        )).thenReturn(mockConnection);

        boolean result = sourceService.testConnection(SourceType.DB, config);

        assertTrue(result);
        verify(databaseConnector).getConnection(
                "jdbc:postgresql://localhost:5432/testdb",
                "testuser",
                "testpass"
        );
        verify(mockConnection).close();
    }

    @Test
    void shouldThrowExceptionWhenDatabaseConnectionFails() throws SQLException {
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("port", 5432);
        config.put("database", "testdb");
        config.put("username", "testuser");
        config.put("password", "wrongpass");

        when(databaseConnector.getConnection(anyString(), anyString(), anyString()))
                .thenThrow(new SQLException("Authentication failed"));

        assertThrows(IllegalStateException.class, () ->
                sourceService.testConnection(SourceType.DB, config)
        );
    }

    @Test
    void shouldDefaultToPort5432WhenPortNotProvided() throws SQLException {
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("database", "testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");

        when(databaseConnector.getConnection(
                "jdbc:postgresql://localhost:5432/testdb",
                "testuser",
                "testpass"
        )).thenReturn(mockConnection);

        sourceService.testConnection(SourceType.DB, config);

        verify(databaseConnector).getConnection(
                "jdbc:postgresql://localhost:5432/testdb",
                "testuser",
                "testpass"
        );
    }

    // ==================== describeSourceSchema Tests ====================

    @Test
    void shouldReturnFileSchemaForCsvSource() {
        Source csvSource = new Source();
        csvSource.setSourceUid("csv-source");
        csvSource.setName("Test CSV");
        csvSource.setType(SourceType.CSV);
        csvSource.setApplicationUser(testUser);
        Map<String, Object> config = new HashMap<>();
        config.put("displayFilename", "test.csv");
        csvSource.setConfig(config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("csv-source", "test@example.com"))
                .thenReturn(Optional.of(csvSource));

        List<TableSchemaResponse> result = sourceService.describeSourceSchema("csv-source", "test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("test.csv", result.get(0).tableName());
    }

    @Test
    void shouldDescribeDatabaseSchemaSuccessfully() throws SQLException {
        Source dbSource = new Source();
        dbSource.setSourceUid("db-source");
        dbSource.setType(SourceType.DB);
        dbSource.setApplicationUser(testUser);
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:postgresql://localhost:5432/testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");
        dbSource.setConfig(config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("db-source", "test@example.com"))
                .thenReturn(Optional.of(dbSource));
        when(databaseConnector.getConnection("jdbc:postgresql://localhost:5432/testdb", "testuser", "testpass"))
                .thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.getCatalog()).thenReturn("testdb");
        when(mockMetaData.getTables(eq("testdb"), isNull(), eq("%"), any(String[].class)))
                .thenReturn(mockTableResultSet);

        // Mock table result set
        when(mockTableResultSet.next()).thenReturn(true, false);
        when(mockTableResultSet.getString("TABLE_SCHEM")).thenReturn("public");
        when(mockTableResultSet.getString("TABLE_NAME")).thenReturn("users");

        // Mock column result set
        when(mockMetaData.getColumns(eq("testdb"), eq("public"), eq("users"), eq("%")))
                .thenReturn(mockColumnResultSet);
        when(mockColumnResultSet.next()).thenReturn(true, true, false);
        when(mockColumnResultSet.getString("COLUMN_NAME")).thenReturn("id", "name");
        when(mockColumnResultSet.getString("TYPE_NAME")).thenReturn("int4", "varchar");
        when(mockColumnResultSet.getString("IS_NULLABLE")).thenReturn("NO", "YES");

        List<TableSchemaResponse> result = sourceService.describeSourceSchema("db-source", "test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("users", result.get(0).tableName());
        assertEquals("public", result.get(0).schema());
        assertEquals(2, result.get(0).columns().size());

        verify(mockConnection).close();
    }

    @Test
    void shouldFilterOutSystemSchemas() throws SQLException {
        Source dbSource = new Source();
        dbSource.setSourceUid("db-source");
        dbSource.setType(SourceType.DB);
        dbSource.setApplicationUser(testUser);
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:postgresql://localhost:5432/testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");
        dbSource.setConfig(config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("db-source", "test@example.com"))
                .thenReturn(Optional.of(dbSource));
        when(databaseConnector.getConnection("jdbc:postgresql://localhost:5432/testdb", "testuser", "testpass"))
                .thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.getCatalog()).thenReturn("testdb");
        when(mockMetaData.getTables(eq("testdb"), isNull(), eq("%"), any(String[].class)))
                .thenReturn(mockTableResultSet);

        // Mock table result set with system schema
        // First row (pg_catalog.pg_tables) will be filtered out before TABLE_NAME is read
        // Second row (public.users) will be included
        when(mockTableResultSet.next()).thenReturn(true, true, false);
        when(mockTableResultSet.getString("TABLE_SCHEM")).thenReturn("pg_catalog", "public");
        when(mockTableResultSet.getString("TABLE_NAME")).thenReturn("users"); // Only called once for public schema

        // Mock column result set for public.users only (system tables should be filtered out)
        when(mockMetaData.getColumns(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(mockColumnResultSet);
        when(mockColumnResultSet.next()).thenReturn(false);

        List<TableSchemaResponse> result = sourceService.describeSourceSchema("db-source", "test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("public", result.get(0).schema());
        assertEquals("users", result.get(0).tableName());
    }

    @Test
    void shouldThrowExceptionWhenDatabaseSchemaInspectionFails() throws SQLException {
        Source dbSource = new Source();
        dbSource.setSourceUid("db-source");
        dbSource.setType(SourceType.DB);
        dbSource.setApplicationUser(testUser);
        Map<String, Object> config = new HashMap<>();
        config.put("jdbcUrl", "jdbc:postgresql://localhost:5432/testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");
        dbSource.setConfig(config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("db-source", "test@example.com"))
                .thenReturn(Optional.of(dbSource));
        when(databaseConnector.getConnection("jdbc:postgresql://localhost:5432/testdb", "testuser", "testpass"))
                .thenThrow(new SQLException("Connection failed"));

        assertThrows(IllegalStateException.class, () ->
                sourceService.describeSourceSchema("db-source", "test@example.com")
        );
    }

    // ==================== discoverSchema Tests ====================

    @Test
    void shouldDiscoverSchemaForDatabaseRequest() throws SQLException {
        Map<String, Object> config = new HashMap<>();
        config.put("host", "localhost");
        config.put("port", 5432);
        config.put("database", "testdb");
        config.put("username", "testuser");
        config.put("password", "testpass");

        ConnectionTestRequest request = new ConnectionTestRequest(SourceType.DB, config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(databaseConnector.getConnection("jdbc:postgresql://localhost:5432/testdb", "testuser", "testpass"))
                .thenReturn(mockConnection);
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockConnection.getCatalog()).thenReturn("testdb");
        when(mockMetaData.getTables(eq("testdb"), isNull(), eq("%"), any(String[].class)))
                .thenReturn(mockTableResultSet);

        when(mockTableResultSet.next()).thenReturn(true, false);
        when(mockTableResultSet.getString("TABLE_SCHEM")).thenReturn("public");
        when(mockTableResultSet.getString("TABLE_NAME")).thenReturn("customers");

        when(mockMetaData.getColumns(eq("testdb"), eq("public"), eq("customers"), eq("%")))
                .thenReturn(mockColumnResultSet);
        when(mockColumnResultSet.next()).thenReturn(false);

        List<TableSchemaResponse> result = sourceService.discoverSchema(request, "test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("customers", result.get(0).tableName());

        verify(mockConnection).close();
    }

    @Test
    void shouldThrowExceptionWhenDiscoverSchemaWithNonDatabaseType() {
        ConnectionTestRequest request = new ConnectionTestRequest(SourceType.CSV, null);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.discoverSchema(request, "test@example.com")
        );
    }

    @Test
    void shouldThrowExceptionWhenDiscoverSchemaWithNullRequest() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));

        assertThrows(IllegalArgumentException.class, () ->
                sourceService.discoverSchema(null, "test@example.com")
        );
    }

    // ==================== parseCsvColumns Tests ====================

    @Test
    void shouldParseCsvColumnsSuccessfully() throws IOException {
        Path csvFile = tempDir.resolve("test.csv");
        Files.writeString(csvFile, "id,name,email\n1,John,john@example.com\n2,Jane,jane@example.com");

        Source csvSource = new Source();
        csvSource.setSourceUid("csv-source");
        csvSource.setType(SourceType.CSV);
        csvSource.setApplicationUser(testUser);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("displayFilename", "test.csv");
        csvSource.setConfig(config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("csv-source", "test@example.com"))
                .thenReturn(Optional.of(csvSource));

        List<TableSchemaResponse> result = sourceService.describeSourceSchema("csv-source", "test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        TableSchemaResponse schema = result.get(0);
        assertEquals("test.csv", schema.tableName());
        assertEquals(3, schema.columns().size());
        assertEquals("id", schema.columns().get(0).name());
        assertEquals("name", schema.columns().get(1).name());
        assertEquals("email", schema.columns().get(2).name());
    }

    @Test
    void shouldHandleQuotedCsvHeaders() throws IOException {
        Path csvFile = tempDir.resolve("quoted.csv");
        Files.writeString(csvFile, "\"First Name\",\"Last Name\",\"Email Address\"\nJohn,Doe,john@example.com");

        Source csvSource = new Source();
        csvSource.setSourceUid("csv-source");
        csvSource.setType(SourceType.CSV);
        csvSource.setApplicationUser(testUser);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("displayFilename", "quoted.csv");
        csvSource.setConfig(config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("csv-source", "test@example.com"))
                .thenReturn(Optional.of(csvSource));

        List<TableSchemaResponse> result = sourceService.describeSourceSchema("csv-source", "test@example.com");

        assertNotNull(result);
        TableSchemaResponse schema = result.get(0);
        assertEquals("First Name", schema.columns().get(0).name());
        assertEquals("Last Name", schema.columns().get(1).name());
        assertEquals("Email Address", schema.columns().get(2).name());
    }

    @Test
    void shouldHandleEmptyColumnNamesInCsv() throws IOException {
        Path csvFile = tempDir.resolve("empty-cols.csv");
        // Use non-trailing empty columns to avoid String.split(",") discarding them
        Files.writeString(csvFile, "id,,name,email,phone\n1,x,John,y,z");

        Source csvSource = new Source();
        csvSource.setSourceUid("csv-source");
        csvSource.setType(SourceType.CSV);
        csvSource.setApplicationUser(testUser);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("displayFilename", "empty-cols.csv");
        csvSource.setConfig(config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("csv-source", "test@example.com"))
                .thenReturn(Optional.of(csvSource));

        List<TableSchemaResponse> result = sourceService.describeSourceSchema("csv-source", "test@example.com");

        assertNotNull(result);
        TableSchemaResponse schema = result.get(0);
        assertEquals(5, schema.columns().size());
        assertEquals("id", schema.columns().get(0).name());
        assertEquals("column_1", schema.columns().get(1).name());
        assertEquals("name", schema.columns().get(2).name());
        assertEquals("email", schema.columns().get(3).name());
        assertEquals("phone", schema.columns().get(4).name());
    }

    @Test
    void shouldHandleBomInCsvFile() throws IOException {
        Path csvFile = tempDir.resolve("bom.csv");
        Files.writeString(csvFile, "\uFEFFid,name,email\n1,John,john@example.com");

        Source csvSource = new Source();
        csvSource.setSourceUid("csv-source");
        csvSource.setType(SourceType.CSV);
        csvSource.setApplicationUser(testUser);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("displayFilename", "bom.csv");
        csvSource.setConfig(config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("csv-source", "test@example.com"))
                .thenReturn(Optional.of(csvSource));

        List<TableSchemaResponse> result = sourceService.describeSourceSchema("csv-source", "test@example.com");

        assertNotNull(result);
        TableSchemaResponse schema = result.get(0);
        assertEquals("id", schema.columns().get(0).name());
    }

    @Test
    void shouldSkipEmptyLinesInCsv() throws IOException {
        Path csvFile = tempDir.resolve("empty-lines.csv");
        Files.writeString(csvFile, "\n\n  \nid,name,email\n1,John,john@example.com");

        Source csvSource = new Source();
        csvSource.setSourceUid("csv-source");
        csvSource.setType(SourceType.CSV);
        csvSource.setApplicationUser(testUser);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", csvFile.toString());
        config.put("displayFilename", "empty-lines.csv");
        csvSource.setConfig(config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("csv-source", "test@example.com"))
                .thenReturn(Optional.of(csvSource));

        List<TableSchemaResponse> result = sourceService.describeSourceSchema("csv-source", "test@example.com");

        assertNotNull(result);
        TableSchemaResponse schema = result.get(0);
        assertEquals(3, schema.columns().size());
        assertEquals("id", schema.columns().get(0).name());
    }

    @Test
    void shouldReturnEmptyColumnsWhenCsvFileNotFound() {
        Source csvSource = new Source();
        csvSource.setSourceUid("csv-source");
        csvSource.setType(SourceType.CSV);
        csvSource.setApplicationUser(testUser);
        Map<String, Object> config = new HashMap<>();
        config.put("filePath", "/nonexistent/file.csv");
        config.put("displayFilename", "missing.csv");
        csvSource.setConfig(config);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findBySourceUidAndApplicationUser_Email("csv-source", "test@example.com"))
                .thenReturn(Optional.of(csvSource));

        List<TableSchemaResponse> result = sourceService.describeSourceSchema("csv-source", "test@example.com");

        assertNotNull(result);
        TableSchemaResponse schema = result.get(0);
        assertEquals(0, schema.columns().size());
    }
}
