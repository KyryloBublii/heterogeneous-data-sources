package service;

import org.example.adapters.DataSourceAdapter;
import org.example.models.dto.IntegrationConfigDTO;
import org.example.models.dto.UnifiedRecord;
import org.example.models.entity.ApplicationUser;
import org.example.models.entity.Dataset;
import org.example.models.entity.Source;
import org.example.models.entity.UnifiedRow;
import org.example.models.enums.SourceRole;
import org.example.models.enums.SourceType;
import org.example.repository.DatasetRepository;
import org.example.repository.SourceRepository;
import org.example.repository.UnifiedRowRepository;
import org.example.repository.UserRepository;
import org.example.service.DataIntegrationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataIntegrationServiceTest {

    @Mock
    private List<DataSourceAdapter> adapters;

    @Mock
    private DataSourceAdapter dataSourceAdapter;

    @Mock
    private SourceRepository sourceRepository;

    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private UnifiedRowRepository unifiedRowRepository;

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private DataIntegrationService dataIntegrationService;

    private Dataset dataset;
    private Source source;
    private IntegrationConfigDTO config;
    private IntegrationConfigDTO.SourceMappingDTO sourceMapping;
    private Map<String, IntegrationConfigDTO.FieldMappingDTO> fieldMappings;

    @BeforeEach
    void setUp() {
        ApplicationUser user = new ApplicationUser();
        user.setId(5L);
        user.setEmail("user@example.com");

        dataset = new Dataset();
        dataset.setId(1L);
        dataset.setName("Test Dataset");
        dataset.setApplicationUser(user);

        source = new Source();
        source.setId(10L);
        source.setName("Test Source");
        source.setType(SourceType.CSV);
        source.setRole(SourceRole.SOURCE);
        source.setApplicationUser(user);
        source.setDataset(dataset);

        fieldMappings = new HashMap<>();
        fieldMappings.put("email", new IntegrationConfigDTO.FieldMappingDTO("email", "email", "string", null, true));

        sourceMapping = new IntegrationConfigDTO.SourceMappingDTO(10L, fieldMappings);
        config = new IntegrationConfigDTO(1L, List.of(sourceMapping));
    }

    @Test
    void shouldRunIntegrationSuccessfully() throws Exception {
        // Arrange
        UnifiedRecord record = new UnifiedRecord();
        record.setSourceIdentifier("source-1");
        record.setRecordKey("record-1");
        record.addField("email", "test@example.com");

        when(adapters.stream()).thenReturn(Stream.of(dataSourceAdapter));
        when(dataSourceAdapter.supportsSource(source)).thenReturn(true);
        when(dataSourceAdapter.extract(source)).thenReturn(List.of(record));
        when(datasetRepository.findByIdAndApplicationUser_Email(1L, "user@example.com"))
                .thenReturn(Optional.of(dataset));
        when(sourceRepository.findByIdAndApplicationUser_Email(10L, "user@example.com"))
                .thenReturn(Optional.of(source));
        when(unifiedRowRepository.save(any(UnifiedRow.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // Act
        Map<String, Object> result = dataIntegrationService.runIntegration(config, "user@example.com");

        // Assert
        assertTrue((Boolean) result.get("success"));
        assertEquals(1, result.get("recordsProcessed"));
        assertEquals(1L, result.get("datasetId"));
        assertTrue(((List<?>) result.get("errors")).isEmpty());

        verify(datasetRepository).findByIdAndApplicationUser_Email(1L, "user@example.com");
        verify(sourceRepository).findByIdAndApplicationUser_Email(10L, "user@example.com");
        verify(dataSourceAdapter).extract(source);

        ArgumentCaptor<UnifiedRow> rowCaptor = ArgumentCaptor.forClass(UnifiedRow.class);
        verify(unifiedRowRepository, times(1)).save(rowCaptor.capture());
        UnifiedRow savedRow = rowCaptor.getValue();
        assertEquals(dataset, savedRow.getDataset());
        assertEquals(source, savedRow.getSource());
        assertEquals("record-1", savedRow.getRecordKey());
    }

    @Test
    void shouldReturnErrorWhenDatasetNotFound() {
        // Arrange
        when(datasetRepository.findByIdAndApplicationUser_Email(1L, "user@example.com"))
                .thenReturn(Optional.empty());

        // Act
        Map<String, Object> result = dataIntegrationService.runIntegration(config, "user@example.com");

        // Assert
        assertFalse((Boolean) result.get("success"));
        assertEquals(0, result.get("recordsProcessed"));
        assertTrue(((List<?>) result.get("errors")).isEmpty());
        assertTrue(result.get("error").toString().contains("Dataset not found"));

        verify(sourceRepository, never()).findByIdAndApplicationUser_Email(anyLong(), anyString());
        verify(unifiedRowRepository, never()).save(any());
    }

    @Test
    void shouldCollectErrorsWhenAdapterMissing() {
        // Arrange
        when(datasetRepository.findByIdAndApplicationUser_Email(1L, "user@example.com"))
                .thenReturn(Optional.of(dataset));
        when(sourceRepository.findByIdAndApplicationUser_Email(10L, "user@example.com"))
                .thenReturn(Optional.of(source));
        when(adapters.stream()).thenReturn(Stream.empty());

        // Act
        Map<String, Object> result = dataIntegrationService.runIntegration(config, "user@example.com");

        // Assert
        assertFalse((Boolean) result.get("success"));
        assertEquals(0, result.get("recordsProcessed"));
        List<?> errors = (List<?>) result.get("errors");
        assertEquals(1, errors.size());
        assertTrue(errors.get(0).toString().contains("Source 10"));

        verify(unifiedRowRepository, never()).save(any());
    }

    @Test
    void shouldTransformRecordsWithDefaultsAndCasting() {
        // Arrange
        UnifiedRecord record = new UnifiedRecord();
        record.setRecordKey("record-2");
        record.addField("age", "30");

        IntegrationConfigDTO.FieldMappingDTO requiredDefaultMapping =
                new IntegrationConfigDTO.FieldMappingDTO("missing", "missing", "string", "fallback", true);
        IntegrationConfigDTO.FieldMappingDTO castMapping =
                new IntegrationConfigDTO.FieldMappingDTO("age", "age", "integer", null, false);

        Map<String, IntegrationConfigDTO.FieldMappingDTO> mappings = new LinkedHashMap<>();
        mappings.put("missingField", requiredDefaultMapping);
        mappings.put("age", castMapping);

        // Act
        List<UnifiedRecord> transformed = dataIntegrationService.transform(List.of(record), mappings);

        // Assert
        assertEquals(1, transformed.size());
        UnifiedRecord transformedRecord = transformed.get(0);
        assertEquals("fallback", transformedRecord.getField("missingField"));
        assertEquals(30, transformedRecord.getField("age"));
    }

    @Test
    void shouldSkipRequiredFieldWithoutDefault() {
        // Arrange
        UnifiedRecord record = new UnifiedRecord();
        record.setRecordKey("record-3");

        IntegrationConfigDTO.FieldMappingDTO requiredMapping =
                new IntegrationConfigDTO.FieldMappingDTO("unavailable", "unavailable", "string", null, true);

        Map<String, IntegrationConfigDTO.FieldMappingDTO> mappings = Map.of("requiredField", requiredMapping);

        // Act
        List<UnifiedRecord> transformed = dataIntegrationService.transform(List.of(record), mappings);

        // Assert
        assertEquals(1, transformed.size());
        assertFalse(transformed.get(0).hasField("requiredField"));
    }

    @Test
    void shouldLeaveValueUnchangedWhenCastingFails() {
        // Arrange
        UnifiedRecord record = new UnifiedRecord();
        record.setRecordKey("record-4");
        record.addField("count", "NaN");

        IntegrationConfigDTO.FieldMappingDTO mapping =
                new IntegrationConfigDTO.FieldMappingDTO("count", "count", "integer", null, false);

        Map<String, IntegrationConfigDTO.FieldMappingDTO> mappings = Map.of("count", mapping);

        // Act
        List<UnifiedRecord> transformed = dataIntegrationService.transform(List.of(record), mappings);

        // Assert
        assertEquals("NaN", transformed.get(0).getField("count"));
    }

    @Test
    void shouldContinueLoadingWhenSaveFails() {
        // Arrange
        UnifiedRecord first = new UnifiedRecord(Map.of("id", 1), "s1", "r1");
        UnifiedRecord second = new UnifiedRecord(Map.of("id", 2), "s1", "r2");

        when(unifiedRowRepository.save(any(UnifiedRow.class)))
                .thenReturn(new UnifiedRow())
                .thenThrow(new RuntimeException("DB error"));

        // Act
        int loaded = dataIntegrationService.load(List.of(first, second), dataset, source);

        // Assert
        assertEquals(1, loaded);
        verify(unifiedRowRepository, times(2)).save(any(UnifiedRow.class));
    }
}