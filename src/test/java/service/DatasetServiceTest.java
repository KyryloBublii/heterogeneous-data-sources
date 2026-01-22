package service;

import org.example.models.dto.*;
import org.example.models.entity.*;
import org.example.models.enums.DataType;
import org.example.models.enums.DatasetStatus;
import org.example.repository.*;
import org.example.service.DatasetService;
import org.example.service.SourceService;
import org.example.service.TransformService;
import org.example.service.ingestion.DestinationOutputService;
import org.example.service.ingestion.IngestionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DatasetServiceTest {

    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private DatasetFieldRepository datasetFieldRepository;

    @Mock
    private DatasetMappingRepository datasetMappingRepository;

    @Mock
    private UserRepository userRepository;

    @Mock
    private SourceRepository sourceRepository;

    @Mock
    private IntegrationConnectionRepository integrationConnectionRepository;

    @Mock
    private RawEventRepository rawEventRepository;

    @Mock
    private UnifiedRowRepository unifiedRowRepository;

    @Mock
    private TransformRunRepository transformRunRepository;

    @Mock
    private SourceService sourceService;

    @Mock
    private DestinationOutputService destinationOutputService;

    @Mock
    private RelationshipRepository relationshipRepository;

    @Mock
    private IngestionService ingestionService;

    @Mock
    private TransformService transformService;

    @InjectMocks
    private DatasetService datasetService;

    private ApplicationUser testUser;
    private Dataset testDataset;
    private DatasetField testField;
    private Source testSource;

    @BeforeEach
    void setUp() {
        testUser = new ApplicationUser();
        testUser.setId(1L);
        testUser.setEmail("test@example.com");
        testUser.setName("Test User");

        testDataset = new Dataset();
        testDataset.setId(1L);
        testDataset.setDatasetUid("dataset-uid");
        testDataset.setName("Test Dataset");
        testDataset.setDescription("Test Description");
        testDataset.setApplicationUser(testUser);
        testDataset.setStatus(DatasetStatus.ACTIVE);
        testDataset.setCreatedAt(Instant.now());

        testField = new DatasetField();
        testField.setId(1L);
        testField.setDatasetFieldUid("field-uid");
        testField.setName("email");
        testField.setDtype(DataType.TEXT);
        testField.setDataset(testDataset);
        testField.setPosition(0);

        testSource = new Source();
        testSource.setId(1L);
        testSource.setSourceUid("source-uid");
        testSource.setName("Test Source");
        testSource.setApplicationUser(testUser);
    }

    @Test
    void createDatasetForUser_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.save(any(Dataset.class))).thenReturn(testDataset);

        Dataset result = datasetService.createDatasetForUser(
                "Test Dataset",
                "Test Description",
                "customer",
                "test@example.com"
        );

        assertNotNull(result);
        assertEquals("Test Dataset", result.getName());
        
        ArgumentCaptor<Dataset> datasetCaptor = ArgumentCaptor.forClass(Dataset.class);
        verify(datasetRepository).save(datasetCaptor.capture());
        
        Dataset savedDataset = datasetCaptor.getValue();
        assertNotNull(savedDataset.getDatasetUid());
        assertEquals(testUser, savedDataset.getApplicationUser());
        assertEquals(DatasetStatus.ACTIVE, savedDataset.getStatus());
    }

    @Test
    void createDatasetForUser_UserNotFound_ThrowsException() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class, () ->
                datasetService.createDatasetForUser("Test", "Desc", "customer", "test@example.com")
        );
        
        verify(datasetRepository, never()).save(any());
    }

    @Test
    void addField_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Id(1L, 1L)).thenReturn(Optional.of(testDataset));
        when(datasetFieldRepository.save(any(DatasetField.class))).thenReturn(testField);

        DatasetFieldDTO fieldDTO = new DatasetFieldDTO("email", "TEXT", true, false, null, 0);

        DatasetField result = datasetService.addField(1L, fieldDTO, "test@example.com");

        assertNotNull(result);
        assertEquals("email", result.getName());
        
        ArgumentCaptor<DatasetField> fieldCaptor = ArgumentCaptor.forClass(DatasetField.class);
        verify(datasetFieldRepository).save(fieldCaptor.capture());
        
        DatasetField savedField = fieldCaptor.getValue();
        assertNotNull(savedField.getDatasetFieldUid());
        assertEquals(DataType.TEXT, savedField.getDtype());
        assertTrue(savedField.getIsNullable());
    }

    @Test
    void addMapping_Success() {
        when(datasetRepository.findById(1L)).thenReturn(Optional.of(testDataset));
        when(sourceRepository.findById(1L)).thenReturn(Optional.of(testSource));
        when(datasetFieldRepository.findByDatasetFieldUid("field-uid")).thenReturn(Optional.of(testField));
        
        DatasetMapping expectedMapping = new DatasetMapping();
        expectedMapping.setId(1L);
        when(datasetMappingRepository.save(any(DatasetMapping.class))).thenReturn(expectedMapping);

        DatasetMappingDTO dto = new DatasetMappingDTO("field-uid", "source_email", "source_email", "NONE", null, false);

        DatasetMapping result = datasetService.addMapping(1L, 1L, dto);

        assertNotNull(result);
        
        ArgumentCaptor<DatasetMapping> mappingCaptor = ArgumentCaptor.forClass(DatasetMapping.class);
        verify(datasetMappingRepository).save(mappingCaptor.capture());
        
        DatasetMapping savedMapping = mappingCaptor.getValue();
        assertNotNull(savedMapping.getDatasetMappingUid());
        assertEquals(testDataset, savedMapping.getDataset());
        assertEquals(testSource, savedMapping.getSource());
        assertEquals(testField, savedMapping.getDatasetField());
    }

    @Test
    void listAllForUser_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findAllByApplicationUser_Email("test@example.com"))
                .thenReturn(Arrays.asList(testDataset));

        List<Dataset> result = datasetService.listAllForUser("test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("Test Dataset", result.get(0).getName());
    }

    @Test
    void getDatasetForUser_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Id(1L, 1L)).thenReturn(Optional.of(testDataset));

        Dataset result = datasetService.getDatasetForUser(1L, "test@example.com");

        assertNotNull(result);
        assertEquals("Test Dataset", result.getName());
    }

    @Test
    void getDatasetForUser_NotFound_ThrowsException() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Id(1L, 1L)).thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class, () ->
                datasetService.getDatasetForUser(1L, "test@example.com")
        );
    }

    @Test
    void updateDataset_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Id(1L, 1L)).thenReturn(Optional.of(testDataset));
        when(datasetRepository.save(any(Dataset.class))).thenReturn(testDataset);

        UpdateDatasetRequest request = new UpdateDatasetRequest(
                "Updated Name",
                "Updated Description",
                DatasetStatus.PAUSED,
                "order"
        );

        DatasetDetailDTO result = datasetService.updateDataset(1L, request, "test@example.com");

        assertNotNull(result);
        
        ArgumentCaptor<Dataset> datasetCaptor = ArgumentCaptor.forClass(Dataset.class);
        verify(datasetRepository).save(datasetCaptor.capture());
        
        Dataset updatedDataset = datasetCaptor.getValue();
        assertEquals("Updated Name", updatedDataset.getName());
        assertEquals("Updated Description", updatedDataset.getDescription());
        assertEquals(DatasetStatus.PAUSED, updatedDataset.getStatus());
        assertNotNull(updatedDataset.getUpdatedAt());
    }

    @Test
    void deleteDataset_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Id(1L, 1L)).thenReturn(Optional.of(testDataset));
        when(sourceRepository.findAllByDataset_Id(1L)).thenReturn(Arrays.asList(testSource));
        when(integrationConnectionRepository.findAllByDataset_Id(1L)).thenReturn(Collections.emptyList());

        datasetService.deleteDataset(1L, "test@example.com");

        verify(datasetRepository).delete(testDataset);
        verify(sourceRepository).saveAll(anyList());
    }

    @Test
    void listFields_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Id(1L, 1L)).thenReturn(Optional.of(testDataset));
        when(datasetFieldRepository.findAllByDataset_Id(1L)).thenReturn(Arrays.asList(testField));

        List<DatasetFieldView> result = datasetService.listFields(1L, "test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("email", result.get(0).name());
        assertEquals("TEXT", result.get(0).dtype());
    }

    @Test
    void deleteField_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Id(1L, 1L)).thenReturn(Optional.of(testDataset));
        when(datasetFieldRepository.findById(1L)).thenReturn(Optional.of(testField));

        datasetService.deleteField(1L, 1L, "test@example.com");

        verify(datasetFieldRepository).delete(testField);
    }

    @Test
    void deleteField_FieldNotInDataset_ThrowsException() {
        Dataset otherDataset = new Dataset();
        otherDataset.setId(2L);
        testField.setDataset(otherDataset);

        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Id(1L, 1L)).thenReturn(Optional.of(testDataset));
        when(datasetFieldRepository.findById(1L)).thenReturn(Optional.of(testField));

        assertThrows(IllegalArgumentException.class, () ->
                datasetService.deleteField(1L, 1L, "test@example.com")
        );
        
        verify(datasetFieldRepository, never()).delete(any());
    }

    @Test
    void countSources_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(sourceRepository.findAllByDataset_IdAndApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(Arrays.asList(testSource));

        long count = datasetService.countSources(1L, "test@example.com");

        assertEquals(1, count);
    }

    @Test
    void countMappings_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Id(1L, 1L)).thenReturn(Optional.of(testDataset));
        
        DatasetMapping mapping = new DatasetMapping();
        when(datasetMappingRepository.findAllByDataset(testDataset)).thenReturn(Arrays.asList(mapping));

        long count = datasetService.countMappings(1L, "test@example.com");

        assertEquals(1, count);
    }

    @Test
    void getPipelineStatus_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(datasetRepository.findByIdAndApplicationUser_Id(1L, 1L)).thenReturn(Optional.of(testDataset));
        when(sourceRepository.findAllByDataset_IdAndApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(Arrays.asList(testSource));
        when(rawEventRepository.countByDataset_Id(1L)).thenReturn(10L);
        when(datasetMappingRepository.findAllByDataset(testDataset)).thenReturn(Collections.emptyList());
        when(unifiedRowRepository.countByDatasetAndIsExcludedFalse(testDataset)).thenReturn(5L);
        when(unifiedRowRepository.findFirstByDatasetOrderByIngestedAtDesc(testDataset))
                .thenReturn(Optional.empty());

        PipelineStatusResponse result = datasetService.getPipelineStatus(1L, "test@example.com");

        assertNotNull(result);
        assertEquals(1L, result.datasetId());
        assertTrue(result.hasSources());
        assertTrue(result.hasIngestion());
        assertFalse(result.hasMappings());
        assertTrue(result.hasTransformation());
    }
}
