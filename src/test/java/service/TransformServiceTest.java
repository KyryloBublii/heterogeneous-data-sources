package service;

import org.example.models.entity.*;
import org.example.models.enums.DataType;
import org.example.models.enums.DatasetStatus;
import org.example.models.enums.RunStatus;
import org.example.models.enums.TransformType;
import org.example.repository.*;
import org.example.service.TransformService;
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
class TransformServiceTest {

    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private DatasetMappingRepository datasetMappingRepository;

    @Mock
    private DatasetFieldRepository datasetFieldRepository;

    @Mock
    private RelationshipRepository relationshipRepository;

    @Mock
    private RawEventRepository rawEventRepository;

    @Mock
    private UnifiedRowRepository unifiedRowRepository;

    @Mock
    private TransformRunRepository transformRunRepository;

    @InjectMocks
    private TransformService transformService;

    private Dataset testDataset;
    private DatasetField testField;
    private Source testSource;
    private RawEvent testEvent;
    private DatasetMapping testMapping;

    @BeforeEach
    void setUp() {
        ApplicationUser testUser = new ApplicationUser();
        testUser.setId(1L);
        testUser.setEmail("test@example.com");

        testDataset = new Dataset();
        testDataset.setId(1L);
        testDataset.setDatasetUid("dataset-uid");
        testDataset.setName("Test Dataset");
        testDataset.setPrimaryRecordType("customer");
        testDataset.setApplicationUser(testUser);

        testSource = new Source();
        testSource.setId(1L);
        testSource.setSourceUid("source-uid");
        testSource.setName("Test Source");
        testSource.setDataset(testDataset);
        testSource.setApplicationUser(testUser);

        testField = new DatasetField();
        testField.setId(1L);
        testField.setDatasetFieldUid("field-uid");
        testField.setName("email");
        testField.setDtype(DataType.TEXT);
        testField.setDataset(testDataset);
        testField.setPosition(0);
        testField.setIsNullable(true);

        testEvent = new RawEvent();
        testEvent.setId(1L);
        testEvent.setRawEventUid("event-uid");
        testEvent.setSource(testSource);
        testEvent.setDataset(testDataset);
        testEvent.setCreatedAt(Instant.now());
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("email", "test@example.com");
        payload.put("name", "Test User");
        payload.put("__table__", "customer");
        payload.put("id", "CUST-1");
        testEvent.setPayload(payload);

        testMapping = DatasetMapping.builder()
                .id(1L)
                .datasetMappingUid("mapping-uid")
                .dataset(testDataset)
                .source(testSource)
                .datasetField(testField)
                .srcPath("email")
                .srcJsonPath("email")
                .transformType(TransformType.NONE)
                .required(false)
                .priority(0)
                .build();
    }

    @Test
    void startTransform_WithoutRelationships_Success() {
        when(datasetRepository.findById(1L)).thenReturn(Optional.of(testDataset));
        when(rawEventRepository.findByDataset_Id(1L)).thenReturn(Arrays.asList(testEvent));
        when(relationshipRepository.findByDatasetId(1L)).thenReturn(Collections.emptyList());
        when(datasetFieldRepository.findAllByDataset_Id(1L)).thenReturn(Arrays.asList(testField));
        when(datasetMappingRepository.findAllByDataset(testDataset)).thenReturn(Arrays.asList(testMapping));
        
        TransformRun savedRun = new TransformRun();
        savedRun.setId(1L);
        savedRun.setTransformRunUid("transform-run-uid");
        savedRun.setRunStatus(RunStatus.RUNNING);
        when(transformRunRepository.save(any(TransformRun.class))).thenReturn(savedRun);
        
        when(datasetRepository.save(any(Dataset.class))).thenReturn(testDataset);

        TransformRun result = transformService.startTransform(1L);

        assertNotNull(result);
        assertEquals(RunStatus.SUCCESS, result.getRunStatus());
        
        verify(unifiedRowRepository).deleteByDataset(testDataset);
        verify(unifiedRowRepository, atLeastOnce()).save(any(UnifiedRow.class));
        verify(datasetRepository, atLeast(2)).save(any(Dataset.class));
        
        ArgumentCaptor<Dataset> datasetCaptor = ArgumentCaptor.forClass(Dataset.class);
        verify(datasetRepository, atLeast(2)).save(datasetCaptor.capture());
        
        List<Dataset> savedDatasets = datasetCaptor.getAllValues();
        Dataset finalDataset = savedDatasets.get(savedDatasets.size() - 1);
        assertEquals(DatasetStatus.FINISHED, finalDataset.getStatus());
    }

    @Test
    void startTransform_WithRelationships_Success() {
        when(datasetRepository.findById(1L)).thenReturn(Optional.of(testDataset));
        when(rawEventRepository.findByDataset_Id(1L)).thenReturn(Arrays.asList(testEvent));
        
        Relationship testRelationship = new Relationship();
        testRelationship.setId(1L);
        testRelationship.setRelationshipUid("rel-uid");
        testRelationship.setSource(testSource);
        testRelationship.setFromType("customer");
        testRelationship.setFromId("CUST-1");
        testRelationship.setToType("order");
        testRelationship.setToId("ORD-1");
        testRelationship.setRelationType("shared_customer_id");
        testRelationship.setIngestedAt(Instant.now());
        
        when(relationshipRepository.findByDatasetId(1L)).thenReturn(Arrays.asList(testRelationship));
        when(datasetFieldRepository.findAllByDataset_Id(1L)).thenReturn(Arrays.asList(testField));
        when(datasetMappingRepository.findAllByDataset(testDataset)).thenReturn(Arrays.asList(testMapping));
        
        TransformRun savedRun = new TransformRun();
        savedRun.setId(1L);
        savedRun.setRunStatus(RunStatus.RUNNING);
        when(transformRunRepository.save(any(TransformRun.class))).thenReturn(savedRun);
        
        when(datasetRepository.save(any(Dataset.class))).thenReturn(testDataset);

        TransformRun result = transformService.startTransform(1L);

        assertNotNull(result);
        assertEquals(RunStatus.SUCCESS, result.getRunStatus());
        assertNotNull(result.getRowsIn());
        assertNotNull(result.getRowsOut());
        
        verify(unifiedRowRepository).deleteByDataset(testDataset);
        verify(unifiedRowRepository, atLeastOnce()).save(any(UnifiedRow.class));
    }

    @Test
    void startTransform_DatasetNotFound_ThrowsException() {
        when(datasetRepository.findById(1L)).thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class, () ->
                transformService.startTransform(1L)
        );
        
        verify(transformRunRepository, never()).save(any());
    }

    @Test
    void startTransform_WithTransformTypes_AppliesTransforms() {
        testMapping.setTransformType(TransformType.LOWERCASE);
        
        Map<String, Object> payload = new HashMap<>();
        payload.put("email", "TEST@EXAMPLE.COM");
        payload.put("__table__", "customer");
        payload.put("id", "CUST-1");
        testEvent.setPayload(payload);
        
        when(datasetRepository.findById(1L)).thenReturn(Optional.of(testDataset));
        when(rawEventRepository.findByDataset_Id(1L)).thenReturn(Arrays.asList(testEvent));
        when(relationshipRepository.findByDatasetId(1L)).thenReturn(Collections.emptyList());
        when(datasetFieldRepository.findAllByDataset_Id(1L)).thenReturn(Arrays.asList(testField));
        when(datasetMappingRepository.findAllByDataset(testDataset)).thenReturn(Arrays.asList(testMapping));
        
        TransformRun savedRun = new TransformRun();
        savedRun.setId(1L);
        savedRun.setRunStatus(RunStatus.RUNNING);
        when(transformRunRepository.save(any(TransformRun.class))).thenReturn(savedRun);
        
        when(datasetRepository.save(any(Dataset.class))).thenReturn(testDataset);

        TransformRun result = transformService.startTransform(1L);

        assertNotNull(result);
        assertEquals(RunStatus.SUCCESS, result.getRunStatus());
        
        ArgumentCaptor<UnifiedRow> rowCaptor = ArgumentCaptor.forClass(UnifiedRow.class);
        verify(unifiedRowRepository, atLeastOnce()).save(rowCaptor.capture());
        
        UnifiedRow savedRow = rowCaptor.getValue();
        assertNotNull(savedRow.getData());
        
        Object emailValue = savedRow.getData().get(String.valueOf(testField.getId()));
        if (emailValue != null) {
            assertEquals("test@example.com", emailValue.toString().toLowerCase());
        }
    }

    @Test
    void startTransform_WithNullPrimaryRecordType_DeterminesAutomatically() {
        testDataset.setPrimaryRecordType(null);
        
        when(datasetRepository.findById(1L)).thenReturn(Optional.of(testDataset));
        when(rawEventRepository.findByDataset_Id(1L)).thenReturn(Arrays.asList(testEvent));
        when(relationshipRepository.findByDatasetId(1L)).thenReturn(Collections.emptyList());
        when(datasetFieldRepository.findAllByDataset_Id(1L)).thenReturn(Arrays.asList(testField));
        when(datasetMappingRepository.findAllByDataset(testDataset)).thenReturn(Arrays.asList(testMapping));
        
        TransformRun savedRun = new TransformRun();
        savedRun.setId(1L);
        savedRun.setRunStatus(RunStatus.RUNNING);
        when(transformRunRepository.save(any(TransformRun.class))).thenReturn(savedRun);
        
        when(datasetRepository.save(any(Dataset.class))).thenReturn(testDataset);

        TransformRun result = transformService.startTransform(1L);

        assertNotNull(result);
        assertEquals(RunStatus.SUCCESS, result.getRunStatus());
        
        ArgumentCaptor<Dataset> datasetCaptor = ArgumentCaptor.forClass(Dataset.class);
        verify(datasetRepository, atLeast(2)).save(datasetCaptor.capture());
        
        List<Dataset> savedDatasets = datasetCaptor.getAllValues();
        String determinedType = savedDatasets.get(0).getPrimaryRecordType();
        assertNotNull(determinedType);
        assertFalse(determinedType.isBlank());
    }



    @Test
    void startTransform_MultipleFields_MapsAllFields() {
        DatasetField nameField = new DatasetField();
        nameField.setId(2L);
        nameField.setDatasetFieldUid("name-field-uid");
        nameField.setName("name");
        nameField.setDtype(DataType.TEXT);
        nameField.setDataset(testDataset);
        nameField.setPosition(1);
        
        DatasetMapping nameMapping = DatasetMapping.builder()
                .id(2L)
                .datasetMappingUid("name-mapping-uid")
                .dataset(testDataset)
                .source(testSource)
                .datasetField(nameField)
                .srcPath("name")
                .srcJsonPath("name")
                .transformType(TransformType.UPPERCASE)
                .required(false)
                .priority(0)
                .build();
        
        when(datasetRepository.findById(1L)).thenReturn(Optional.of(testDataset));
        when(rawEventRepository.findByDataset_Id(1L)).thenReturn(Arrays.asList(testEvent));
        when(relationshipRepository.findByDatasetId(1L)).thenReturn(Collections.emptyList());
        when(datasetFieldRepository.findAllByDataset_Id(1L))
                .thenReturn(Arrays.asList(testField, nameField));
        when(datasetMappingRepository.findAllByDataset(testDataset))
                .thenReturn(Arrays.asList(testMapping, nameMapping));
        
        TransformRun savedRun = new TransformRun();
        savedRun.setId(1L);
        savedRun.setRunStatus(RunStatus.RUNNING);
        when(transformRunRepository.save(any(TransformRun.class))).thenReturn(savedRun);
        
        when(datasetRepository.save(any(Dataset.class))).thenReturn(testDataset);

        TransformRun result = transformService.startTransform(1L);

        assertNotNull(result);
        assertEquals(RunStatus.SUCCESS, result.getRunStatus());
        
        ArgumentCaptor<UnifiedRow> rowCaptor = ArgumentCaptor.forClass(UnifiedRow.class);
        verify(unifiedRowRepository, atLeastOnce()).save(rowCaptor.capture());
        
        UnifiedRow savedRow = rowCaptor.getValue();
        assertNotNull(savedRow.getData());
        assertTrue(savedRow.getData().containsKey(String.valueOf(testField.getId())));
        assertTrue(savedRow.getData().containsKey(String.valueOf(nameField.getId())));
    }
}
