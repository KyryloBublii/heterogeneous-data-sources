package service;

import org.example.models.entity.*;
import org.example.models.enums.RunStatus;
import org.example.models.enums.SourceRole;
import org.example.models.enums.SourceType;
import org.example.repository.DatasetRepository;
import org.example.repository.IngestionRunRepository;
import org.example.repository.SourceRepository;
import org.example.service.ingestion.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IngestionServiceTest {

    @Mock
    private List<RecordExtractor> extractors;

    @Mock
    private WrapperMappingService wrapperMappingService;

    @Mock
    private RelationshipService relationshipService;

    @Mock
    private RawEventService rawEventService;

    @Mock
    private RelationshipPersistenceService relationshipPersistenceService;

    @Mock
    private DestinationOutputService destinationOutputService;

    @Mock
    private IngestionRunService ingestionRunService;

    @Mock
    private IngestionRunRepository ingestionRunRepository;

    @Mock
    private DatasetRepository datasetRepository;

    @Mock
    private SourceRepository sourceRepository;

    @Mock
    private RecordExtractor csvExtractor;

    @InjectMocks
    private IngestionService ingestionService;

    private Dataset testDataset;
    private Source testSource;
    private IngestionRun testRun;
    private ApplicationUser testUser;

    @BeforeEach
    void setUp() {
        testUser = new ApplicationUser();
        testUser.setId(1L);
        testUser.setEmail("test@example.com");

        testDataset = new Dataset();
        testDataset.setId(1L);
        testDataset.setDatasetUid("dataset-uid");
        testDataset.setName("Test Dataset");
        testDataset.setApplicationUser(testUser);

        testSource = new Source();
        testSource.setId(1L);
        testSource.setSourceUid("source-uid");
        testSource.setName("Test Source");
        testSource.setType(SourceType.CSV);
        testSource.setRole(SourceRole.SOURCE);
        testSource.setDataset(testDataset);
        testSource.setApplicationUser(testUser);
        testSource.setConfig(Map.of("format", "csv", "filePath", "/test/file.csv"));

        testRun = new IngestionRun();
        testRun.setId(1L);
        testRun.setIngestionUid("run-uid");
        testRun.setSource(testSource);
        testRun.setDataset(testDataset);
        testRun.setRunStatus(RunStatus.QUEUED);
        testRun.setStartedAt(Instant.now());
    }

    @Test
    void ingestDataset_NoSources_ReturnsEmpty() {
        when(datasetRepository.findById(1L)).thenReturn(Optional.of(testDataset));
        when(sourceRepository.findAllByDataset_Id(1L)).thenReturn(Collections.emptyList());

        Map<Source, IngestionRun> result = ingestionService.ingestDataset(1L);

        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(ingestionRunRepository, never()).save(any());
    }

    @Test
    void ingestDataset_DatasetNotFound_ThrowsException() {
        when(datasetRepository.findById(1L)).thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class, () ->
                ingestionService.ingestDataset(1L)
        );
    }



}
