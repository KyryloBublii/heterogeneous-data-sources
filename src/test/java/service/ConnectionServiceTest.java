package service;

import org.example.models.dto.ConnectionRequest;
import org.example.models.dto.ConnectionResponse;
import org.example.models.dto.TableSelectionDTO;
import org.example.models.dto.TableSelectionUpdateRequest;
import org.example.models.entity.ApplicationUser;
import org.example.models.entity.Dataset;
import org.example.models.entity.IntegrationConnection;
import org.example.models.entity.Source;
import org.example.models.enums.DatasetStatus;
import org.example.models.enums.SourceType;
import org.example.repository.DatasetRepository;
import org.example.repository.IntegrationConnectionRepository;
import org.example.repository.UserRepository;
import org.example.service.ConnectionService;
import org.example.service.SourceService;
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
class ConnectionServiceTest {

    @Mock
    private IntegrationConnectionRepository connectionRepository;

    @Mock
    private SourceService sourceService;

    @Mock
    private UserRepository userRepository;

    @Mock
    private DatasetRepository datasetRepository;

    @InjectMocks
    private ConnectionService connectionService;

    private ApplicationUser testUser;
    private Dataset testDataset;
    private Source testSource;
    private Source testDestination;
    private IntegrationConnection testConnection;

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
        testDataset.setApplicationUser(testUser);
        testDataset.setStatus(DatasetStatus.ACTIVE);

        testSource = new Source();
        testSource.setId(1L);
        testSource.setSourceUid("source-uid");
        testSource.setName("Test Source");
        testSource.setType(SourceType.CSV);
        testSource.setApplicationUser(testUser);
        testSource.setDataset(testDataset);

        testDestination = new Source();
        testDestination.setId(2L);
        testDestination.setSourceUid("dest-uid");
        testDestination.setName("Test Destination");
        testDestination.setType(SourceType.CSV);
        testDestination.setApplicationUser(testUser);
        testDestination.setDataset(testDataset);

        testConnection = new IntegrationConnection();
        testConnection.setId(1L);
        testConnection.setConnectionUid("conn-uid");
        testConnection.setDataset(testDataset);
        testConnection.setSource(testSource);
        testConnection.setDestination(testDestination);
        testConnection.setRelation("LOAD");
        testConnection.setCreatedBy("test@example.com");
        testConnection.setCreatedAt(Instant.now());
    }

    @Test
    void createConnection_WithDatasetId_Success() {
        ConnectionRequest request = new ConnectionRequest(
                "source-uid",
                "dest-uid",
                "LOAD",
                1L,
                List.of()
        );

        when(datasetRepository.findByIdAndApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(Optional.of(testDataset));
        when(sourceService.getSourceById("source-uid", "test@example.com")).thenReturn(testSource);
        when(sourceService.getSourceById("dest-uid", "test@example.com")).thenReturn(testDestination);
        when(connectionRepository.save(any(IntegrationConnection.class))).thenReturn(testConnection);

        ConnectionResponse response = connectionService.createConnection(request, "test@example.com", "test@example.com");

        assertNotNull(response);
        assertEquals("conn-uid", response.id());
        assertEquals("source-uid", response.sourceId());
        assertEquals("dest-uid", response.destinationId());
        assertEquals("LOAD", response.relation());

        ArgumentCaptor<IntegrationConnection> captor = ArgumentCaptor.forClass(IntegrationConnection.class);
        verify(connectionRepository).save(captor.capture());

        IntegrationConnection saved = captor.getValue();
        assertNotNull(saved.getConnectionUid());
        assertEquals(testDataset, saved.getDataset());
        assertEquals(testSource, saved.getSource());
        assertEquals(testDestination, saved.getDestination());
        verify(sourceService).triggerIngestion(eq(testSource), eq(testDestination), anyList());
    }

    @Test
    void createConnection_WithoutDatasetId_UsesSourceDataset() {
        ConnectionRequest request = new ConnectionRequest(
                "source-uid",
                null,
                "LOAD",
                null,
                List.of()
        );

        when(sourceService.getSourceById("source-uid", "test@example.com")).thenReturn(testSource);
        when(connectionRepository.save(any(IntegrationConnection.class))).thenReturn(testConnection);

        ConnectionResponse response = connectionService.createConnection(request, "test@example.com", "test@example.com");

        assertNotNull(response);
        verify(connectionRepository).save(any(IntegrationConnection.class));
        verify(sourceService).triggerIngestion(eq(testSource), eq(null), anyList());
    }

    @Test
    void createConnection_DatasetNotFound_ThrowsException() {
        ConnectionRequest request = new ConnectionRequest(
                "source-uid",
                null,
                "LOAD",
                1L,
                List.of()
        );

        when(datasetRepository.findByIdAndApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class, () ->
                connectionService.createConnection(request, "test@example.com", "test@example.com")
        );

        verify(connectionRepository, never()).save(any());
    }

    @Test
    void createConnection_NoDatasetAvailable_ThrowsException() {
        Source sourceWithoutDataset = new Source();
        sourceWithoutDataset.setSourceUid("source-uid");
        sourceWithoutDataset.setDataset(null);

        ConnectionRequest request = new ConnectionRequest(
                "source-uid",
                null,
                "LOAD",
                null,
                List.of()
        );

        when(sourceService.getSourceById("source-uid", "test@example.com")).thenReturn(sourceWithoutDataset);

        assertThrows(IllegalArgumentException.class, () ->
                connectionService.createConnection(request, "test@example.com", "test@example.com")
        );

        verify(connectionRepository, never()).save(any());
    }

    @Test
    void createConnection_SourceNotInDataset_ThrowsException() {
        Dataset otherDataset = new Dataset();
        otherDataset.setId(2L);
        testSource.setDataset(otherDataset);

        ConnectionRequest request = new ConnectionRequest(
                "source-uid",
                null,
                "LOAD",
                1L,
                List.of()
        );

        when(datasetRepository.findByIdAndApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(Optional.of(testDataset));
        when(sourceService.getSourceById("source-uid", "test@example.com")).thenReturn(testSource);

        assertThrows(IllegalArgumentException.class, () ->
                connectionService.createConnection(request, "test@example.com", "test@example.com")
        );

        verify(connectionRepository, never()).save(any());
    }

    @Test
    void createConnection_WithTableSelections_Success() {
        TableSelectionDTO selection = new TableSelectionDTO("users", "public", List.of("id", "email"));
        ConnectionRequest request = new ConnectionRequest(
                "source-uid",
                "dest-uid",
                "LOAD",
                1L,
                Arrays.asList(selection)
        );

        Map<String, Object> expectedTableSelection = new LinkedHashMap<>();
        expectedTableSelection.put("tableName", "users");
        expectedTableSelection.put("schema", "public");
        expectedTableSelection.put("columns", Arrays.asList("id", "email"));

        IntegrationConnection connectionWithSelections = new IntegrationConnection();
        connectionWithSelections.setConnectionUid("conn-uid");
        connectionWithSelections.setDataset(testDataset);
        connectionWithSelections.setSource(testSource);
        connectionWithSelections.setDestination(testDestination);
        connectionWithSelections.setTableSelection(Arrays.asList(expectedTableSelection));

        when(datasetRepository.findByIdAndApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(Optional.of(testDataset));
        when(sourceService.getSourceById("source-uid", "test@example.com")).thenReturn(testSource);
        when(sourceService.getSourceById("dest-uid", "test@example.com")).thenReturn(testDestination);
        when(connectionRepository.save(any(IntegrationConnection.class))).thenReturn(connectionWithSelections);

        ConnectionResponse response = connectionService.createConnection(request, "test@example.com", "test@example.com");

        assertNotNull(response);
        verify(connectionRepository).save(any(IntegrationConnection.class));
        verify(sourceService).triggerIngestion(eq(testSource), eq(testDestination), anyList());
    }

    @Test
    void createConnection_DatabaseSourceWithoutTableSelection_SkipsIngestion() {
        testSource.setType(SourceType.DB);
        ConnectionRequest request = new ConnectionRequest(
                "source-uid",
                "dest-uid",
                "LOAD",
                1L,
                null
        );

        when(datasetRepository.findByIdAndApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(Optional.of(testDataset));
        when(sourceService.getSourceById("source-uid", "test@example.com")).thenReturn(testSource);
        when(sourceService.getSourceById("dest-uid", "test@example.com")).thenReturn(testDestination);
        when(connectionRepository.save(any(IntegrationConnection.class))).thenReturn(testConnection);

        ConnectionResponse response = connectionService.createConnection(request, "test@example.com", "test@example.com");

        assertNotNull(response);
        verify(sourceService, never()).triggerIngestion(any(), any(), any());
    }

    @Test
    void updateTableSelection_Success() {
        TableSelectionDTO selection = new TableSelectionDTO("orders", "public", List.of("id", "total"));
        TableSelectionUpdateRequest request = new TableSelectionUpdateRequest(Arrays.asList(selection));

        Map<String, Object> expectedTableSelection = new LinkedHashMap<>();
        expectedTableSelection.put("tableName", "orders");
        expectedTableSelection.put("schema", "public");
        expectedTableSelection.put("columns", Arrays.asList("id", "total"));

        IntegrationConnection updatedConnection = new IntegrationConnection();
        updatedConnection.setConnectionUid("conn-uid");
        updatedConnection.setDataset(testDataset);
        updatedConnection.setSource(testSource);
        updatedConnection.setDestination(testDestination);
        updatedConnection.setTableSelection(Arrays.asList(expectedTableSelection));

        when(connectionRepository.findByConnectionUidAndSource_ApplicationUser_Email("conn-uid", "test@example.com"))
                .thenReturn(testConnection);
        when(connectionRepository.save(any(IntegrationConnection.class))).thenReturn(updatedConnection);

        ConnectionResponse response = connectionService.updateTableSelection("conn-uid", request, "test@example.com");

        assertNotNull(response);
        verify(connectionRepository).save(any(IntegrationConnection.class));
        verify(sourceService).triggerIngestion(eq(testSource), eq(testDestination), anyList());
    }

    @Test
    void updateTableSelection_ConnectionNotFound_ThrowsException() {
        TableSelectionUpdateRequest request = new TableSelectionUpdateRequest(List.of());

        when(connectionRepository.findByConnectionUidAndSource_ApplicationUser_Email("conn-uid", "test@example.com"))
                .thenReturn(null);

        assertThrows(IllegalArgumentException.class, () ->
                connectionService.updateTableSelection("conn-uid", request, "test@example.com")
        );

        verify(connectionRepository, never()).save(any());
        verify(sourceService, never()).triggerIngestion(any(), any(), any());
    }

    @Test
    void listConnections_WithoutDatasetId_ReturnsAll() {
        when(connectionRepository.findAllBySource_ApplicationUser_Email("test@example.com"))
                .thenReturn(Arrays.asList(testConnection));

        List<ConnectionResponse> responses = connectionService.listConnections("test@example.com", null);

        assertNotNull(responses);
        assertEquals(1, responses.size());
        assertEquals("conn-uid", responses.get(0).id());
        verify(connectionRepository).findAllBySource_ApplicationUser_Email("test@example.com");
    }

    @Test
    void listConnections_WithDatasetId_ReturnsFiltered() {
        when(connectionRepository.findAllByDataset_IdAndSource_ApplicationUser_Email(1L, "test@example.com"))
                .thenReturn(Arrays.asList(testConnection));

        List<ConnectionResponse> responses = connectionService.listConnections("test@example.com", 1L);

        assertNotNull(responses);
        assertEquals(1, responses.size());
        assertEquals("conn-uid", responses.get(0).id());
        verify(connectionRepository).findAllByDataset_IdAndSource_ApplicationUser_Email(1L, "test@example.com");
    }

    @Test
    void listConnections_NoConnections_ReturnsEmptyList() {
        when(connectionRepository.findAllBySource_ApplicationUser_Email("test@example.com"))
                .thenReturn(Collections.emptyList());

        List<ConnectionResponse> responses = connectionService.listConnections("test@example.com", null);

        assertNotNull(responses);
        assertTrue(responses.isEmpty());
    }
}
