package controller;

import org.example.controllers.DatasetController;
import org.example.models.dto.CreateDatasetRequest;
import org.example.models.dto.DatasetSummaryDTO;
import org.example.models.entity.ApplicationUser;
import org.example.models.entity.Dataset;
import org.example.models.entity.UnifiedRow;
import org.example.models.enums.DatasetStatus;
import org.example.repository.RawEventRepository;
import org.example.repository.UnifiedRowRepository;
import org.example.service.DatasetService;
import org.example.service.TransformService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DatasetControllerTest {

    @Mock
    private DatasetService datasetService;

    @Mock
    private UnifiedRowRepository unifiedRowRepository;

    @Mock
    private RawEventRepository rawEventRepository;

    @Mock
    private TransformService transformService;

    @Mock
    private Authentication authentication;

    @InjectMocks
    private DatasetController datasetController;

    private ApplicationUser testUser;
    private Dataset testDataset;
    private UnifiedRow testRow;

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

        testRow = new UnifiedRow();
        testRow.setId(1L);
        testRow.setIngestedAt(Instant.now());
    }

    @Test
    void listDatasets_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.listAllForUser("test@example.com")).thenReturn(Arrays.asList(testDataset));
        when(unifiedRowRepository.countByDataset(testDataset)).thenReturn(10L);
        when(unifiedRowRepository.findFirstByDatasetOrderByIngestedAtDesc(testDataset))
                .thenReturn(Optional.of(testRow));

        List<DatasetSummaryDTO> result = datasetController.listDatasets(authentication);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("Test Dataset", result.get(0).name());
        assertEquals(10L, result.get(0).recordCount());
        assertEquals("Test User", result.get(0).owner());

        verify(datasetService).listAllForUser("test@example.com");
        verify(unifiedRowRepository).countByDataset(testDataset);
    }

    @Test
    void listDatasets_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                datasetController.listDatasets(authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(datasetService, never()).listAllForUser(anyString());
    }

    @Test
    void createDataset_Success() {
        CreateDatasetRequest request = new CreateDatasetRequest("New Dataset", "Description", "customer");

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.createDatasetForUser("New Dataset", "Description", "customer", "test@example.com"))
                .thenReturn(testDataset);
        when(unifiedRowRepository.countByDataset(testDataset)).thenReturn(0L);
        when(unifiedRowRepository.findFirstByDatasetOrderByIngestedAtDesc(testDataset))
                .thenReturn(Optional.empty());

        ResponseEntity<DatasetSummaryDTO> response = datasetController.createDataset(request, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("Test Dataset", response.getBody().name());
        assertEquals(0L, response.getBody().recordCount());

        verify(datasetService).createDatasetForUser("New Dataset", "Description", "customer", "test@example.com");
    }

    @Test
    void createDataset_NoAuthentication_ThrowsUnauthorized() {
        CreateDatasetRequest request = new CreateDatasetRequest("New Dataset", "Description", "customer");
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                datasetController.createDataset(request, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(datasetService, never()).createDatasetForUser(anyString(), anyString(), anyString(), anyString());
    }

    @Test
    void createDataset_WithNullDescription_Success() {
        CreateDatasetRequest request = new CreateDatasetRequest("New Dataset", null, "customer");

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.createDatasetForUser("New Dataset", null, "customer", "test@example.com"))
                .thenReturn(testDataset);
        when(unifiedRowRepository.countByDataset(testDataset)).thenReturn(0L);
        when(unifiedRowRepository.findFirstByDatasetOrderByIngestedAtDesc(testDataset))
                .thenReturn(Optional.empty());

        ResponseEntity<DatasetSummaryDTO> response = datasetController.createDataset(request, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        verify(datasetService).createDatasetForUser("New Dataset", null, "customer", "test@example.com");
    }

    @Test
    void listDatasets_EmptyList_ReturnsEmptyList() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.listAllForUser("test@example.com")).thenReturn(Arrays.asList());

        List<DatasetSummaryDTO> result = datasetController.listDatasets(authentication);

        assertNotNull(result);
        assertTrue(result.isEmpty());

        verify(datasetService).listAllForUser("test@example.com");
    }

    @Test
    void listDatasets_WithMultipleDatasets_ReturnsAll() {
        Dataset dataset2 = new Dataset();
        dataset2.setId(2L);
        dataset2.setName("Dataset 2");
        dataset2.setApplicationUser(testUser);
        dataset2.setStatus(DatasetStatus.ACTIVE);

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.listAllForUser("test@example.com"))
                .thenReturn(Arrays.asList(testDataset, dataset2));
        when(unifiedRowRepository.countByDataset(any())).thenReturn(5L);
        when(unifiedRowRepository.findFirstByDatasetOrderByIngestedAtDesc(any()))
                .thenReturn(Optional.empty());

        List<DatasetSummaryDTO> result = datasetController.listDatasets(authentication);

        assertNotNull(result);
        assertEquals(2, result.size());

        verify(datasetService).listAllForUser("test@example.com");
    }
}
