package controller;

import org.example.controllers.DataIntegrationController;
import org.example.models.dto.IntegrationConfigDTO;
import org.example.models.entity.Dataset;
import org.example.models.entity.UnifiedRow;
import org.example.models.enums.DatasetStatus;
import org.example.repository.UnifiedRowRepository;
import org.example.service.DataIntegrationService;
import org.example.service.DatasetService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataIntegrationControllerTest {

    @Mock
    private DataIntegrationService integrationService;

    @Mock
    private UnifiedRowRepository unifiedRowRepository;

    @Mock
    private DatasetService datasetService;

    @Mock
    private Authentication authentication;

    @InjectMocks
    private DataIntegrationController dataIntegrationController;

    private Dataset testDataset;
    private UnifiedRow testRow;
    private IntegrationConfigDTO testConfig;

    @BeforeEach
    void setUp() {
        testDataset = new Dataset();
        testDataset.setId(1L);
        testDataset.setName("Test Dataset");
        testDataset.setStatus(DatasetStatus.ACTIVE);

        testRow = new UnifiedRow();
        testRow.setId(1L);
        testRow.setUnifiedRowUid("row-1");
        testRow.setDataset(testDataset);
        testRow.setData(Map.of("id", 1, "name", "Test"));
        testRow.setIngestedAt(Instant.now());

        testConfig = new IntegrationConfigDTO();
        testConfig.setDatasetId(1L);
        testConfig.setSourceMappings(Collections.emptyList());
    }

    @Test
    void runIntegration_Success() {
        Map<String, Object> successResult = Map.of(
                "success", true,
                "message", "Integration completed",
                "recordsProcessed", 100
        );

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(integrationService.runIntegration(testConfig, "test@example.com")).thenReturn(successResult);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.runIntegration(testConfig, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue((Boolean) response.getBody().get("success"));
        assertEquals(100, response.getBody().get("recordsProcessed"));

        verify(integrationService).runIntegration(testConfig, "test@example.com");
    }

    @Test
    void runIntegration_PartialSuccess_ReturnsPartialContent() {
        Map<String, Object> partialResult = Map.of(
                "success", false,
                "message", "Partially completed",
                "errors", Arrays.asList("Error 1", "Error 2")
        );

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(integrationService.runIntegration(testConfig, "test@example.com")).thenReturn(partialResult);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.runIntegration(testConfig, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.PARTIAL_CONTENT, response.getStatusCode());
        assertFalse((Boolean) response.getBody().get("success"));
    }

    @Test
    void runIntegration_ServiceThrowsException_ReturnsInternalServerError() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(integrationService.runIntegration(any(), anyString()))
                .thenThrow(new RuntimeException("Integration failed"));

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.runIntegration(testConfig, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertFalse((Boolean) response.getBody().get("success"));
        assertTrue(response.getBody().get("error").toString().contains("Integration failed"));
    }

    @Test
    void runIntegration_NoAuthentication_ReturnsInternalServerError() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.runIntegration(testConfig, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertFalse((Boolean) response.getBody().get("success"));
        assertTrue(response.getBody().get("error").toString().contains("Authentication required"));

        verify(integrationService, never()).runIntegration(any(), anyString());
    }

    @Test
    void getUnifiedData_WithDatasetId_Success() {
        Page<UnifiedRow> page = new PageImpl<>(Arrays.asList(testRow));

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com")).thenReturn(testDataset);
        when(unifiedRowRepository.findByDataset(eq(testDataset), any(Pageable.class))).thenReturn(page);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.getUnifiedData(1L, 0, 50, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().containsKey("data"));
        assertEquals(0, response.getBody().get("currentPage"));
        assertEquals(1, response.getBody().get("totalPages"));
        assertEquals(1L, response.getBody().get("totalRecords"));

        verify(datasetService).getDatasetForUser(1L, "test@example.com");
        verify(unifiedRowRepository).findByDataset(eq(testDataset), any(Pageable.class));
    }

    @Test
    void getUnifiedData_WithoutDatasetId_ReturnsAllDatasets() {
        Page<UnifiedRow> page = new PageImpl<>(Arrays.asList(testRow));

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.listAllForUser("test@example.com")).thenReturn(Arrays.asList(testDataset));
        when(unifiedRowRepository.findByDatasetIn(anyList(), any(Pageable.class))).thenReturn(page);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.getUnifiedData(null, 0, 50, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody().containsKey("data"));

        verify(datasetService).listAllForUser("test@example.com");
        verify(unifiedRowRepository).findByDatasetIn(anyList(), any(Pageable.class));
    }

    @Test
    void getUnifiedData_NoDatasets_ReturnsEmptyPage() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.listAllForUser("test@example.com")).thenReturn(Collections.emptyList());

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.getUnifiedData(null, 0, 50, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(0L, response.getBody().get("totalRecords"));

        verify(unifiedRowRepository, never()).findByDatasetIn(anyList(), any(Pageable.class));
    }

    @Test
    void getUnifiedData_NoAuthentication_ReturnsInternalServerError() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.getUnifiedData(1L, 0, 50, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertTrue(response.getBody().containsKey("error"));
        assertTrue(response.getBody().get("error").toString().contains("Authentication required"));

        verify(datasetService, never()).getDatasetForUser(anyLong(), anyString());
    }

    @Test
    void getUnifiedDataCount_WithDatasetId_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com")).thenReturn(testDataset);
        when(unifiedRowRepository.countByDataset(testDataset)).thenReturn(100L);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.getUnifiedDataCount(1L, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(100L, response.getBody().get("count"));
        assertEquals(1L, response.getBody().get("datasetId"));

        verify(datasetService).getDatasetForUser(1L, "test@example.com");
        verify(unifiedRowRepository).countByDataset(testDataset);
    }

    @Test
    void getUnifiedDataCount_WithoutDatasetId_CountsAll() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.listAllForUser("test@example.com")).thenReturn(Arrays.asList(testDataset));
        when(unifiedRowRepository.countByDatasetIn(anyList())).thenReturn(150L);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.getUnifiedDataCount(null, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(150L, response.getBody().get("count"));

        verify(datasetService).listAllForUser("test@example.com");
        verify(unifiedRowRepository).countByDatasetIn(anyList());
    }

    @Test
    void getUnifiedDataCount_NoAuthentication_ReturnsInternalServerError() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.getUnifiedDataCount(1L, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertTrue(response.getBody().containsKey("error"));
        assertTrue(response.getBody().get("error").toString().contains("Authentication required"));

        verify(datasetService, never()).getDatasetForUser(anyLong(), anyString());
    }

    @Test
    void clearUnifiedData_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com")).thenReturn(testDataset);
        when(unifiedRowRepository.countByDataset(testDataset)).thenReturn(50L);
        doNothing().when(unifiedRowRepository).deleteByDataset(testDataset);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.clearUnifiedData(1L, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue((Boolean) response.getBody().get("success"));
        assertEquals(50L, response.getBody().get("deletedRecords"));
        assertEquals(1L, response.getBody().get("datasetId"));

        verify(datasetService).getDatasetForUser(1L, "test@example.com");
        verify(unifiedRowRepository).deleteByDataset(testDataset);
    }

    @Test
    void clearUnifiedData_NoAuthentication_ReturnsInternalServerError() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.clearUnifiedData(1L, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertFalse((Boolean) response.getBody().get("success"));
        assertTrue(response.getBody().get("error").toString().contains("Authentication required"));

        verify(datasetService, never()).getDatasetForUser(anyLong(), anyString());
        verify(unifiedRowRepository, never()).deleteByDataset(any());
    }

    @Test
    void clearUnifiedData_ServiceThrowsException_ReturnsInternalServerError() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com"))
                .thenThrow(new RuntimeException("Dataset not found"));

        ResponseEntity<Map<String, Object>> response =
                dataIntegrationController.clearUnifiedData(1L, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertFalse((Boolean) response.getBody().get("success"));
        assertTrue(response.getBody().get("error").toString().contains("Dataset not found"));
    }
}
