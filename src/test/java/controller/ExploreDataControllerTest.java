package controller;

import org.example.controllers.ExploreDataController;
import org.example.models.entity.Dataset;
import org.example.models.entity.UnifiedRow;
import org.example.models.enums.DatasetStatus;
import org.example.repository.UnifiedRowRepository;
import org.example.service.DatasetService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ExploreDataControllerTest {

    @Mock
    private DatasetService datasetService;

    @Mock
    private UnifiedRowRepository unifiedRowRepository;

    @Mock
    private Authentication authentication;

    @InjectMocks
    private ExploreDataController exploreDataController;

    private Dataset testDataset;
    private UnifiedRow testRow1;
    private UnifiedRow testRow2;

    @BeforeEach
    void setUp() {
        testDataset = new Dataset();
        testDataset.setId(1L);
        testDataset.setName("Test Dataset");
        testDataset.setStatus(DatasetStatus.ACTIVE);

        testRow1 = new UnifiedRow();
        testRow1.setId(1L);
        testRow1.setUnifiedRowUid("row-1");
        testRow1.setDataset(testDataset);
        testRow1.setData(Map.of("id", 1, "name", "Alice", "email", "alice@example.com"));
        testRow1.setIngestedAt(Instant.now());

        testRow2 = new UnifiedRow();
        testRow2.setId(2L);
        testRow2.setUnifiedRowUid("row-2");
        testRow2.setDataset(testDataset);
        testRow2.setData(Map.of("id", 2, "name", "Bob", "email", "bob@example.com"));
        testRow2.setIngestedAt(Instant.now());
    }

    @Test
    void loadDataset_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com")).thenReturn(testDataset);
        when(unifiedRowRepository.findByDataset(testDataset)).thenReturn(Arrays.asList(testRow1, testRow2));

        List<Map<String, Object>> result = exploreDataController.loadDataset(1L, null, authentication);

        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("Alice", result.get(0).get("name"));
        assertEquals("Bob", result.get(1).get("name"));

        verify(datasetService).getDatasetForUser(1L, "test@example.com");
        verify(unifiedRowRepository).findByDataset(testDataset);
    }

    @Test
    void loadDataset_WithFilter_ReturnsFilteredResults() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com")).thenReturn(testDataset);
        when(unifiedRowRepository.findByDataset(testDataset)).thenReturn(Arrays.asList(testRow1, testRow2));

        List<Map<String, Object>> result = exploreDataController.loadDataset(1L, "name=Alice", authentication);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("Alice", result.get(0).get("name"));

        verify(datasetService).getDatasetForUser(1L, "test@example.com");
    }

    @Test
    void loadDataset_WithInvalidFilter_ReturnsAllResults() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com")).thenReturn(testDataset);
        when(unifiedRowRepository.findByDataset(testDataset)).thenReturn(Arrays.asList(testRow1, testRow2));

        List<Map<String, Object>> result = exploreDataController.loadDataset(1L, "invalidfilter", authentication);

        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    void loadDataset_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                exploreDataController.loadDataset(1L, null, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(datasetService, never()).getDatasetForUser(anyLong(), anyString());
    }

    @Test
    void loadDataset_NullAuthentication_ThrowsUnauthorized() {
        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                exploreDataController.loadDataset(1L, null, null)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(datasetService, never()).getDatasetForUser(anyLong(), anyString());
    }

    @Test
    void exportDataset_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com")).thenReturn(testDataset);
        when(unifiedRowRepository.findByDataset(testDataset)).thenReturn(Arrays.asList(testRow1, testRow2));

        ResponseEntity<byte[]> response = exploreDataController.exportDataset(1L, null, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());

        String csvContent = new String(response.getBody(), StandardCharsets.UTF_8);
        // Check that all headers are present (order may vary)
        assertTrue(csvContent.contains("id"));
        assertTrue(csvContent.contains("name"));
        assertTrue(csvContent.contains("email"));
        // Check data
        assertTrue(csvContent.contains("Alice"));
        assertTrue(csvContent.contains("Bob"));

        HttpHeaders headers = response.getHeaders();
        assertTrue(headers.getContentDisposition().toString().contains("attachment"));
        assertTrue(headers.getContentDisposition().toString().contains("Test Dataset"));

        verify(datasetService).getDatasetForUser(1L, "test@example.com");
        verify(unifiedRowRepository).findByDataset(testDataset);
    }

    @Test
    void exportDataset_WithFilter_ReturnsFilteredCSV() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com")).thenReturn(testDataset);
        when(unifiedRowRepository.findByDataset(testDataset)).thenReturn(Arrays.asList(testRow1, testRow2));

        ResponseEntity<byte[]> response = exploreDataController.exportDataset(1L, "name=Alice", authentication);

        assertNotNull(response);
        String csvContent = new String(response.getBody(), StandardCharsets.UTF_8);
        assertTrue(csvContent.contains("Alice"));
        assertFalse(csvContent.contains("Bob"));
    }

    @Test
    void exportDataset_EmptyData_ReturnsEmptyCSV() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com")).thenReturn(testDataset);
        when(unifiedRowRepository.findByDataset(testDataset)).thenReturn(Collections.emptyList());

        ResponseEntity<byte[]> response = exploreDataController.exportDataset(1L, null, authentication);

        assertNotNull(response);
        assertEquals(0, response.getBody().length);
    }

    @Test
    void exportDataset_WithCommasInData_ProperlyEscapes() {
        UnifiedRow rowWithComma = new UnifiedRow();
        rowWithComma.setId(3L);
        rowWithComma.setDataset(testDataset);
        rowWithComma.setData(Map.of("id", 3, "name", "Smith, John", "email", "john@example.com"));

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(datasetService.getDatasetForUser(1L, "test@example.com")).thenReturn(testDataset);
        when(unifiedRowRepository.findByDataset(testDataset)).thenReturn(Arrays.asList(rowWithComma));

        ResponseEntity<byte[]> response = exploreDataController.exportDataset(1L, null, authentication);

        assertNotNull(response);
        String csvContent = new String(response.getBody(), StandardCharsets.UTF_8);
        assertTrue(csvContent.contains("\"Smith, John\""));
    }

    @Test
    void exportDataset_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                exploreDataController.exportDataset(1L, null, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(datasetService, never()).getDatasetForUser(anyLong(), anyString());
    }
}
