package controller;

import org.example.controllers.SourceController;
import org.example.models.dto.ConnectionTestRequest;
import org.example.models.dto.SourceDTO;
import org.example.models.entity.ApplicationUser;
import org.example.models.entity.Source;
import org.example.models.enums.SourceRole;
import org.example.models.enums.SourceStatus;
import org.example.models.enums.SourceType;
import org.example.service.FileStorageService;
import org.example.service.SourceService;
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SourceControllerTest {

    @Mock
    private SourceService sourceService;

    @Mock
    private FileStorageService fileStorageService;

    @Mock
    private Authentication authentication;

    @InjectMocks
    private SourceController sourceController;

    private ApplicationUser testUser;
    private Source testSource;
    private SourceDTO testSourceDTO;

    @BeforeEach
    void setUp() {
        testUser = new ApplicationUser();
        testUser.setId(1L);
        testUser.setEmail("test@example.com");
        testUser.setName("Test User");

        testSource = new Source();
        testSource.setId(1L);
        testSource.setSourceUid("source-uid");
        testSource.setName("Test Source");
        testSource.setType(SourceType.CSV);
        testSource.setRole(SourceRole.SOURCE);
        testSource.setApplicationUser(testUser);

        testSourceDTO = new SourceDTO(
                "source-uid",        // id
                "Test Source",       // name
                SourceType.CSV,      // type
                SourceRole.SOURCE,   // role
                Map.of("path", "/test/path.csv"), // config
                SourceStatus.ACTIVE, // status
                null,                // createdAt
                null,                // updatedAt
                null                 // datasetId
        );
    }

    @Test
    void addSource_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(sourceService.createSource(any(SourceDTO.class), eq("test@example.com")))
                .thenReturn(testSource);

        ResponseEntity<SourceDTO> response = sourceController.addSource(testSourceDTO, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("source-uid", response.getBody().id());
        assertEquals("Test Source", response.getBody().name());

        verify(sourceService).createSource(any(SourceDTO.class), eq("test@example.com"));
    }

    @Test
    void addSource_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                sourceController.addSource(testSourceDTO, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(sourceService, never()).createSource(any(), anyString());
    }

    @Test
    void testConnection_Success() {
        ConnectionTestRequest request = new ConnectionTestRequest(
                SourceType.CSV,
                Map.of("path", "/test/path.csv")
        );

        when(sourceService.testConnection(SourceType.CSV, Map.of("path", "/test/path.csv")))
                .thenReturn(true);

        ResponseEntity<Map<String, Object>> response = sourceController.testConnection(request);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue((Boolean) response.getBody().get("success"));

        verify(sourceService).testConnection(SourceType.CSV, Map.of("path", "/test/path.csv"));
    }

    @Test
    void testConnection_Failure() {
        ConnectionTestRequest request = new ConnectionTestRequest(
                SourceType.DB,
                Map.of("host", "localhost")
        );

        when(sourceService.testConnection(SourceType.DB, Map.of("host", "localhost")))
                .thenReturn(false);

        ResponseEntity<Map<String, Object>> response = sourceController.testConnection(request);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertFalse((Boolean) response.getBody().get("success"));

        verify(sourceService).testConnection(SourceType.DB, Map.of("host", "localhost"));
    }

    @Test
    void getAvailableSources_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(sourceService.listReusableSources("test@example.com"))
                .thenReturn(Arrays.asList(testSource));

        ResponseEntity<List<SourceDTO>> response = sourceController.getAvailableSources(authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(1, response.getBody().size());
        assertEquals("Test Source", response.getBody().get(0).name());

        verify(sourceService).listReusableSources("test@example.com");
    }

    @Test
    void getAvailableSources_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                sourceController.getAvailableSources(authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(sourceService, never()).listReusableSources(anyString());
    }

    @Test
    void getAllSources_WithoutFilters_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(sourceService.getAllSources(null, null, "test@example.com"))
                .thenReturn(Arrays.asList(testSource));

        ResponseEntity<List<SourceDTO>> response = sourceController.getAllSources(null, null, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(1, response.getBody().size());

        verify(sourceService).getAllSources(null, null, "test@example.com");
    }

    @Test
    void getAllSources_WithRoleFilter_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(sourceService.getAllSources(SourceRole.SOURCE, null, "test@example.com"))
                .thenReturn(Arrays.asList(testSource));

        ResponseEntity<List<SourceDTO>> response = sourceController.getAllSources(SourceRole.SOURCE, null, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(1, response.getBody().size());

        verify(sourceService).getAllSources(SourceRole.SOURCE, null, "test@example.com");
    }

    @Test
    void getAllSources_WithDatasetIdFilter_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(sourceService.getAllSources(null, 1L, "test@example.com"))
                .thenReturn(Arrays.asList(testSource));

        ResponseEntity<List<SourceDTO>> response = sourceController.getAllSources(null, 1L, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(1, response.getBody().size());

        verify(sourceService).getAllSources(null, 1L, "test@example.com");
    }

    @Test
    void getAllSources_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                sourceController.getAllSources(null, null, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(sourceService, never()).getAllSources(any(), any(), anyString());
    }

    @Test
    void getSource_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(sourceService.getSourceById("source-uid", "test@example.com"))
                .thenReturn(testSource);

        ResponseEntity<Source> response = sourceController.getSource("source-uid", authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("Test Source", response.getBody().getName());

        verify(sourceService).getSourceById("source-uid", "test@example.com");
    }

    @Test
    void getSource_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                sourceController.getSource("source-uid", authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(sourceService, never()).getSourceById(anyString(), anyString());
    }

    @Test
    void updateSource_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(sourceService.updateSource("source-uid", testSourceDTO, "test@example.com"))
                .thenReturn(testSource);

        ResponseEntity<Source> response = sourceController.updateSource("source-uid", testSourceDTO, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());

        verify(sourceService).updateSource("source-uid", testSourceDTO, "test@example.com");
    }

    @Test
    void updateSource_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                sourceController.updateSource("source-uid", testSourceDTO, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(sourceService, never()).updateSource(anyString(), any(), anyString());
    }
}
