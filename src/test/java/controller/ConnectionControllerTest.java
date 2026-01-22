package controller;

import org.example.controllers.ConnectionController;
import org.example.models.dto.ConnectionRequest;
import org.example.models.dto.ConnectionResponse;
import org.example.models.dto.TableSelectionDTO;
import org.example.models.dto.TableSelectionUpdateRequest;
import org.example.service.ConnectionService;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ConnectionControllerTest {

    @Mock
    private ConnectionService connectionService;

    @Mock
    private Authentication authentication;

    @InjectMocks
    private ConnectionController connectionController;

    private ConnectionResponse testResponse;
    private ConnectionRequest testRequest;

    @BeforeEach
    void setUp() {
        testResponse = new ConnectionResponse(
                "conn-123",
                "source-123",
                "Test Source",
                "dest-123",
                "Test Destination",
                "LOAD",
                List.of(),
                "test@example.com",
                Instant.now(),
                1L
        );

        testRequest = new ConnectionRequest(
                "source-123",
                "dest-123",
                "LOAD",
                1L,
                List.of()
        );
    }

    @Test
    void listConnections_WithAuthentication_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(connectionService.listConnections("test@example.com", null))
                .thenReturn(Arrays.asList(testResponse));

        ResponseEntity<List<ConnectionResponse>> response =
                connectionController.listConnections(null, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(1, response.getBody().size());
        assertEquals("conn-123", response.getBody().get(0).id());

        verify(connectionService).listConnections("test@example.com", null);
    }

    @Test
    void listConnections_WithDatasetIdFilter_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(connectionService.listConnections("test@example.com", 1L))
                .thenReturn(Arrays.asList(testResponse));

        ResponseEntity<List<ConnectionResponse>> response =
                connectionController.listConnections(1L, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(1, response.getBody().size());

        verify(connectionService).listConnections("test@example.com", 1L);
    }

    @Test
    void listConnections_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                connectionController.listConnections(null, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(connectionService, never()).listConnections(anyString(), any());
    }

    @Test
    void listConnections_NullAuthentication_ThrowsUnauthorized() {
        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                connectionController.listConnections(null, null)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(connectionService, never()).listConnections(anyString(), any());
    }

    @Test
    void createConnection_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(connectionService.createConnection(any(), eq("test@example.com"), eq("test@example.com")))
                .thenReturn(testResponse);

        ResponseEntity<ConnectionResponse> response =
                connectionController.createConnection(testRequest, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("conn-123", response.getBody().id());
        assertEquals("source-123", response.getBody().sourceId());

        verify(connectionService).createConnection(testRequest, "test@example.com", "test@example.com");
    }

    @Test
    void createConnection_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                connectionController.createConnection(testRequest, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(connectionService, never()).createConnection(any(), anyString(), anyString());
    }

    @Test
    void updateTableSelection_Success() {
        TableSelectionDTO selection = new TableSelectionDTO("users", "public", List.of("id", "email"));
        TableSelectionUpdateRequest updateRequest = new TableSelectionUpdateRequest(Arrays.asList(selection));

        ConnectionResponse updatedResponse = new ConnectionResponse(
                "conn-123",
                "source-123",
                "Test Source",
                "dest-123",
                "Test Destination",
                "LOAD",
                Arrays.asList(selection),
                "test@example.com",
                Instant.now(),
                1L
        );

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(connectionService.updateTableSelection("conn-123", updateRequest, "test@example.com"))
                .thenReturn(updatedResponse);

        ResponseEntity<ConnectionResponse> response =
                connectionController.updateTableSelection("conn-123", updateRequest, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(1, response.getBody().tableSelections().size());
        assertEquals("users", response.getBody().tableSelections().get(0).tableName());

        verify(connectionService).updateTableSelection("conn-123", updateRequest, "test@example.com");
    }

    @Test
    void updateTableSelection_NoAuthentication_ThrowsUnauthorized() {
        TableSelectionUpdateRequest updateRequest = new TableSelectionUpdateRequest(List.of());
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                connectionController.updateTableSelection("conn-123", updateRequest, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(connectionService, never()).updateTableSelection(anyString(), any(), anyString());
    }

    @Test
    void updateTableSelection_NullAuthentication_ThrowsUnauthorized() {
        TableSelectionUpdateRequest updateRequest = new TableSelectionUpdateRequest(List.of());

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                connectionController.updateTableSelection("conn-123", updateRequest, null)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(connectionService, never()).updateTableSelection(anyString(), any(), anyString());
    }
}
