package controller;

import org.example.controllers.DataController;
import org.example.models.entity.IngestionRun;
import org.example.models.enums.RunStatus;
import org.example.service.DataService;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataControllerTest {

    @Mock
    private DataService dataService;

    @Mock
    private Authentication authentication;

    @InjectMocks
    private DataController dataController;

    private IngestionRun testRun;

    @BeforeEach
    void setUp() {
        testRun = new IngestionRun();
        testRun.setId(1L);
        testRun.setIngestionUid("run-123");
        testRun.setRunStatus(RunStatus.SUCCESS);
        testRun.setStartedAt(Instant.now());
        testRun.setRowsRead(100);
        testRun.setRowsStored(100);
    }

    @Test
    void getAllRuns_WithAuthentication_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(dataService.getAllRuns("test@example.com")).thenReturn(Arrays.asList(testRun));

        ResponseEntity<List<IngestionRun>> response = dataController.getAllRuns(authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(1, response.getBody().size());
        assertEquals("run-123", response.getBody().get(0).getIngestionUid());

        verify(dataService).getAllRuns("test@example.com");
    }

    @Test
    void getAllRuns_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                dataController.getAllRuns(authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(dataService, never()).getAllRuns(anyString());
    }

    @Test
    void getAllRuns_NullAuthentication_ThrowsUnauthorized() {
        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                dataController.getAllRuns(null)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(dataService, never()).getAllRuns(anyString());
    }

    @Test
    void getRecentRuns_WithDefaultLimit_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(dataService.getRecentRuns("test@example.com", 10)).thenReturn(Arrays.asList(testRun));

        ResponseEntity<List<IngestionRun>> response = dataController.getRecentRuns(10, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals(1, response.getBody().size());

        verify(dataService).getRecentRuns("test@example.com", 10);
    }

    @Test
    void getRecentRuns_WithCustomLimit_Success() {
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        when(dataService.getRecentRuns("test@example.com", 5)).thenReturn(Arrays.asList(testRun));

        ResponseEntity<List<IngestionRun>> response = dataController.getRecentRuns(5, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(1, response.getBody().size());

        verify(dataService).getRecentRuns("test@example.com", 5);
    }

    @Test
    void getRecentRuns_NoAuthentication_ThrowsUnauthorized() {
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                dataController.getRecentRuns(10, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(dataService, never()).getRecentRuns(anyString(), anyInt());
    }

    @Test
    void getRecentRuns_NullAuthentication_ThrowsUnauthorized() {
        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                dataController.getRecentRuns(10, null)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(dataService, never()).getRecentRuns(anyString(), anyInt());
    }
}
