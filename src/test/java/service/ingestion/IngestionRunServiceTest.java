package service.ingestion;

import org.example.models.entity.IngestionRun;
import org.example.models.enums.RunStatus;
import org.example.repository.IngestionRunRepository;
import org.example.service.ingestion.IngestionRunService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IngestionRunServiceTest {

    @Mock
    private IngestionRunRepository ingestionRunRepository;

    private IngestionRunService service;

    @BeforeEach
    void setUp() {
        service = new IngestionRunService(ingestionRunRepository);
    }

    @Test
    void testMarkRunning_shouldSetStatusToRunning() {
        IngestionRun run = new IngestionRun();

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markRunning(run);

        assertEquals(RunStatus.RUNNING, result.getRunStatus());
        verify(ingestionRunRepository).save(run);
    }

    @Test
    void testMarkRunning_shouldSetStartedAtIfNull() {
        IngestionRun run = new IngestionRun();
        run.setStartedAt(null);

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        Instant before = Instant.now();
        IngestionRun result = service.markRunning(run);
        Instant after = Instant.now();

        assertNotNull(result.getStartedAt());
        assertTrue(result.getStartedAt().isAfter(before.minusSeconds(1)));
        assertTrue(result.getStartedAt().isBefore(after.plusSeconds(1)));
    }

    @Test
    void testMarkRunning_shouldNotOverwriteExistingStartedAt() {
        IngestionRun run = new IngestionRun();
        Instant existingStartTime = Instant.parse("2024-01-01T10:00:00Z");
        run.setStartedAt(existingStartTime);

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markRunning(run);

        assertEquals(existingStartTime, result.getStartedAt());
    }

    @Test
    void testMarkRunning_shouldReturnSavedRun() {
        IngestionRun run = new IngestionRun();
        IngestionRun savedRun = new IngestionRun();
        savedRun.setRunStatus(RunStatus.RUNNING);

        when(ingestionRunRepository.save(run)).thenReturn(savedRun);

        IngestionRun result = service.markRunning(run);

        assertSame(savedRun, result);
    }

    @Test
    void testMarkSuccess_shouldSetStatusToSuccess() {
        IngestionRun run = new IngestionRun();

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markSuccess(run, 100, 95);

        assertEquals(RunStatus.SUCCESS, result.getRunStatus());
        verify(ingestionRunRepository).save(run);
    }

    @Test
    void testMarkSuccess_shouldSetRowCounts() {
        IngestionRun run = new IngestionRun();

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markSuccess(run, 100, 95);

        assertEquals(100, result.getRowsRead());
        assertEquals(95, result.getRowsStored());
    }

    @Test
    void testMarkSuccess_shouldSetEndedAt() {
        IngestionRun run = new IngestionRun();

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        Instant before = Instant.now();
        IngestionRun result = service.markSuccess(run, 50, 50);
        Instant after = Instant.now();

        assertNotNull(result.getEndedAt());
        assertTrue(result.getEndedAt().isAfter(before.minusSeconds(1)));
        assertTrue(result.getEndedAt().isBefore(after.plusSeconds(1)));
    }

    @Test
    void testMarkSuccess_shouldClearErrorMessage() {
        IngestionRun run = new IngestionRun();
        run.setErrorMessage("Previous error");

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markSuccess(run, 10, 10);

        assertNull(result.getErrorMessage());
    }

    @Test
    void testMarkSuccess_withZeroRows_shouldStillSucceed() {
        IngestionRun run = new IngestionRun();

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markSuccess(run, 0, 0);

        assertEquals(RunStatus.SUCCESS, result.getRunStatus());
        assertEquals(0, result.getRowsRead());
        assertEquals(0, result.getRowsStored());
    }

    @Test
    void testMarkFailure_shouldSetStatusToFailed() {
        IngestionRun run = new IngestionRun();

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markFailure(run, "Connection timeout");

        assertEquals(RunStatus.FAILED, result.getRunStatus());
        verify(ingestionRunRepository).save(run);
    }

    @Test
    void testMarkFailure_shouldSetErrorMessage() {
        IngestionRun run = new IngestionRun();
        String errorMessage = "Database connection failed";

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markFailure(run, errorMessage);

        assertEquals(errorMessage, result.getErrorMessage());
    }

    @Test
    void testMarkFailure_shouldSetEndedAt() {
        IngestionRun run = new IngestionRun();

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        Instant before = Instant.now();
        IngestionRun result = service.markFailure(run, "Error occurred");
        Instant after = Instant.now();

        assertNotNull(result.getEndedAt());
        assertTrue(result.getEndedAt().isAfter(before.minusSeconds(1)));
        assertTrue(result.getEndedAt().isBefore(after.plusSeconds(1)));
    }

    @Test
    void testMarkFailure_withNullMessage_shouldAcceptNull() {
        IngestionRun run = new IngestionRun();

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markFailure(run, null);

        assertEquals(RunStatus.FAILED, result.getRunStatus());
        assertNull(result.getErrorMessage());
    }

    @Test
    void testMarkFailure_withEmptyMessage_shouldAcceptEmpty() {
        IngestionRun run = new IngestionRun();

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markFailure(run, "");

        assertEquals(RunStatus.FAILED, result.getRunStatus());
        assertEquals("", result.getErrorMessage());
    }

    @Test
    void testMarkFailure_withLongMessage_shouldAcceptLongMessage() {
        IngestionRun run = new IngestionRun();
        String longMessage = "Error: ".repeat(100);

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        IngestionRun result = service.markFailure(run, longMessage);

        assertEquals(longMessage, result.getErrorMessage());
    }

    @Test
    void testMarkSuccess_shouldReturnSavedRun() {
        IngestionRun run = new IngestionRun();
        IngestionRun savedRun = new IngestionRun();
        savedRun.setRunStatus(RunStatus.SUCCESS);

        when(ingestionRunRepository.save(run)).thenReturn(savedRun);

        IngestionRun result = service.markSuccess(run, 10, 10);

        assertSame(savedRun, result);
    }

    @Test
    void testMarkFailure_shouldReturnSavedRun() {
        IngestionRun run = new IngestionRun();
        IngestionRun savedRun = new IngestionRun();
        savedRun.setRunStatus(RunStatus.FAILED);

        when(ingestionRunRepository.save(run)).thenReturn(savedRun);

        IngestionRun result = service.markFailure(run, "Error");

        assertSame(savedRun, result);
    }

    @Test
    void testAllMethods_shouldPersistChanges() {
        IngestionRun run = new IngestionRun();
        ArgumentCaptor<IngestionRun> captor = ArgumentCaptor.forClass(IngestionRun.class);

        when(ingestionRunRepository.save(any(IngestionRun.class))).thenReturn(run);

        // Test running
        service.markRunning(run);
        verify(ingestionRunRepository, times(1)).save(captor.capture());
        assertEquals(RunStatus.RUNNING, captor.getValue().getRunStatus());

        // Test success
        service.markSuccess(run, 100, 95);
        verify(ingestionRunRepository, times(2)).save(captor.capture());
        assertEquals(RunStatus.SUCCESS, captor.getValue().getRunStatus());

        // Test failure
        service.markFailure(run, "Error");
        verify(ingestionRunRepository, times(3)).save(captor.capture());
        assertEquals(RunStatus.FAILED, captor.getValue().getRunStatus());
    }
}
