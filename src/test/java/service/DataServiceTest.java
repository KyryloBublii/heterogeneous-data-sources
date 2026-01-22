package service;

import org.example.models.entity.ApplicationUser;
import org.example.models.entity.IngestionRun;
import org.example.models.enums.RunStatus;
import org.example.repository.IngestionRunRepository;
import org.example.repository.UserRepository;
import org.example.service.DataService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DataServiceTest {

    @Mock
    private IngestionRunRepository ingestionRunRepository;

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private DataService dataService;

    private ApplicationUser testUser;
    private IngestionRun testRun;

    @BeforeEach
    void setUp() {
        testUser = new ApplicationUser();
        testUser.setId(1L);
        testUser.setEmail("test@example.com");
        testUser.setName("Test User");

        testRun = new IngestionRun();
        testRun.setId(1L);
        testRun.setIngestionUid("run-123");
        testRun.setRunStatus(RunStatus.SUCCESS);
        testRun.setStartedAt(Instant.now());
        testRun.setRowsRead(100);
        testRun.setRowsStored(100);
    }

    @Test
    void getRecentRuns_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        Page<IngestionRun> page = new PageImpl<>(Arrays.asList(testRun));
        when(ingestionRunRepository.findBySource_ApplicationUser_Id(eq(1L), any(Pageable.class)))
                .thenReturn(page);

        List<IngestionRun> result = dataService.getRecentRuns("test@example.com", 10);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("run-123", result.get(0).getIngestionUid());

        verify(userRepository).findByEmail("test@example.com");
        verify(ingestionRunRepository).findBySource_ApplicationUser_Id(eq(1L), any(Pageable.class));
    }

    @Test
    void getRecentRuns_WithCustomLimit_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        Page<IngestionRun> page = new PageImpl<>(Arrays.asList(testRun));
        when(ingestionRunRepository.findBySource_ApplicationUser_Id(eq(1L), any(Pageable.class)))
                .thenReturn(page);

        List<IngestionRun> result = dataService.getRecentRuns("test@example.com", 5);

        assertNotNull(result);
        assertEquals(1, result.size());

        verify(ingestionRunRepository).findBySource_ApplicationUser_Id(eq(1L), any(Pageable.class));
    }

    @Test
    void getRecentRuns_NegativeLimit_UsesMinimumOfOne() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        Page<IngestionRun> page = new PageImpl<>(Arrays.asList(testRun));
        when(ingestionRunRepository.findBySource_ApplicationUser_Id(eq(1L), any(Pageable.class)))
                .thenReturn(page);

        List<IngestionRun> result = dataService.getRecentRuns("test@example.com", -5);

        assertNotNull(result);
        verify(ingestionRunRepository).findBySource_ApplicationUser_Id(eq(1L), any(Pageable.class));
    }

    @Test
    void getRecentRuns_UserNotFound_ThrowsException() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class, () ->
                dataService.getRecentRuns("test@example.com", 10)
        );

        verify(ingestionRunRepository, never()).findBySource_ApplicationUser_Id(anyLong(), any());
    }

    @Test
    void getRecentRuns_EmptyEmail_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                dataService.getRecentRuns("", 10)
        );

        verify(userRepository, never()).findByEmail(anyString());
        verify(ingestionRunRepository, never()).findBySource_ApplicationUser_Id(anyLong(), any());
    }

    @Test
    void getRecentRuns_NullEmail_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                dataService.getRecentRuns(null, 10)
        );

        verify(userRepository, never()).findByEmail(anyString());
        verify(ingestionRunRepository, never()).findBySource_ApplicationUser_Id(anyLong(), any());
    }

    @Test
    void getAllRuns_Success() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.of(testUser));
        when(ingestionRunRepository.findAllBySource_ApplicationUser_Id(eq(1L), any(Sort.class)))
                .thenReturn(Arrays.asList(testRun));

        List<IngestionRun> result = dataService.getAllRuns("test@example.com");

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("run-123", result.get(0).getIngestionUid());

        verify(userRepository).findByEmail("test@example.com");
        verify(ingestionRunRepository).findAllBySource_ApplicationUser_Id(eq(1L), any(Sort.class));
    }

    @Test
    void getAllRuns_UserNotFound_ThrowsException() {
        when(userRepository.findByEmail("test@example.com")).thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class, () ->
                dataService.getAllRuns("test@example.com")
        );

        verify(ingestionRunRepository, never()).findAllBySource_ApplicationUser_Id(anyLong(), any());
    }

    @Test
    void getAllRuns_EmptyEmail_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                dataService.getAllRuns("")
        );

        verify(userRepository, never()).findByEmail(anyString());
        verify(ingestionRunRepository, never()).findAllBySource_ApplicationUser_Id(anyLong(), any());
    }

    @Test
    void getAllRuns_NullEmail_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                dataService.getAllRuns(null)
        );

        verify(userRepository, never()).findByEmail(anyString());
        verify(ingestionRunRepository, never()).findAllBySource_ApplicationUser_Id(anyLong(), any());
    }
}
