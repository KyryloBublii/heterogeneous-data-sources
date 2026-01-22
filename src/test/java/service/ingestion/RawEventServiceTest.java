package service.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.models.entity.Dataset;
import org.example.models.entity.IngestionRun;
import org.example.models.entity.RawEvent;
import org.example.models.entity.Source;
import org.example.repository.RawEventRepository;
import org.example.service.ingestion.RawEventService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RawEventServiceTest {

    @Mock
    private RawEventRepository rawEventRepository;

    private ObjectMapper objectMapper;
    private RawEventService service;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        service = new RawEventService(rawEventRepository, objectMapper);
    }

    @Test
    void testWrite_withSingleRecord_shouldPersist() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();
        List<Map<String, Object>> records = List.of(Map.of("id", 1, "name", "John"));

        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Collections.emptySet());
        when(rawEventRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.write(source, run, records);

        assertEquals(1, result);
        verify(rawEventRepository).saveAll(anyList());
    }

    @Test
    void testWrite_withMultipleRecords_shouldPersistAll() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "name", "John"),
                Map.of("id", 2, "name", "Jane"),
                Map.of("id", 3, "name", "Bob")
        );

        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Collections.emptySet());
        when(rawEventRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.write(source, run, records);

        assertEquals(3, result);
    }

    @Test
    void testWrite_withDuplicatesInBatch_shouldDeduplicateBeforePersisting() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();

        Map<String, Object> record1 = Map.of("id", 1, "name", "John");
        Map<String, Object> record2 = Map.of("id", 1, "name", "John"); // Duplicate

        List<Map<String, Object>> records = List.of(record1, record2);

        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Collections.emptySet());
        when(rawEventRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.write(source, run, records);

        assertEquals(1, result); // Only 1 unique record
        ArgumentCaptor<List<RawEvent>> captor = ArgumentCaptor.forClass(List.class);
        verify(rawEventRepository).saveAll(captor.capture());
        assertEquals(1, captor.getValue().size());
    }

    @Test
    void testWrite_withExistingHash_shouldSkipRecord() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();
        Map<String, Object> record = Map.of("id", 1, "name", "John");
        List<Map<String, Object>> records = List.of(record);

        // Simulate that this record already exists
        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Set.of(computeHash(record)));

        int result = service.write(source, run, records);

        assertEquals(0, result);
        verify(rawEventRepository, never()).saveAll(anyList());
    }

    @Test
    void testWrite_withPartialExisting_shouldOnlyPersistNew() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();

        Map<String, Object> record1 = Map.of("id", 1, "name", "John");
        Map<String, Object> record2 = Map.of("id", 2, "name", "Jane");

        List<Map<String, Object>> records = List.of(record1, record2);

        // Only record1 already exists
        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Set.of(computeHash(record1)));
        when(rawEventRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.write(source, run, records);

        assertEquals(1, result); // Only record2 is new
        ArgumentCaptor<List<RawEvent>> captor = ArgumentCaptor.forClass(List.class);
        verify(rawEventRepository).saveAll(captor.capture());
        assertEquals(1, captor.getValue().size());
    }

    @Test
    void testWrite_withEmptyRecords_shouldReturnZero() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();
        List<Map<String, Object>> records = Collections.emptyList();

        int result = service.write(source, run, records);

        assertEquals(0, result);
        verify(rawEventRepository, never()).saveAll(anyList());
    }

    @Test
    void testWrite_shouldSetAllRawEventFields() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();
        Map<String, Object> record = Map.of("id", 1, "name", "John");
        List<Map<String, Object>> records = List.of(record);

        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Collections.emptySet());
        when(rawEventRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        service.write(source, run, records);

        ArgumentCaptor<List<RawEvent>> captor = ArgumentCaptor.forClass(List.class);
        verify(rawEventRepository).saveAll(captor.capture());

        RawEvent event = captor.getValue().get(0);
        assertNotNull(event.getRawEventUid());
        assertEquals(source, event.getSource());
        assertEquals(run, event.getIngestionRun());
        assertEquals(source.getDataset(), event.getDataset());
        assertEquals(record, event.getPayload());
        assertNotNull(event.getPayloadHash());
        assertNotNull(event.getCreatedAt());
    }

    @Test
    void testWrite_shouldGenerateConsistentHashes() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();

        Map<String, Object> record1 = Map.of("id", 1, "name", "John");
        Map<String, Object> record2 = Map.of("id", 1, "name", "John"); // Same content

        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Collections.emptySet());
        when(rawEventRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        service.write(source, run, List.of(record1));
        ArgumentCaptor<List<RawEvent>> captor1 = ArgumentCaptor.forClass(List.class);
        verify(rawEventRepository, times(1)).saveAll(captor1.capture());
        String hash1 = captor1.getValue().get(0).getPayloadHash();

        reset(rawEventRepository);
        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Collections.emptySet());
        when(rawEventRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        service.write(source, run, List.of(record2));
        ArgumentCaptor<List<RawEvent>> captor2 = ArgumentCaptor.forClass(List.class);
        verify(rawEventRepository).saveAll(captor2.capture());
        String hash2 = captor2.getValue().get(0).getPayloadHash();

        assertEquals(hash1, hash2);
    }

    @Test
    void testWrite_withConcurrentInsertion_shouldFallbackToIndividualSave() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();
        Map<String, Object> record = Map.of("id", 1, "name", "John");
        List<Map<String, Object>> records = List.of(record);

        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Collections.emptySet());

        // Simulate batch save failure
        when(rawEventRepository.saveAll(anyList()))
                .thenThrow(new RuntimeException("Concurrent modification"));

        // Individual save should work
        when(rawEventRepository.existsBySourceAndPayloadHash(any(), anyString()))
                .thenReturn(false);
        when(rawEventRepository.save(any(RawEvent.class))).thenAnswer(i -> i.getArgument(0));

        int result = service.write(source, run, records);

        assertEquals(1, result);
        verify(rawEventRepository).saveAll(anyList());
        verify(rawEventRepository).save(any(RawEvent.class));
    }

    @Test
    void testWrite_withConcurrentInsertionDuplicate_shouldSkip() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();
        Map<String, Object> record = Map.of("id", 1, "name", "John");
        List<Map<String, Object>> records = List.of(record);

        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Collections.emptySet());

        // Simulate batch save failure
        when(rawEventRepository.saveAll(anyList()))
                .thenThrow(new RuntimeException("Concurrent modification"));

        // Record now exists (inserted by another process)
        when(rawEventRepository.existsBySourceAndPayloadHash(any(), anyString()))
                .thenReturn(true);

        int result = service.write(source, run, records);

        assertEquals(0, result);
        verify(rawEventRepository, never()).save(any(RawEvent.class));
    }

    @Test
    void testWrite_withDataIntegrityViolation_shouldHandleGracefully() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();
        Map<String, Object> record = Map.of("id", 1, "name", "John");
        List<Map<String, Object>> records = List.of(record);

        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Collections.emptySet());

        when(rawEventRepository.saveAll(anyList()))
                .thenThrow(new RuntimeException("Constraint violation"));

        when(rawEventRepository.existsBySourceAndPayloadHash(any(), anyString()))
                .thenReturn(false);

        when(rawEventRepository.save(any(RawEvent.class)))
                .thenThrow(new DataIntegrityViolationException("Duplicate key"));

        int result = service.write(source, run, records);

        assertEquals(0, result);
    }

    @Test
    void testWrite_shouldGenerateUniqueUids() {
        Source source = createSource();
        IngestionRun run = new IngestionRun();
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1),
                Map.of("id", 2),
                Map.of("id", 3)
        );

        when(rawEventRepository.findExistingPayloadHashes(any(), anyList()))
                .thenReturn(Collections.emptySet());
        when(rawEventRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        service.write(source, run, records);

        ArgumentCaptor<List<RawEvent>> captor = ArgumentCaptor.forClass(List.class);
        verify(rawEventRepository).saveAll(captor.capture());

        List<RawEvent> events = captor.getValue();
        Set<String> uids = new HashSet<>();
        for (RawEvent event : events) {
            uids.add(event.getRawEventUid());
        }

        assertEquals(3, uids.size()); // All UIDs should be unique
    }

    private Source createSource() {
        Source source = new Source();
        source.setId(1L);
        source.setName("test-source");
        Dataset dataset = new Dataset();
        dataset.setId(1L);
        source.setDataset(dataset);
        return source;
    }

    private String computeHash(Map<String, Object> record) {
        try {
            String json = objectMapper.writeValueAsString(record);
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder();
            for (byte b : hashed) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
