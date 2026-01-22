package service.transform;

import org.example.models.entity.Source;
import org.example.service.TransformService;
import org.example.service.transform.RecordMerger;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RecordMergerTest {

    @Test
    void testMerge_withEmptyInputs_shouldNotModify() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        RecordMerger.merge(payloadBySource, unifiedPayload, null, null, null);

        assertTrue(payloadBySource.isEmpty());
        assertTrue(unifiedPayload.isEmpty());
    }

    @Test
    void testMerge_withPrimaryContext_shouldMergeToUnified() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        Source source = createSource(1L);
        Map<String, Object> payload = Map.of("name", "John", "age", 30);
        TransformService.RecordContext context = new TransformService.RecordContext(
                source, payload, Instant.now(), "user", "1", "key1"
        );

        RecordMerger.merge(payloadBySource, unifiedPayload, List.of(context), null, null);

        assertEquals("John", unifiedPayload.get("name"));
        assertEquals(30, unifiedPayload.get("age"));
    }

    @Test
    void testMerge_withPrimaryContext_shouldMergeToPayloadBySource() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        Source source = createSource(1L);
        Map<String, Object> payload = Map.of("name", "John");
        TransformService.RecordContext context = new TransformService.RecordContext(
                source, payload, Instant.now(), "user", "1", "key1"
        );

        RecordMerger.merge(payloadBySource, unifiedPayload, List.of(context), null, null);

        assertTrue(payloadBySource.containsKey(1L));
        assertEquals("John", payloadBySource.get(1L).get("name"));
    }

    @Test
    void testMerge_withRelationPayloads_shouldMergeToUnified() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        Map<String, Object> relationPayload = Map.of("email", "john@example.com");

        RecordMerger.merge(payloadBySource, unifiedPayload, null, List.of(relationPayload), null);

        assertEquals("john@example.com", unifiedPayload.get("email"));
    }

    @Test
    void testMerge_withRelatedContexts_shouldMergeToUnified() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        Source source = createSource(2L);
        Map<String, Object> payload = Map.of("phone", "555-1234");
        TransformService.RecordContext context = new TransformService.RecordContext(
                source, payload, Instant.now(), "contact", "100", "key1"
        );

        RecordMerger.merge(payloadBySource, unifiedPayload, null, null, List.of(context));

        assertEquals("555-1234", unifiedPayload.get("phone"));
    }

    @Test
    void testMerge_shouldNotOverwriteExistingValues() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();
        unifiedPayload.put("name", "Existing Name");

        Source source = createSource(1L);
        Map<String, Object> payload = Map.of("name", "New Name", "age", 30);
        TransformService.RecordContext context = new TransformService.RecordContext(
                source, payload, Instant.now(), "user", "1", "key1"
        );

        RecordMerger.merge(payloadBySource, unifiedPayload, List.of(context), null, null);

        assertEquals("Existing Name", unifiedPayload.get("name")); // Should not overwrite
        assertEquals(30, unifiedPayload.get("age")); // New field should be added
    }

    @Test
    void testMerge_withMultiplePrimaryContexts_shouldMergeAll() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        Source source1 = createSource(1L);
        Source source2 = createSource(2L);

        Map<String, Object> payload1 = new HashMap<>();
        payload1.put("name", "John");

        Map<String, Object> payload2 = new HashMap<>();
        payload2.put("email", "john@example.com");

        TransformService.RecordContext context1 = new TransformService.RecordContext(
                source1, payload1, Instant.now(), "user", "1", "key1"
        );
        TransformService.RecordContext context2 = new TransformService.RecordContext(
                source2, payload2, Instant.now(), "user", "1", "key2"
        );

        RecordMerger.merge(payloadBySource, unifiedPayload, List.of(context1, context2), null, null);

        assertEquals("John", unifiedPayload.get("name"));
        assertEquals("john@example.com", unifiedPayload.get("email"));
        assertTrue(payloadBySource.containsKey(1L));
        assertTrue(payloadBySource.containsKey(2L));
    }

    @Test
    void testMerge_withEmptyContextList_shouldHandleGracefully() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        assertDoesNotThrow(() -> {
            RecordMerger.merge(payloadBySource, unifiedPayload, Collections.emptyList(), null, null);
        });

        assertTrue(unifiedPayload.isEmpty());
    }

    @Test
    void testMerge_withNullRelationPayload_shouldSkip() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        List<Map<String, Object>> relationPayloads = Arrays.asList(
                null,
                Map.of("email", "test@example.com")
        );

        RecordMerger.merge(payloadBySource, unifiedPayload, null, relationPayloads, null);

        assertEquals("test@example.com", unifiedPayload.get("email"));
    }

    @Test
    void testMerge_withNullValues_shouldSkipNullFields() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        Map<String, Object> payload = new HashMap<>();
        payload.put("name", "John");
        payload.put("age", null);

        Source source = createSource(1L);
        TransformService.RecordContext context = new TransformService.RecordContext(
                source, payload, Instant.now(), "user", "1", "key1"
        );

        RecordMerger.merge(payloadBySource, unifiedPayload, List.of(context), null, null);

        assertEquals("John", unifiedPayload.get("name"));
        assertFalse(unifiedPayload.containsKey("age")); // Null values should not be merged
    }

    @Test
    void testMerge_withTimestampNormalization_shouldNormalizeTimestamps() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        Instant now = Instant.now();
        Map<String, Object> payload = Map.of("created_at", now);

        Source source = createSource(1L);
        TransformService.RecordContext context = new TransformService.RecordContext(
                source, payload, Instant.now(), "user", "1", "key1"
        );

        RecordMerger.merge(payloadBySource, unifiedPayload, List.of(context), null, null);

        assertNotNull(unifiedPayload.get("created_at"));
    }

    @Test
    void testMerge_withSourceWithoutId_shouldHandleGracefully() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        Source source = new Source(); // No ID set
        Map<String, Object> payload = Map.of("name", "John");
        TransformService.RecordContext context = new TransformService.RecordContext(
                source, payload, Instant.now(), "user", "1", "key1"
        );

        assertDoesNotThrow(() -> {
            RecordMerger.merge(payloadBySource, unifiedPayload, List.of(context), null, null);
        });

        assertEquals("John", unifiedPayload.get("name"));
    }

    @Test
    void testMerge_withAllInputTypes_shouldMergeInCorrectOrder() {
        Map<Long, Map<String, Object>> payloadBySource = new HashMap<>();
        Map<String, Object> unifiedPayload = new HashMap<>();

        Source source = createSource(1L);

        Map<String, Object> primaryPayload = new HashMap<>();
        primaryPayload.put("name", "John");

        TransformService.RecordContext primaryContext = new TransformService.RecordContext(
                source, primaryPayload, Instant.now(), "user", "1", "key1"
        );

        Map<String, Object> relationPayload = new HashMap<>();
        relationPayload.put("email", "john@example.com");

        Map<String, Object> relatedPayload = new HashMap<>();
        relatedPayload.put("phone", "555-1234");

        TransformService.RecordContext relatedContext = new TransformService.RecordContext(
                source, relatedPayload, Instant.now(), "contact", "100", "key2"
        );

        RecordMerger.merge(payloadBySource, unifiedPayload,
                List.of(primaryContext), List.of(relationPayload), List.of(relatedContext));

        assertEquals("John", unifiedPayload.get("name"));
        assertEquals("john@example.com", unifiedPayload.get("email"));
        assertEquals("555-1234", unifiedPayload.get("phone"));
    }

    private Source createSource(Long id) {
        Source source = new Source();
        source.setId(id);
        source.setName("source_" + id);
        return source;
    }
}
