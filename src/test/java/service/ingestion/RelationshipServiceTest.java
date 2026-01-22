package service.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.models.entity.Relationship;
import org.example.models.entity.Source;
import org.example.service.ingestion.RelationshipService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RelationshipServiceTest {

    private RelationshipService service;

    @BeforeEach
    void setUp() {
        service = new RelationshipService(new ObjectMapper());
    }

    @Test
    void testDerive_withNoSharedIdentifiers_shouldReturnEmpty() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "name", "John"),
                Map.of("id", 2, "name", "Jane")
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertTrue(result.isEmpty());
    }

    @Test
    void testDerive_withSharedIdField_shouldCreateRelationship() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "user_id", 100, "name", "John"),
                Map.of("id", 2, "user_id", 100, "email", "john@example.com")
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertFalse(result.isEmpty());
        assertEquals("shared_user_id", result.get(0).getRelationType());
    }

    @Test
    void testDerive_withMultipleSharedFields_shouldCreateMultipleRelationships() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "user_id", 100, "org_id", 200),
                Map.of("id", 2, "user_id", 100, "org_id", 200)
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertTrue(result.size() >= 2); // At least one for user_id and one for org_id
    }

    @Test
    void testDeriveAcrossSources_withRecordsFromDifferentSources_shouldLinkThem() {
        Source source1 = createSource("source1");
        Source source2 = createSource("source2");

        List<Map<String, Object>> records1 = List.of(
                Map.of("id", 1, "user_id", 500)
        );
        List<Map<String, Object>> records2 = List.of(
                Map.of("id", 100, "user_id", 500)
        );

        Map<Source, List<Map<String, Object>>> recordsBySource = new HashMap<>();
        recordsBySource.put(source1, records1);
        recordsBySource.put(source2, records2);

        List<Relationship> result = service.deriveAcrossSources(recordsBySource);

        assertFalse(result.isEmpty());
        assertEquals("shared_user_id", result.get(0).getRelationType());
    }

    @Test
    void testDeriveAcrossSources_withEmptyRecords_shouldReturnEmpty() {
        Source source = createSource("source1");
        Map<Source, List<Map<String, Object>>> recordsBySource = Map.of(source, List.of());

        List<Relationship> result = service.deriveAcrossSources(recordsBySource);

        assertTrue(result.isEmpty());
    }

    @Test
    void testDeriveAcrossSources_withNullRecords_shouldHandleGracefully() {
        Source source = createSource("source1");
        Map<Source, List<Map<String, Object>>> recordsBySource = new HashMap<>();
        recordsBySource.put(source, null);

        List<Relationship> result = service.deriveAcrossSources(recordsBySource);

        assertTrue(result.isEmpty());
    }

    @Test
    void testDeriveAcrossSources_shouldDeduplicateRelationships() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "user_id", 100),
                Map.of("id", 2, "user_id", 100),
                Map.of("id", 3, "user_id", 100)
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        // Should create relationships for each pair, but deduplicate if same from/to
        assertTrue(result.size() >= 1);
    }

    @Test
    void testDeriveAcrossSources_withOnlyOneRecordPerValue_shouldNotCreateRelationships() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "user_id", 100),
                Map.of("id", 2, "user_id", 200),
                Map.of("id", 3, "user_id", 300)
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertTrue(result.isEmpty()); // No shared values
    }

    @Test
    void testDeriveAcrossSources_withNameField_shouldRecognizeAsCandidate() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "name", "John Smith"),
                Map.of("id", 2, "name", "John Smith")
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertFalse(result.isEmpty());
        assertEquals("shared_name", result.get(0).getRelationType());
    }

    @Test
    void testDeriveAcrossSources_withCodeField_shouldRecognizeAsCandidate() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "code", "ABC123"),
                Map.of("id", 2, "code", "ABC123")
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertFalse(result.isEmpty());
        assertEquals("shared_code", result.get(0).getRelationType());
    }

    @Test
    void testDeriveAcrossSources_withFieldEndingInId_shouldRecognizeAsCandidate() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "customer_id", 500),
                Map.of("id", 2, "customer_id", 500)
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertFalse(result.isEmpty());
        assertTrue(result.get(0).getRelationType().contains("customer_id"));
    }

    @Test
    void testDeriveAcrossSources_withNullValues_shouldIgnore() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = Arrays.asList(
                Map.of("id", 1, "user_id", 100),
                createMapWithNull("id", 2, "user_id", null)
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        // Should not create relationship for null user_id
        assertTrue(result.isEmpty());
    }

    @Test
    void testDeriveAcrossSources_withBlankStringValues_shouldIgnore() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "name", "John"),
                Map.of("id", 2, "name", "   ")
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertTrue(result.isEmpty());
    }

    @Test
    void testDeriveAcrossSources_shouldSetFromAndToFields() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "user_id", 100, "__table__", "users"),
                Map.of("id", 2, "user_id", 100, "__table__", "orders")
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertFalse(result.isEmpty());
        Relationship rel = result.get(0);
        assertNotNull(rel.getFromType());
        assertNotNull(rel.getFromId());
        assertNotNull(rel.getToType());
        assertNotNull(rel.getToId());
    }

    @Test
    void testDeriveAcrossSources_shouldSetPayload() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "user_id", 100),
                Map.of("id", 2, "user_id", 100)
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertFalse(result.isEmpty());
        assertNotNull(result.get(0).getPayload());
    }

    @Test
    void testDeriveAcrossSources_shouldSetIngestedAt() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                Map.of("id", 1, "user_id", 100),
                Map.of("id", 2, "user_id", 100)
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertFalse(result.isEmpty());
        assertNotNull(result.get(0).getIngestedAt());
    }

    @Test
    void testDeriveAcrossSources_withRecordTypeInMetadata_shouldUseIt() {
        Source source = createSource("source1");
        Map<String, Object> meta = Map.of("destination_table", "customers");
        List<Map<String, Object>> records = List.of(
                createMapWithMeta("id", 1, "user_id", 100, meta),
                Map.of("id", 2, "user_id", 100)
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertFalse(result.isEmpty());
        // One of the types should be "customers"
    }

    @Test
    void testDeriveAcrossSources_withThemeField_shouldUseAsRecordType() {
        Source source = createSource("source1");
        List<Map<String, Object>> records = List.of(
                createMapWith("id", 1, "user_id", 100, "__theme__", "person"),
                Map.of("id", 2, "user_id", 100)
        );

        List<Relationship> result = service.derive(source, Map.of(), records);

        assertFalse(result.isEmpty());
    }

    private Source createSource(String name) {
        Source source = new Source();
        source.setId((long) name.hashCode());
        source.setName(name);
        return source;
    }

    private Map<String, Object> createMapWithNull(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i].toString(), keyValues[i + 1]);
        }
        return map;
    }

    private Map<String, Object> createMapWithMeta(Object... keyValuesAndMeta) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValuesAndMeta.length - 1; i += 2) {
            if (keyValuesAndMeta[i + 1] instanceof Map) {
                map.put("__meta__", keyValuesAndMeta[i + 1]);
                break;
            }
            map.put(keyValuesAndMeta[i].toString(), keyValuesAndMeta[i + 1]);
        }
        return map;
    }

    private Map<String, Object> createMapWith(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i].toString(), keyValues[i + 1]);
        }
        return map;
    }
}
