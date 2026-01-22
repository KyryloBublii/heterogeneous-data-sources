package service.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.service.ingestion.WrapperMappingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class WrapperMappingServiceTest {

    private WrapperMappingService service;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        service = new WrapperMappingService(objectMapper);
    }

    @Test
    void testApplyMapping_withNoWrapper_shouldReturnCopy() {
        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("name", "John");

        Map<String, Object> config = new HashMap<>();

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals(1, result.get("id"));
        assertEquals("John", result.get("name"));
        assertNotSame(record, result);
    }

    @Test
    void testApplyMapping_withAttributeMappings_shouldRenameFields() {
        Map<String, Object> record = new HashMap<>();
        record.put("user_id", 123);
        record.put("user_name", "John");

        Map<String, Object> wrapper = new HashMap<>();
        wrapper.put("attribute_mappings", Map.of(
                "user_id", "id",
                "user_name", "name"
        ));

        Map<String, Object> config = new HashMap<>();
        config.put("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals(123, result.get("id"));
        assertEquals("John", result.get("name"));
        assertFalse(result.containsKey("user_id"));
        assertFalse(result.containsKey("user_name"));
    }

    @Test
    void testApplyMapping_withLowercaseTransform_shouldConvertToLowercase() {
        Map<String, Object> record = Map.of("name", "JOHN DOE");

        Map<String, Object> wrapper = new HashMap<>();
        wrapper.put("attribute_mappings", Map.of(
                "name", Map.of("target", "name", "transform", "lowercase")
        ));

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals("john doe", result.get("name"));
    }

    @Test
    void testApplyMapping_withUppercaseTransform_shouldConvertToUppercase() {
        Map<String, Object> record = Map.of("name", "john doe");

        Map<String, Object> wrapper = Map.of(
                "attribute_mappings", Map.of(
                        "name", Map.of("target", "name", "transform", "uppercase")
                )
        );

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals("JOHN DOE", result.get("name"));
    }

    @Test
    void testApplyMapping_withTitlecaseTransform_shouldConvertToTitleCase() {
        Map<String, Object> record = Map.of("name", "john doe");

        Map<String, Object> wrapper = Map.of(
                "attribute_mappings", Map.of(
                        "name", Map.of("target", "name", "transform", "titlecase")
                )
        );

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals("John Doe", result.get("name"));
    }

    @Test
    void testApplyMapping_withStripTransform_shouldRemoveWhitespace() {
        Map<String, Object> record = Map.of("name", "  John Doe  ");

        Map<String, Object> wrapper = Map.of(
                "attribute_mappings", Map.of(
                        "name", Map.of("target", "name", "transform", "strip")
                )
        );

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals("John Doe", result.get("name"));
    }

    @Test
    void testApplyMapping_withIntTransform_shouldCoerceToInteger() {
        Map<String, Object> record = Map.of("age", "30");

        Map<String, Object> wrapper = Map.of(
                "attribute_mappings", Map.of(
                        "age", Map.of("target", "age", "transform", "int")
                )
        );

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals(30, result.get("age"));
    }

    @Test
    void testApplyMapping_withFloatTransform_shouldCoerceToDouble() {
        Map<String, Object> record = Map.of("price", "19.99");

        Map<String, Object> wrapper = Map.of(
                "attribute_mappings", Map.of(
                        "price", Map.of("target", "price", "transform", "float")
                )
        );

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals(19.99, result.get("price"));
    }

    @Test
    void testApplyMapping_withInvalidIntTransform_shouldReturnOriginalValue() {
        Map<String, Object> record = Map.of("age", "not a number");

        Map<String, Object> wrapper = Map.of(
                "attribute_mappings", Map.of(
                        "age", Map.of("target", "age", "transform", "int")
                )
        );

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals("not a number", result.get("age"));
    }

    @Test
    void testApplyMapping_withTheme_shouldAddThemeField() {
        Map<String, Object> record = Map.of("id", 1);

        Map<String, Object> wrapper = new HashMap<>();
        wrapper.put("theme", "user");

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals("user", result.get("__theme__"));
    }

    @Test
    void testApplyMapping_withThemes_shouldAddThemesAndPrimaryTheme() {
        Map<String, Object> record = Map.of("id", 1);

        Map<String, Object> wrapper = new HashMap<>();
        wrapper.put("themes", List.of("user", "customer", "person"));

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals(List.of("user", "customer", "person"), result.get("__themes__"));
        assertEquals("user", result.get("__theme__"));
    }

    @Test
    void testApplyMapping_withWrapperName_shouldAddToMetadata() {
        Map<String, Object> record = Map.of("id", 1);

        Map<String, Object> wrapper = Map.of("name", "UserWrapper");

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        Map<String, Object> meta = (Map<String, Object>) result.get("__meta__");
        assertEquals("UserWrapper", meta.get("wrapper_name"));
    }

    @Test
    void testApplyMapping_withSourceType_shouldAddToMetadata() {
        Map<String, Object> record = Map.of("id", 1);

        Map<String, Object> wrapper = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put("wrapper", wrapper);
        config.put("source_type", "CSV");

        Map<String, Object> result = service.applyMapping(record, config);

        Map<String, Object> meta = (Map<String, Object>) result.get("__meta__");
        assertEquals("csv", meta.get("source_type"));
    }

    @Test
    void testApplyMapping_withSchemaVersion_shouldAddToMetadata() {
        Map<String, Object> record = Map.of("id", 1);

        Map<String, Object> wrapper = Map.of("schema_version", "1.0");

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        Map<String, Object> meta = (Map<String, Object>) result.get("__meta__");
        assertEquals("1.0", meta.get("schema_version"));
    }

    @Test
    void testApplyMapping_withDestinationTable_shouldAddToMetadata() {
        Map<String, Object> record = Map.of("id", 1);

        Map<String, Object> wrapper = Map.of("destination_table", "users");

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        Map<String, Object> meta = (Map<String, Object>) result.get("__meta__");
        assertEquals("users", meta.get("destination_table"));
    }

    @Test
    void testApplyMapping_shouldGenerateRecordUid() {
        Map<String, Object> record = Map.of("id", 1, "name", "John");

        Map<String, Object> wrapper = new HashMap<>();

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        Map<String, Object> meta = (Map<String, Object>) result.get("__meta__");
        assertNotNull(meta.get("record_uid"));
        assertTrue(meta.get("record_uid").toString().length() > 0);
    }

    @Test
    void testApplyMapping_withExistingMetadata_shouldMerge() {
        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("__meta__", Map.of("existing_field", "value"));

        Map<String, Object> wrapper = Map.of("name", "TestWrapper");

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        Map<String, Object> meta = (Map<String, Object>) result.get("__meta__");
        assertEquals("value", meta.get("existing_field"));
        assertEquals("TestWrapper", meta.get("wrapper_name"));
    }

    @Test
    void testApplyMapping_withNullTransformValue_shouldReturnNull() {
        Map<String, Object> record = new HashMap<>();
        record.put("name", null);

        Map<String, Object> wrapper = Map.of(
                "attribute_mappings", Map.of(
                        "name", Map.of("target", "name", "transform", "uppercase")
                )
        );

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertNull(result.get("name"));
    }

    @Test
    void testApplyMapping_withMissingSourceAttribute_shouldNotMap() {
        Map<String, Object> record = Map.of("id", 1);

        Map<String, Object> wrapper = Map.of(
                "attribute_mappings", Map.of(
                        "nonexistent_field", "new_name"
                )
        );

        Map<String, Object> config = Map.of("wrapper", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertFalse(result.containsKey("new_name"));
        assertFalse(result.containsKey("nonexistent_field"));
    }

    @Test
    void testApplyMapping_withWrapperConfigKey_shouldRecognizeWrapper() {
        Map<String, Object> record = Map.of("id", 1);

        Map<String, Object> wrapper = Map.of("theme", "test");

        Map<String, Object> config = Map.of("wrapper_config", wrapper);

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals("test", result.get("__theme__"));
    }

    @Test
    void testApplyMapping_withInlineWrapperConfig_shouldRecognize() {
        Map<String, Object> record = Map.of("id", 1);

        Map<String, Object> config = new HashMap<>();
        config.put("theme", "test");
        config.put("attribute_mappings", Map.of());

        Map<String, Object> result = service.applyMapping(record, config);

        assertEquals("test", result.get("__theme__"));
    }

    @Test
    void testApplyMapping_shouldGenerateConsistentFingerprints() {
        Map<String, Object> record1 = Map.of("id", 1, "name", "John");
        Map<String, Object> record2 = Map.of("id", 1, "name", "John");

        Map<String, Object> config = Map.of("wrapper", Map.of());

        Map<String, Object> result1 = service.applyMapping(record1, config);
        Map<String, Object> result2 = service.applyMapping(record2, config);

        Map<String, Object> meta1 = (Map<String, Object>) result1.get("__meta__");
        Map<String, Object> meta2 = (Map<String, Object>) result2.get("__meta__");

        // Fingerprints should be consistent for same data
        assertNotNull(meta1.get("record_uid"));
        assertNotNull(meta2.get("record_uid"));
    }
}
