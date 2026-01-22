package utility;

import org.example.service.transform.UnifiedPayloadNormalizer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class UnifiedPayloadNormalizerTest {

    @Test
    void normalizePayload_SimpleMap_NormalizesCorrectly() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("email", "test@example.com");
        payload.put("name", "Test User");
        payload.put("age", 30);
        
        Map<String, Object> result = UnifiedPayloadNormalizer.normalizePayload(payload);
        
        assertNotNull(result);
        assertEquals("test@example.com", result.get("email"));
        assertEquals("Test User", result.get("name"));
        assertEquals(30, result.get("age"));
    }

    @Test
    void normalizePayload_WithTimestamps_NormalizesTimestamps() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("created_at", "2024-01-15T10:30:00Z");
        payload.put("name", "Test");

        
        Map<String, Object> result = UnifiedPayloadNormalizer.normalizePayload(payload);

        
        assertNotNull(result);
        assertTrue(result.containsKey("created_at"));
    }

    @Test
    void normalizePayload_NestedStructure_NormalizesRecursively() {
        Map<String, Object> nested = new HashMap<>();
        nested.put("city", "New York");

        Map<String, Object> payload = new HashMap<>();
        payload.put("name", "Test");
        payload.put("address", nested);

        
        Map<String, Object> result = UnifiedPayloadNormalizer.normalizePayload(payload);

        
        assertNotNull(result);
        assertTrue(result.containsKey("address"));
        assertInstanceOf(Map.class, result.get("address"));
    }


}