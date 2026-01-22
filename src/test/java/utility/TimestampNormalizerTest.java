package utility;

import org.example.service.transform.TimestampNormalizer;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TimestampNormalizerTest {

    @Test
    void normalizeValue_Map_NormalizesRecursively() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Test");
        map.put("timestamp", "2024-01-15T10:30:00Z");
        map.put("count", 10);

        Object result = TimestampNormalizer.normalizeValue(map);

        assertInstanceOf(Map.class, result);
        Map<?, ?> resultMap = (Map<?, ?>) result;
        assertEquals("Test", resultMap.get("name"));
        assertEquals(10, resultMap.get("count"));
    }

    @Test
    void normalizeValue_List_NormalizesRecursively() {
        List<Object> list = new ArrayList<>();
        list.add("text");
        list.add(42);
        list.add("2024-01-15T10:30:00Z");

        Object result = TimestampNormalizer.normalizeValue(list);

        assertInstanceOf(List.class, result);
        List<?> resultList = (List<?>) result;
        assertEquals(3, resultList.size());
        assertEquals("text", resultList.get(0));
        assertEquals(42, resultList.get(1));
    }

    @Test
    void normalizeValue_NestedStructure_NormalizesAll() {
        Map<String, Object> inner = new HashMap<>();
        inner.put("value", 100);
        
        Map<String, Object> outer = new HashMap<>();
        outer.put("nested", inner);
        outer.put("list", Arrays.asList(1, 2, 3));

        Object result = TimestampNormalizer.normalizeValue(outer);

        assertInstanceOf(Map.class, result);
        Map<?, ?> resultMap = (Map<?, ?>) result;
        assertTrue(resultMap.containsKey("nested"));
        assertTrue(resultMap.containsKey("list"));
    }
}