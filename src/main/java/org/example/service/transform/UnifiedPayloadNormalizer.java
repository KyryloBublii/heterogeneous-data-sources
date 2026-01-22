package org.example.service.transform;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class UnifiedPayloadNormalizer {

    private UnifiedPayloadNormalizer() {
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> normalizePayload(Map<String, Object> payload) {
        if (payload == null) {
            return Map.of();
        }
        Map<String, Object> normalized = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> mapVal) {
                normalized.put(entry.getKey(), normalizePayload((Map<String, Object>) mapVal));
            } else if (value instanceof List<?> listVal) {
                normalized.put(entry.getKey(), normalizeList(listVal));
            } else {
                normalized.put(entry.getKey(), TimestampNormalizer.normalizeValue(value));
            }
        }
        return normalized;
    }

    private static List<Object> normalizeList(List<?> listVal) {
        List<Object> normalized = new ArrayList<>();
        for (Object item : listVal) {
            if (item instanceof Map<?, ?> mapItem) {
                normalized.add(normalizePayload((Map<String, Object>) mapItem));
            } else if (item instanceof List<?> nestedList) {
                normalized.add(normalizeList(nestedList));
            } else {
                normalized.add(TimestampNormalizer.normalizeValue(item));
            }
        }
        return normalized;
    }
}
