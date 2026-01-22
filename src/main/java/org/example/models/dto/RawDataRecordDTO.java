package org.example.models.dto;

import java.time.Instant;
import java.util.Map;

public record RawDataRecordDTO(
        String id,
        String sourceName,
        Map<String, Object> payload,
        Instant createdAt
) {
}
