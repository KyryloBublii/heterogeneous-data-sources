package org.example.models.dto;

import java.util.Map;

public record ExportResultDTO(
        long rowsSent,
        Map<String, Object> destinations
) {
}
