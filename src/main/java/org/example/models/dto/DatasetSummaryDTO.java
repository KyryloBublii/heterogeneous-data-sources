package org.example.models.dto;

import org.example.models.enums.DatasetStatus;

import java.time.Instant;

public record DatasetSummaryDTO(
        Long id,
        String name,
        DatasetStatus status,
        String owner,
        String description,
        long recordCount,
        Instant lastUpdated
) {
}
