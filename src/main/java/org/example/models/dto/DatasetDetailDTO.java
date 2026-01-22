package org.example.models.dto;

import org.example.models.enums.DatasetStatus;

import java.time.Instant;

public record DatasetDetailDTO(
        Long id,
        String name,
        String description,
        String primaryRecordType,
        DatasetStatus status,
        Instant createdAt,
        Instant updatedAt
) {
}
