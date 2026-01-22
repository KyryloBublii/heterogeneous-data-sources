package org.example.models.dto;

import java.time.Instant;
import java.util.List;

public record ConnectionResponse(
        String id,
        String sourceId,
        String sourceName,
        String destinationId,
        String destinationName,
        String relation,
        List<TableSelectionDTO> tableSelections,
        String createdBy,
        Instant createdAt,
        Long datasetId
) {
}
