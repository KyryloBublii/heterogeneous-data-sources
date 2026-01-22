package org.example.models.dto;

import java.time.Instant;

public record PipelineStatusResponse(
        Long datasetId,
        String datasetName,
        String description,
        String owner,
        boolean hasSources,
        boolean hasIngestion,
        boolean hasMappings,
        boolean hasTransformation,
        boolean readyForExport,
        Instant lastUpdated
) {
}
