package org.example.models.dto;

public record AddMappingRequest(
        Long sourceId,
        String datasetFieldUid,
        String srcPath,
        String transformType,
        String srcJsonPath,
        String transformSql,
        boolean required
) {
}
