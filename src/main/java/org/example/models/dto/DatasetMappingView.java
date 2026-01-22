package org.example.models.dto;

public record DatasetMappingView(
        Long id,
        String uid,
        Long sourceId,
        String sourceName,
        Long datasetFieldId,
        String datasetFieldUid,
        String datasetFieldName,
        String srcPath,
        String transformType,
        boolean required
) {
}
