package org.example.models.dto;

import java.util.List;

public record MappingEditorDataResponse(
        String datasetName,
        List<MappingEditorTable> tables
) {
    public record MappingEditorTable(
            String name,
            List<String> columns,
            List<MappingEditorRow> rows
    ) {}

    public record MappingEditorRow(
            java.util.Map<String, Object> values
    ) {}
}
