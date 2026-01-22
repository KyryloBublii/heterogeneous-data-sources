package org.example.models.dto;

import java.util.List;

public record MappingEditorSaveRequest(
        List<FieldInput> fields,
        List<MappingInput> mappings
) {
    public record FieldInput(
            String name,
            String dtype,
            Boolean required,
            Integer position
    ) {}

    public record MappingInput(
            String datasetFieldName,
            String table,
            String column
    ) {}
}
