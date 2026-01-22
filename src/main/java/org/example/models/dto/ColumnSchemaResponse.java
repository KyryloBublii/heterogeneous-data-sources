package org.example.models.dto;

public record ColumnSchemaResponse(
        String name,
        String dataType,
        boolean nullable
) {
}
