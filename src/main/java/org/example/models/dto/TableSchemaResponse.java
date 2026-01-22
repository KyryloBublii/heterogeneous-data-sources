package org.example.models.dto;

import java.util.List;

public record TableSchemaResponse(
        String schema,
        String tableName,
        List<ColumnSchemaResponse> columns
) {
}
