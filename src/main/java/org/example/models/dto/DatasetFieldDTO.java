package org.example.models.dto;

public record DatasetFieldDTO(
        String name,
        String dtype,
        boolean isNullable,
        boolean isUnique,
        String defaultExpr,
        int position
) {
}
