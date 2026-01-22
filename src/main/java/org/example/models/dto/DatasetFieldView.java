package org.example.models.dto;

public record DatasetFieldView(
        Long id,
        String uid,
        String name,
        String dtype,
        boolean nullable,
        boolean unique,
        Integer position,
        String defaultExpr
) {
}
