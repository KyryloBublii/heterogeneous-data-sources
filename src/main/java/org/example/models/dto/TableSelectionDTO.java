package org.example.models.dto;

import jakarta.validation.constraints.NotBlank;

import java.util.List;

public record TableSelectionDTO(
        @NotBlank String tableName,
        String schema,
        List<String> columns
) {
}
