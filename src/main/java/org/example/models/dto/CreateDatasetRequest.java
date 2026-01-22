package org.example.models.dto;

import jakarta.validation.constraints.NotBlank;

public record CreateDatasetRequest(
        @NotBlank(message = "Dataset name is required")
        String name,
        String description,
        String primaryRecordType
) {
}
