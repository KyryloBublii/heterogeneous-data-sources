package org.example.models.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;

import java.util.List;

public record ConnectionRequest(
        @NotBlank String sourceId,
        String destinationId,
        String relation,
        Long datasetId,
        @Valid List<TableSelectionDTO> tableSelections
) {
}
