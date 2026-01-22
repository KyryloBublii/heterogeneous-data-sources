package org.example.models.dto;

import jakarta.validation.Valid;
import java.util.List;

public record TableSelectionUpdateRequest(
        @Valid List<TableSelectionDTO> tableSelections
) {
}
