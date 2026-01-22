package org.example.models.dto;

import java.util.List;

public record RawDataPage(
        List<RawDataRecordDTO> content,
        long totalElements,
        int totalPages,
        int page,
        int size
) {
}
