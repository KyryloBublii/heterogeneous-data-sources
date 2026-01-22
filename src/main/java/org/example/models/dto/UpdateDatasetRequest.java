package org.example.models.dto;

import org.example.models.enums.DatasetStatus;

public record UpdateDatasetRequest(
        String name,
        String description,
        DatasetStatus datasetStatus,
        String primaryRecordType
) {
}
