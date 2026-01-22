package org.example.models.dto;

import org.example.models.enums.DatasetStatus;

import java.time.Instant;

public record DatasetDTO(
        String id,
        String name,
        String description,
        DatasetStatus datasetStatus,
        Instant createdAt
){

}

