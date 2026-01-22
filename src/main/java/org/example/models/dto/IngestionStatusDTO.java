package org.example.models.dto;

import org.example.models.enums.RunStatus;

import java.time.Instant;

public record IngestionStatusDTO (
        String id,
        String sourceId,
        RunStatus status, // Enum: QUEUED, RUNNING, SUCCESS, FAILED
        int rowsRead,
        int rowsStored,
        String errorMessage,
        Instant startedAt,
        Instant endedAt
        )
{
}