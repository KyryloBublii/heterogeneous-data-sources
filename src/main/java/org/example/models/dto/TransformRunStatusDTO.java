package org.example.models.dto;

import java.time.Instant;
import org.example.models.enums.RunStatus;

public record TransformRunStatusDTO(
        String runUid,
        RunStatus status,
        Integer rowsIn,
        Integer rowsOut,
        String errorMessage,
        Instant startedAt,
        Instant endedAt
) {
}
