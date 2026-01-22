package org.example.models.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.example.models.enums.SourceRole;
import org.example.models.enums.SourceStatus;
import org.example.models.enums.SourceType;

import java.time.Instant;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public record SourceDTO (
    String id,
    String name,
    SourceType type,
    SourceRole role,
    Map<String, Object> config,
    SourceStatus status,
    Instant createdAt,
    Instant updatedAt,
    Long datasetId
){

}
