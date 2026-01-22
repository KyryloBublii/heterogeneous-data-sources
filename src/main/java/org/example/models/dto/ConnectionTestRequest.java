package org.example.models.dto;

import org.example.models.enums.SourceType;

import java.util.Map;

public record ConnectionTestRequest(SourceType type, Map<String, Object> config) {
}
