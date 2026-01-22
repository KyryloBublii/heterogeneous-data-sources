package org.example.models.dto;

import java.util.List;
import java.util.Map;

public record UnifiedDataDTO (
    String id,
    List<Map<String, Object>> data
)
{
}
