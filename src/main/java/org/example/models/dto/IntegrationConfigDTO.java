package org.example.models.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class IntegrationConfigDTO {
    private Long datasetId;
    private List<SourceMappingDTO> sourceMappings;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SourceMappingDTO {
        private Long sourceId;
        private Map<String, FieldMappingDTO> fieldMappings;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FieldMappingDTO {
        private String sourceField;
        private String targetField;
        private String targetType;
        private Object defaultValue;
        private boolean required;
    }
}
