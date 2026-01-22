package org.example.models.dto;

public record DatasetMappingDTO (
        String datasetFieldId,
        String srcPath,
        String srcJsonPath,
        String transformType,
        String transformSql,
        boolean required
){

}
