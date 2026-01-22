package org.example.service;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.adapters.DataSourceAdapter;
import org.example.models.dto.IntegrationConfigDTO;
import org.example.models.dto.UnifiedRecord;
import org.example.models.entity.Dataset;
import org.example.models.entity.Source;
import org.example.models.entity.UnifiedRow;
import org.example.repository.DatasetRepository;
import org.example.repository.SourceRepository;
import org.example.repository.UnifiedRowRepository;
import org.example.repository.UserRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class DataIntegrationService {

    private final List<DataSourceAdapter> adapters;
    private final SourceRepository sourceRepository;
    private final DatasetRepository datasetRepository;
    private final UnifiedRowRepository unifiedRowRepository;
    private final UserRepository userRepository;

    @Transactional
    public Map<String, Object> runIntegration(IntegrationConfigDTO config, String userEmail) {
        log.info("Starting data integration for dataset ID: {}", config.getDatasetId());

        long startTime = System.currentTimeMillis();
        int totalRecordsProcessed = 0;
        List<String> errors = new ArrayList<>();

        try {
            if (!StringUtils.hasText(userEmail)) {
                throw new IllegalArgumentException("User email is required");
            }

            Dataset dataset = datasetRepository.findByIdAndApplicationUser_Email(config.getDatasetId(), userEmail)
                    .orElseThrow(() -> new IllegalArgumentException("Dataset not found: " + config.getDatasetId()));

            for (IntegrationConfigDTO.SourceMappingDTO sourceMapping : config.getSourceMappings()) {
                try {
                    Source source = sourceRepository.findByIdAndApplicationUser_Email(sourceMapping.getSourceId(), userEmail)
                            .orElseThrow(() -> new IllegalArgumentException("Source not found: " + sourceMapping.getSourceId()));

                    List<UnifiedRecord> extractedRecords = extract(source);
                    log.info("Extracted {} records from source: {}", extractedRecords.size(), source.getName());

                    List<UnifiedRecord> transformedRecords = transform(extractedRecords, sourceMapping.getFieldMappings());
                    log.info("Transformed {} records", transformedRecords.size());

                    int loadedCount = load(transformedRecords, dataset, source);
                    log.info("Loaded {} records into unified dataset", loadedCount);

                    totalRecordsProcessed += loadedCount;

                } catch (Exception e) {
                    log.error("Error processing source {}: {}", sourceMapping.getSourceId(), e.getMessage(), e);
                    errors.add("Source " + sourceMapping.getSourceId() + ": " + e.getMessage());
                }
            }

            long duration = System.currentTimeMillis() - startTime;

            Map<String, Object> result = new HashMap<>();
            result.put("success", errors.isEmpty());
            result.put("datasetId", config.getDatasetId());
            result.put("recordsProcessed", totalRecordsProcessed);
            result.put("duration", duration);
            result.put("errors", errors);

            log.info("Integration completed. Processed {} records in {} ms", totalRecordsProcessed, duration);

            return result;

        } catch (Exception e) {
            log.error("Fatal error during integration: {}", e.getMessage(), e);

            Map<String, Object> result = new HashMap<>();
            result.put("success", false);
            result.put("recordsProcessed", totalRecordsProcessed);
            result.put("error", e.getMessage());
            result.put("errors", errors);

            return result;
        }
    }

    public List<UnifiedRecord> extract(Source source) throws Exception {
        log.info("Extracting data from source: {} (type: {})", source.getName(), source.getType());

        DataSourceAdapter adapter = adapters.stream()
                .filter(a -> a.supportsSource(source))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No adapter found for source type: " + source.getType()));

        return adapter.extract(source);
    }

    public List<UnifiedRecord> transform(List<UnifiedRecord> records,
                                         Map<String, IntegrationConfigDTO.FieldMappingDTO> fieldMappings) {
        log.info("Transforming {} records with {} field mappings", records.size(), fieldMappings.size());

        List<UnifiedRecord> transformedRecords = new ArrayList<>();

        for (UnifiedRecord record : records) {
            UnifiedRecord transformedRecord = new UnifiedRecord();
            transformedRecord.setSourceIdentifier(record.getSourceIdentifier());
            transformedRecord.setRecordKey(record.getRecordKey());

            for (Map.Entry<String, IntegrationConfigDTO.FieldMappingDTO> entry : fieldMappings.entrySet()) {
                String targetFieldName = entry.getKey();
                IntegrationConfigDTO.FieldMappingDTO mapping = entry.getValue();

                Object value = record.getField(mapping.getSourceField());

                if (value == null) {
                    if (mapping.isRequired()) {
                        if (mapping.getDefaultValue() != null) {
                            value = mapping.getDefaultValue();
                        } else {
                            log.warn("Required field {} is null in record {}", targetFieldName, record.getRecordKey());
                            continue;
                        }
                    } else {
                        value = mapping.getDefaultValue();
                    }
                }

                Object transformedValue = castToType(value, mapping.getTargetType());
                transformedRecord.addField(targetFieldName, transformedValue);
            }

            transformedRecords.add(transformedRecord);
        }

        return transformedRecords;
    }

    @Transactional
    public int load(List<UnifiedRecord> records, Dataset dataset, Source source) {
        log.info("Loading {} records into dataset: {}", records.size(), dataset.getName());

        int loadedCount = 0;

        for (UnifiedRecord record : records) {
            try {
                UnifiedRow row = new UnifiedRow();
                row.setUnifiedRowUid(UUID.randomUUID().toString());
                row.setDataset(dataset);
                row.setSource(source);
                row.setRecordKey(record.getRecordKey());
                row.setData(record.getFields());
                row.setIngestedAt(Instant.now());

                unifiedRowRepository.save(row);
                loadedCount++;

            } catch (Exception e) {
                log.error("Error saving record {}: {}", record.getRecordKey(), e.getMessage());
            }
        }

        log.info("Successfully loaded {} out of {} records", loadedCount, records.size());
        return loadedCount;
    }

    private Object castToType(Object value, String targetType) {
        if (value == null || targetType == null) {
            return value;
        }

        try {
            switch (targetType.toLowerCase()) {
                case "string":
                case "text":
                    return value.toString();

                case "integer":
                case "int":
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    }
                    return Integer.parseInt(value.toString());

                case "long":
                    if (value instanceof Number) {
                        return ((Number) value).longValue();
                    }
                    return Long.parseLong(value.toString());

                case "double":
                case "decimal":
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                    return Double.parseDouble(value.toString());

                case "boolean":
                    if (value instanceof Boolean) {
                        return value;
                    }
                    return Boolean.parseBoolean(value.toString());

                case "date":
                case "timestamp":
                    return value;

                default:
                    return value;
            }
        } catch (Exception e) {
            log.warn("Failed to cast value {} to type {}: {}", value, targetType, e.getMessage());
            return value;
        }
    }
}
