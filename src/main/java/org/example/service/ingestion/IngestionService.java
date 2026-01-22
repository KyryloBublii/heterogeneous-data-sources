package org.example.service.ingestion;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.models.entity.IngestionRun;
import org.example.models.entity.Dataset;
import org.example.models.entity.Relationship;
import org.example.models.entity.Source;
import org.example.models.enums.RunStatus;
import org.example.models.enums.SourceRole;
import org.example.repository.IngestionRunRepository;
import org.example.repository.DatasetRepository;
import org.example.repository.SourceRepository;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class IngestionService {

    private final List<RecordExtractor> extractors;
    private final WrapperMappingService wrapperMappingService;
    private final RelationshipService relationshipService;
    private final RawEventService rawEventService;
    private final RelationshipPersistenceService relationshipPersistenceService;
    private final DestinationOutputService destinationOutputService;
    private final IngestionRunService ingestionRunService;
    private final IngestionRunRepository ingestionRunRepository;
    private final DatasetRepository datasetRepository;
    private final SourceRepository sourceRepository;

    @Async
    public void startIngestionAsync(Long ingestionRunId) {
        ingestionRunRepository.findById(ingestionRunId)
                .ifPresentOrElse(run -> executeIngestion(run, Map.of()), () -> log.warn("Ingestion run {} not found", ingestionRunId));
    }

    @Async
    public void startIngestionAsync(Long ingestionRunId, Map<String, Object> overrides) {
        Map<String, Object> safeOverrides = overrides == null ? Map.of() : overrides;
        ingestionRunRepository.findById(ingestionRunId)
                .ifPresentOrElse(run -> executeIngestion(run, safeOverrides), () -> log.warn("Ingestion run {} not found", ingestionRunId));
    }

    private void executeIngestion(IngestionRun run, Map<String, Object> overrides) {
        try {
            IngestionRun persisted = ingestionRunService.markRunning(run);
            Source source = sourceRepository.findById(persisted.getSource().getId())
                    .orElseThrow(() -> new IllegalStateException("Source not found for ingestion run"));
            Source destination = null;
            if (persisted.getDestination() != null && persisted.getDestination().getId() != null) {
                destination = sourceRepository.findById(persisted.getDestination().getId())
                        .orElseThrow(() -> new IllegalStateException("Destination not found for ingestion run"));
            }
            Map<String, Object> sourceConfig = new LinkedHashMap<>(Optional.ofNullable(source.getConfig()).orElse(Map.of()));
            sourceConfig.putAll(overrides);
            String format = stringValue(sourceConfig.getOrDefault("format", source.getType().name()));
            RecordExtractor extractor = resolveExtractor(format);
            List<Map<String, Object>> rawRecords = extractor.extract(source, sourceConfig);
            List<Map<String, Object>> mapped = rawRecords.stream()
                    .map(record -> wrapperMappingService.applyMapping(record, sourceConfig))
                    .toList();
            List<Relationship> relationships = relationshipService.derive(source, sourceConfig, mapped);

            int stored = rawEventService.write(source, persisted, mapped);
            relationshipPersistenceService.persist(source, persisted, relationships);
            if (destination != null) {
                destinationOutputService.write(destination, mapped);
            }

            ingestionRunService.markSuccess(persisted, mapped.size(), stored);
            log.info("Ingestion {} succeeded with {} rows", persisted.getIngestionUid(), stored);
        } catch (Exception exception) {
            log.error("Ingestion failed", exception);
            ingestionRunService.markFailure(run, exception.getMessage());
        }
    }

    private RecordExtractor resolveExtractor(String format) {
        return extractors.stream()
                .filter(extractor -> extractor.supports(format))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupported format: " + format));
    }

    public Map<Source, IngestionRun> ingestDataset(Long datasetId) {
        return ingestDataset(datasetId, Map.of());
    }

    public Map<Source, IngestionRun> ingestDataset(Long datasetId, Map<String, Object> overrides) {
        Map<String, Object> safeOverrides = overrides == null ? Map.of() : overrides;
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset not found: " + datasetId));

        List<Source> sources = sourceRepository.findAllByDataset_Id(datasetId)
                .stream()
                .filter(src -> src.getRole() == SourceRole.SOURCE)
                .toList();

        if (sources.isEmpty()) {
            log.warn("No sources found for dataset {}. Skipping ingestion.", datasetId);
            return Map.of();
        }

        Map<Source, IngestionRun> runsBySource = new LinkedHashMap<>();
        Map<Source, List<Map<String, Object>>> mappedBySource = new LinkedHashMap<>();
        for (Source source : sources) {
            IngestionRun run = new IngestionRun();
            run.setIngestionUid(UUID.randomUUID().toString());
            run.setDataset(dataset);
            run.setSource(source);
            run.setRunStatus(RunStatus.QUEUED);
            run.setStartedAt(Instant.now());
            run = ingestionRunRepository.save(run);
            IngestionRun persisted = ingestionRunService.markRunning(run);
            try {
                Map<String, Object> sourceConfig = new LinkedHashMap<>(Optional.ofNullable(source.getConfig()).orElse(Map.of()));
                sourceConfig.putAll(safeOverrides);
                String format = stringValue(sourceConfig.getOrDefault("format", source.getType().name()));
                RecordExtractor extractor = resolveExtractor(format);
                List<Map<String, Object>> rawRecords = extractor.extract(source, sourceConfig);
                List<Map<String, Object>> mapped = rawRecords.stream()
                        .map(record -> wrapperMappingService.applyMapping(record, sourceConfig))
                        .toList();
                mappedBySource.put(source, mapped);

                int stored = rawEventService.write(source, persisted, mapped);
                ingestionRunService.markSuccess(persisted, mapped.size(), stored);
                runsBySource.put(source, persisted);
                log.info("Ingestion {} succeeded with {} rows for source {}", persisted.getIngestionUid(), stored, source.getName());
            } catch (Exception exception) {
                log.error("Ingestion failed for source {}", source.getName(), exception);
                ingestionRunService.markFailure(persisted, exception.getMessage());
            }
        }

        try {
            List<Relationship> relationships = relationshipService.deriveAcrossSources(mappedBySource);
            relationshipPersistenceService.persist(relationships, runsBySource);
            log.info("Derived {} relationships across {} sources for dataset {}", relationships.size(), sources.size(), datasetId);
        } catch (Exception exception) {
            log.error("Failed to derive relationships for dataset {}", datasetId, exception);
        }

        return runsBySource;
    }

    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }
}
