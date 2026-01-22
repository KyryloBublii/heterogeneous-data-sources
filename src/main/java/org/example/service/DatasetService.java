package org.example.service;

import lombok.RequiredArgsConstructor;
import org.example.models.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.example.models.dto.DatasetDetailDTO;
import org.example.models.dto.DatasetFieldDTO;
import org.example.models.dto.DatasetMappingDTO;
import org.example.models.dto.DatasetMappingView;
import org.example.models.dto.TableSelectionDTO;
import org.example.models.dto.TransformRunStatusDTO;
import org.example.models.dto.PipelineStatusResponse;
import org.example.models.dto.ExportResultDTO;
import org.example.models.dto.DatasetFieldView;
import org.example.models.dto.AddMappingRequest;
import org.example.models.dto.UpdateDatasetRequest;
import org.example.models.dto.MappingEditorDataResponse;
import org.example.models.dto.MappingEditorSaveRequest;
import org.example.models.enums.DataType;
import org.example.models.enums.DatasetStatus;
import org.example.models.enums.TransformType;
import org.example.repository.DatasetFieldRepository;
import org.example.repository.DatasetMappingRepository;
import org.example.repository.DatasetRepository;
import org.example.repository.IntegrationConnectionRepository;
import org.example.repository.RawEventRepository;
import org.example.repository.SourceRepository;
import org.example.repository.TransformRunRepository;
import org.example.repository.UnifiedRowRepository;
import org.example.repository.UserRepository;
import org.example.repository.RelationshipRepository;
import org.example.utils.AppUtils;
import org.example.service.ingestion.IngestionService;
import org.example.service.ingestion.DestinationOutputService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class DatasetService {

    private static final Logger log = LoggerFactory.getLogger(DatasetService.class);

    private final DatasetRepository datasetRepository;
    private final DatasetFieldRepository datasetFieldRepository;
    private final DatasetMappingRepository datasetMappingRepository;
    private final UserRepository userRepository;
    private final SourceRepository sourceRepository;
    private final IntegrationConnectionRepository integrationConnectionRepository;
    private final RawEventRepository rawEventRepository;
    private final UnifiedRowRepository unifiedRowRepository;
    private final TransformRunRepository transformRunRepository;
    private final SourceService sourceService;
    private final DestinationOutputService destinationOutputService;
    private final RelationshipRepository relationshipRepository;
    private final IngestionService ingestionService;
    private final TransformService transformService;


    public Dataset createDatasetForUser(String name, String description, String primaryRecordType, String userEmail) {
        ApplicationUser owner = requireUser(userEmail);
        return persistDataset(name, description, null, primaryRecordType, owner);
    }

    public DatasetField addField(Long datasetId, DatasetFieldDTO fieldDTO, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        DataType dataType = parseDataType(fieldDTO.dtype());

        DatasetField field = new DatasetField();
        field.setDatasetFieldUid(AppUtils.generateUUID());
        field.setDataset(dataset);
        field.setName(fieldDTO.name());
        field.setDtype(dataType);
        field.setIsNullable(fieldDTO.isNullable());
        field.setIsUnique(fieldDTO.isUnique());
        field.setDefaultExpr(fieldDTO.defaultExpr());
        field.setPosition(fieldDTO.position());

        return datasetFieldRepository.save(field);
    }

    @Transactional
    public void deleteField(Long datasetId, Long fieldId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        DatasetField field = datasetFieldRepository.findById(fieldId)
                .orElseThrow(() -> new IllegalArgumentException("Field not found"));
        if (!field.getDataset().getId().equals(dataset.getId())) {
            throw new IllegalArgumentException("Field does not belong to dataset");
        }
        datasetFieldRepository.delete(field);
    }

    public DatasetMapping addMapping(Long datasetId, Long sourceId, DatasetMappingDTO dto) {
        Dataset dataset = getDataset(datasetId);
        Source source = sourceRepository.findById(sourceId)
                .orElseThrow(() -> new IllegalArgumentException("Source not found"));
        DatasetField datasetField = datasetFieldRepository.findByDatasetFieldUid(dto.datasetFieldId())
                .orElseThrow(() -> new IllegalArgumentException("Dataset field not found"));

        if (!datasetField.getDataset().getId().equals(dataset.getId())) {
            throw new IllegalArgumentException("Field does not belong to the target dataset");
        }

        String resolvedPath = StringUtils.hasText(dto.srcPath()) ? dto.srcPath() : dto.srcJsonPath();
        TransformType transformType = parseTransformType(dto.transformType());

        DatasetMapping datasetMapping = DatasetMapping.builder()
                .datasetMappingUid(AppUtils.generateUUID())
                .dataset(dataset)
                .source(source)
                .datasetField(datasetField)
                .srcJsonPath(StringUtils.hasText(dto.srcJsonPath()) ? dto.srcJsonPath() : resolvedPath)
                .srcPath(resolvedPath != null ? resolvedPath : "")
                .transformType(transformType)
                .transformSql(dto.transformSql())
                .required(dto.required())
                .priority(0)
                .build();

        return datasetMappingRepository.save(datasetMapping);
    }

    @Transactional
    public void deleteMapping(Long datasetId, Long mappingId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        DatasetMapping mapping = datasetMappingRepository.findById(mappingId)
                .orElseThrow(() -> new IllegalArgumentException("Mapping not found"));
        if (!mapping.getDataset().getId().equals(dataset.getId())) {
            throw new IllegalArgumentException("Mapping does not belong to dataset");
        }
        datasetMappingRepository.delete(mapping);
    }

    public DatasetMapping addMapping(Long datasetId, AddMappingRequest request, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        if (request == null || request.sourceId() == null || request.datasetFieldUid() == null) {
            throw new IllegalArgumentException("Source and field are required for mapping");
        }
        DatasetMappingDTO dto = new DatasetMappingDTO(
                request.datasetFieldUid(),
                request.srcPath(),
                request.srcJsonPath(),
                request.transformType(),
                request.transformSql(),
                request.required()
        );
        return addMapping(dataset.getId(), request.sourceId(), dto);
    }

    public List<DatasetMappingView> listMappings(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        return datasetMappingRepository.findAllByDataset(dataset).stream()
                .map(mapping -> new DatasetMappingView(
                        mapping.getId(),
                        mapping.getDatasetMappingUid(),
                        mapping.getSource() != null ? mapping.getSource().getId() : null,
                        mapping.getSource() != null ? mapping.getSource().getName() : null,
                        mapping.getDatasetField() != null ? mapping.getDatasetField().getId() : null,
                        mapping.getDatasetField() != null ? mapping.getDatasetField().getDatasetFieldUid() : null,
                        mapping.getDatasetField() != null ? mapping.getDatasetField().getName() : null,
                        StringUtils.hasText(mapping.getSrcPath()) ? mapping.getSrcPath() : mapping.getSrcJsonPath(),
                        mapping.getTransformType() != null ? mapping.getTransformType().name() : TransformType.NONE.name(),
                        Boolean.TRUE.equals(mapping.getRequired())
                ))
                .toList();
    }

    @Transactional
    public DatasetMapping updateMapping(Long datasetId, Long mappingId, DatasetMappingDTO dto, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        DatasetMapping mapping = datasetMappingRepository.findById(mappingId)
                .orElseThrow(() -> new IllegalArgumentException("Mapping not found"));
        if (!mapping.getDataset().getId().equals(dataset.getId())) {
            throw new IllegalArgumentException("Mapping does not belong to dataset");
        }

        String resolvedPath = StringUtils.hasText(dto.srcPath()) ? dto.srcPath() : dto.srcJsonPath();
        TransformType transformType = parseTransformType(dto.transformType());

        mapping.setSrcJsonPath(StringUtils.hasText(dto.srcJsonPath()) ? dto.srcJsonPath() : resolvedPath);
        mapping.setSrcPath(resolvedPath != null ? resolvedPath : "");
        mapping.setTransformType(transformType);
        mapping.setTransformSql(dto.transformSql());
        mapping.setRequired(dto.required());

        return datasetMappingRepository.save(mapping);
    }

    private TransformType parseTransformType(String raw) {
        if (!StringUtils.hasText(raw)) {
            return TransformType.NONE;
        }
        try {
            return TransformType.valueOf(raw.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            return TransformType.NONE;
        }
    }


    public List<Dataset> listAllForUser(String userEmail) {
        return datasetRepository.findAllByApplicationUser_Email(requireUser(userEmail).getEmail());
    }

    public List<DatasetFieldView> listFields(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        return datasetFieldRepository.findAllByDataset_Id(dataset.getId()).stream()
                .sorted(Comparator.comparing(DatasetField::getPosition, Comparator.nullsLast(Integer::compareTo)))
                .map(field -> new DatasetFieldView(
                        field.getId(),
                        field.getDatasetFieldUid(),
                        field.getName(),
                        field.getDtype() != null ? field.getDtype().name() : null,
                        Boolean.TRUE.equals(field.getIsNullable()),
                        Boolean.TRUE.equals(field.getIsUnique()),
                        field.getPosition(),
                        field.getDefaultExpr()
                ))
                .toList();
    }

    public MappingEditorDataResponse loadMappingEditorData(Long datasetId, String userEmail, int limit) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        int pageSize = Math.max(limit, 1);
        List<RawEvent> events = rawEventRepository.findByDataset_Id(dataset.getId(), PageRequest.of(0, pageSize)).getContent();
        Map<String, List<Map<String, Object>>> grouped = new LinkedHashMap<>();

        Map<String, String> displayIndex = buildDisplayIndex(events);
        Map<String, String> relationshipTargets = buildRelationshipTargets(dataset);

        for (RawEvent event : events) {
            Map<String, Object> payload = event.getPayload();
            if (payload == null) {
                continue;
            }
            String tableName = Optional.ofNullable(payload.get("__table__")).map(Object::toString).orElse("Unknown");
            Map<String, Object> sanitized = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : payload.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith("__")) {
                    continue;
                }
                Object value = substituteRelationship(tableName, entry.getValue(), displayIndex, relationshipTargets);
                sanitized.put(key, value);
            }
            grouped.computeIfAbsent(tableName, t -> new ArrayList<>()).add(sanitized);
        }

        List<MappingEditorDataResponse.MappingEditorTable> tables = new ArrayList<>();
        grouped.forEach((name, rows) -> {
            Set<String> cols = new LinkedHashSet<>();
            rows.forEach(r -> cols.addAll(r.keySet()));
            List<String> ordered = new ArrayList<>(cols);
            tables.add(new MappingEditorDataResponse.MappingEditorTable(
                    name,
                    ordered,
                    rows.stream().map(r -> new MappingEditorDataResponse.MappingEditorRow(r)).toList()
            ));
        });

        return new MappingEditorDataResponse(dataset.getName(), tables);
    }

    @Transactional
    public void saveMappingEditor(Long datasetId, MappingEditorSaveRequest request, String userEmail) {
        if (request == null) {
            throw new IllegalArgumentException("Request body is required");
        }
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        List<RawEvent> events = rawEventRepository.findByDataset_Id(datasetId);
        Map<String, Source> tableSources = new LinkedHashMap<>();
        for (RawEvent event : events) {
            String tableName = Optional.ofNullable(event.getPayload())
                    .map(p -> p.get("__table__"))
                    .map(Object::toString)
                    .orElse(null);
            if (tableName != null && !tableSources.containsKey(tableName)) {
                tableSources.put(tableName, event.getSource());
            }
        }

        if (tableSources.isEmpty()) {
            List<Source> datasetSources = sourceRepository.findAllByDataset_Id(datasetId);
            if (!datasetSources.isEmpty()) {
                Source fallback = datasetSources.get(0);
                tableSources.put("", fallback);
                log.warn("Mapping editor fallback: using first dataset source {} because no table-specific sources were found", fallback.getName());
            }
        }

        datasetMappingRepository.deleteAll(datasetMappingRepository.findAllByDataset(dataset));
        datasetFieldRepository.deleteAll(datasetFieldRepository.findAllByDataset_Id(datasetId));

        Map<String, DatasetField> fieldByName = new LinkedHashMap<>();
        if (request.fields() != null) {
            for (MappingEditorSaveRequest.FieldInput input : request.fields()) {
                DatasetField field = new DatasetField();
                field.setDatasetFieldUid(AppUtils.generateUUID());
                field.setDataset(dataset);
                field.setName(input.name());
                String dtype = input.dtype();
                DataType resolvedType = (dtype == null || dtype.isBlank()) ? DataType.TEXT : parseDataType(dtype);
                field.setDtype(resolvedType);
                field.setIsNullable(!Boolean.TRUE.equals(input.required()));
                field.setIsUnique(false);
                field.setDefaultExpr(null);
                field.setPosition(input.position());
                field = datasetFieldRepository.save(field);
                fieldByName.put(field.getName(), field);
            }
        }

        if (request.mappings() != null) {
            Map<String, Integer> priorityCounter = new HashMap<>();
            for (MappingEditorSaveRequest.MappingInput mappingInput : request.mappings()) {
                if (mappingInput == null || mappingInput.datasetFieldName() == null) {
                    continue;
                }
                DatasetField field = fieldByName.get(mappingInput.datasetFieldName());
                if (field == null) {
                    continue;
                }
                Source source = tableSources.get(mappingInput.table());
                if (source == null) {
                    if (!tableSources.isEmpty()) {
                        source = tableSources.values().iterator().next();
                    } else {
                        List<Source> datasetSources = sourceRepository.findAllByDataset_Id(datasetId);
                        if (!datasetSources.isEmpty()) {
                            source = datasetSources.get(0);
                            tableSources.put(mappingInput.table() == null ? "" : mappingInput.table(), source);
                            log.warn("Mapping editor fallback: using first dataset source {} for table '{}'", source.getName(), mappingInput.table());
                        }
                    }
                }
                if (source == null) {
                    log.warn("Skipping mapping for field {} because no source could be resolved", field.getName());
                    continue;
                }
                String priorityKey = field.getId() + ":" + source.getId();
                int priority = priorityCounter.getOrDefault(priorityKey, -1) + 1;
                priorityCounter.put(priorityKey, priority);

                DatasetMapping mapping = DatasetMapping.builder()
                        .datasetMappingUid(AppUtils.generateUUID())
                        .dataset(dataset)
                        .source(source)
                        .datasetField(field)
                        .srcPath(mappingInput.column())
                        .srcJsonPath(mappingInput.column())
                        .transformType(TransformType.NONE)
                        .required(false)
                        .priority(priority)
                        .build();
                datasetMappingRepository.save(mapping);
            }
        }
    }

    private Map<String, String> buildRelationshipTargets(Dataset dataset) {
        Map<String, String> targets = new HashMap<>();
        List<Source> sources = sourceRepository.findAllByDataset_Id(dataset.getId());
        for (Source source : sources) {
            List<Relationship> relationships = relationshipRepository.findBySource(source);
            for (Relationship relationship : relationships) {
                String fromKey = relationship.getFromType() + "::" + relationship.getFromId();
                String toKey = relationship.getToType() + "::" + relationship.getToId();
                targets.put(fromKey, toKey);
            }
        }
        return targets;
    }

    private Map<String, String> buildDisplayIndex(List<RawEvent> events) {
        Map<String, String> display = new HashMap<>();
        for (RawEvent event : events) {
            Map<String, Object> payload = event.getPayload();
            if (payload == null) {
                continue;
            }
            String table = Optional.ofNullable(payload.get("__table__")).map(Object::toString).orElse("table");
            String identity = resolveIdentity(payload);
            if (identity == null) {
                continue;
            }
            String label = resolveDisplayLabel(payload, identity);
            display.put(table + "::" + identity, label);
        }
        return display;
    }

    private Object substituteRelationship(String table,
                                          Object value,
                                          Map<String, String> displayIndex,
                                          Map<String, String> relationshipTargets) {
        if (value == null) {
            return null;
        }
        String stringValue = value.toString();
        String key = table + "::" + stringValue;
        if (relationshipTargets.containsKey(key)) {
            String targetKey = relationshipTargets.get(key);
            return displayIndex.containsKey(targetKey) ? displayIndex.get(targetKey) : value;
        }
        if (displayIndex.containsKey(key)) {
            return displayIndex.get(key);
        }
        for (Map.Entry<String, String> entry : displayIndex.entrySet()) {
            if (entry.getKey().endsWith("::" + stringValue)) {
                return entry.getValue();
            }
        }
        return value;
    }

    private String resolveIdentity(Map<String, Object> payload) {
        for (String key : List.of("id", "uid", "uuid", "record_id", "identifier")) {
            Object value = payload.get(key);
            if (value != null && !value.toString().isBlank()) {
                return value.toString();
            }
        }
        Object meta = payload.get("__meta__");
        if (meta instanceof Map<?, ?> map) {
            for (String key : List.of("record_uid", "uid", "id")) {
                Object value = map.get(key);
                if (value != null && !value.toString().isBlank()) {
                    return value.toString();
                }
            }
        }
        return null;
    }

    private String resolveDisplayLabel(Map<String, Object> payload, String identity) {
        for (String key : List.of("name", "full_name", "title", "email")) {
            Object value = payload.get(key);
            if (value != null && !value.toString().isBlank()) {
                return value.toString();
            }
        }
        return identity;
    }

    public List<DatasetField> listFieldEntities(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        return datasetFieldRepository.findAllByDataset_Id(dataset.getId());
    }

    public DatasetField updateField(Long datasetId, Long fieldId, DatasetFieldDTO request, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        DatasetField field = datasetFieldRepository.findById(fieldId)
                .orElseThrow(() -> new IllegalArgumentException("Field not found"));
        if (!field.getDataset().getId().equals(dataset.getId())) {
            throw new IllegalArgumentException("Field does not belong to dataset");
        }
        field.setName(request.name());
        field.setDtype(parseDataType(request.dtype()));
        field.setIsNullable(request.isNullable());
        field.setIsUnique(request.isUnique());
        field.setDefaultExpr(request.defaultExpr());
        field.setPosition(request.position());
        return datasetFieldRepository.save(field);
    }

    public long countSources(Long datasetId, String userEmail) {
        return sourceRepository.findAllByDataset_IdAndApplicationUser_Email(datasetId, requireUser(userEmail).getEmail()).size();
    }

    @Transactional
    public TransformRun ingestAndTransform(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        ingestionService.ingestDataset(dataset.getId());
        return transformService.startTransform(dataset.getId());
    }

    public PipelineStatusResponse getPipelineStatus(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        boolean hasSources = countSources(datasetId, userEmail) > 0;
        boolean hasRawData = rawEventRepository.countByDataset_Id(datasetId) > 0;
        boolean hasMappings = countMappings(datasetId, userEmail) > 0;
        boolean hasUnifiedData = unifiedRowRepository.countByDatasetAndIsExcludedFalse(dataset) > 0;
        Instant lastUpdated = unifiedRowRepository.findFirstByDatasetOrderByIngestedAtDesc(dataset)
                .map(UnifiedRow::getIngestedAt)
                .orElse(null);

        return new PipelineStatusResponse(
                dataset.getId(),
                dataset.getName(),
                dataset.getDescription(),
                dataset.getApplicationUser() != null ? dataset.getApplicationUser().getName() : null,
                hasSources,
                hasRawData,
                hasMappings,
                hasUnifiedData,
                hasUnifiedData,
                lastUpdated
        );
    }

    public long countMappings(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        return datasetMappingRepository.findAllByDataset(dataset).size();
    }


    public Dataset getDatasetForUser(Long datasetId, String userEmail) {
        ApplicationUser owner = requireUser(userEmail);
        return datasetRepository.findByIdAndApplicationUser_Id(datasetId, owner.getId())
                .orElseThrow(() -> new IllegalArgumentException("Dataset not found: " + datasetId));
    }


    @Transactional
    public DatasetDetailDTO updateDataset(Long datasetId, UpdateDatasetRequest request, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, userEmail);
        if (request == null) {
            return toDetail(dataset);
        }

        if (StringUtils.hasText(request.name())) {
            dataset.setName(request.name().trim());
        }
        if (request.datasetStatus() != null) {
            dataset.setStatus(request.datasetStatus());
        }
        if (request.description() != null) {
            dataset.setDescription(request.description().trim());
        }
        if (StringUtils.hasText(request.primaryRecordType())) {
            dataset.setPrimaryRecordType(request.primaryRecordType().trim());
        }
        dataset.setUpdatedAt(Instant.now());

        return toDetail(datasetRepository.save(dataset));
    }

    @Transactional
    public void deleteDataset(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, userEmail);
        List<Source> sources = sourceRepository.findAllByDataset_Id(datasetId);
        List<IntegrationConnection> connections = integrationConnectionRepository.findAllByDataset_Id(datasetId);

        sources.forEach(source -> source.setDataset(null));
        connections.forEach(connection -> connection.setDataset(null));

        if (!sources.isEmpty()) {
            sourceRepository.saveAll(sources);
        }
        if (!connections.isEmpty()) {
            integrationConnectionRepository.saveAll(connections);
        }

        datasetRepository.delete(dataset);
    }

    @Transactional
    public List<IngestionRun> ingestDataset(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, userEmail);
        List<Source> sources = sourceRepository.findAllByDataset_IdAndApplicationUser_Email(dataset.getId(), dataset.getApplicationUser().getEmail());
        if (sources.isEmpty()) {
            throw new IllegalStateException("No sources configured for this dataset");
        }

        List<IntegrationConnection> connections = integrationConnectionRepository
                .findAllByDataset_IdAndSource_ApplicationUser_Email(dataset.getId(), dataset.getApplicationUser().getEmail());

        Map<Long, IntegrationConnection> connectionBySource = connections.stream()
                .filter(conn -> conn.getSource() != null && conn.getSource().getId() != null)
                .collect(Collectors.toMap(conn -> conn.getSource().getId(), conn -> conn, (a, b) -> a));

        return sources.stream()
                .map(source -> {
                    IntegrationConnection connection = connectionBySource.get(source.getId());
                    Source destination = connection != null ? connection.getDestination() : null;
                    List<TableSelectionDTO> selections = connection != null ? extractSelections(connection) : List.of();
                    return sourceService.triggerIngestion(source, destination, selections);
                })
                .peek(run -> run.setDataset(dataset))
                .collect(Collectors.toList());
    }


    public TransformRunStatusDTO latestTransformStatus(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        return transformRunRepository.findAllByDatasetOrderByStartedAtDesc(dataset).stream()
                .findFirst()
                .map(run -> new TransformRunStatusDTO(
                        run.getTransformRunUid(),
                        run.getRunStatus(),
                        run.getRowsIn(),
                        run.getRowsOut(),
                        run.getErrorMessage(),
                        run.getStartedAt(),
                        run.getEndedAt()
                ))
                .orElse(null);
    }

    @Transactional(readOnly = true)
    public ExportResultDTO exportUnified(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        List<DatasetField> fields = orderedFields(datasetId);
        List<UnifiedRow> sortedRows = unifiedRowRepository.findOrderedNonExcluded(dataset.getId());
        sortedRows = sortedRows.stream().sorted(unifiedRowOrdering(fields)).toList();
        List<Map<String, Object>> rows = sortedRows.stream()
                .map(UnifiedRow::getData)
                .filter(Objects::nonNull)
                .toList();

        Map<String, Object> destinationResults = integrationConnectionRepository.findAllByDataset_Id(dataset.getId()).stream()
                .filter(conn -> conn.getDestination() != null)
                .collect(Collectors.toMap(
                        conn -> conn.getDestination().getName(),
                        conn -> Map.of(
                                "destinationType", conn.getDestination().getType().name(),
                                "status", "PENDING"
                        ),
                        (a, b) -> a,
                        LinkedHashMap::new
                ));

        long totalSent = 0;
        for (IntegrationConnection connection : integrationConnectionRepository.findAllByDataset_Id(dataset.getId())) {
            if (connection.getDestination() == null) {
                continue;
            }
            Source destination = connection.getDestination();
            destinationResults.putIfAbsent(destination.getName(), new LinkedHashMap<>());
            if (rows.isEmpty()) {
                destinationResults.put(destination.getName(), Map.of("status", "NO_ROWS", "destinationType", destination.getType().name()));
                continue;
            }
            try {
                // reuse ingestion output writer for unified rows
                destinationOutputService.write(destination, rows);
                destinationResults.put(destination.getName(), Map.of(
                        "status", "WRITTEN",
                        "destinationType", destination.getType().name(),
                        "rows", rows.size()
                ));
                totalSent += rows.size();
            } catch (Exception exception) {
                destinationResults.put(destination.getName(), Map.of(
                        "status", "FAILED",
                        "destinationType", destination.getType().name(),
                        "error", exception.getMessage()
                ));
            }
        }

        return new ExportResultDTO(totalSent, destinationResults);
    }

    public ResponseEntity<byte[]> exportUnifiedAsCsv(Long datasetId, String userEmail) {
        Dataset dataset = getDatasetForUser(datasetId, requireUser(userEmail).getEmail());
        List<DatasetField> fields = orderedFields(datasetId);
        List<UnifiedRow> rows = unifiedRowRepository.findOrderedNonExcluded(dataset.getId());
        rows = rows.stream().sorted(unifiedRowOrdering(fields)).toList();
        log.info("[export-csv] Dataset {} SQL: select * from integration.unified_row where dataset_id = {} and (is_excluded = false or is_excluded is null) order by ingested_at asc, unified_row_id asc", datasetId, datasetId);
        log.info("[export-csv] About to write {} unified rows to CSV for dataset {}", rows.size(), datasetId);
        rows.stream().findFirst().ifPresent(sample -> log.info("[export-csv] Sample unified row: {}", sample.getData()));

        if (rows.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "No unified rows available to export. Run a transform first.");
        }

        StringBuilder builder = new StringBuilder();
        List<String> headers = fields.stream().map(DatasetField::getName).toList();
        builder.append(String.join(",", headers)).append('\n');

        for (UnifiedRow row : rows) {
            Map<String, Object> data = row.getData();
            List<String> values = new ArrayList<>();
            for (DatasetField field : fields) {
                Object raw = null;
                if (data != null) {
                    raw = data.get(String.valueOf(field.getId()));
                    if (raw == null) {
                        raw = data.get(field.getName());
                    }
                }
                String text = raw == null ? "" : raw.toString();
                String sanitized = text.replace("\"", "\"\"");
                if (sanitized.contains(",")) {
                    sanitized = '"' + sanitized + '"';
                }
                values.add(sanitized);
            }
            builder.append(String.join(",", values)).append('\n');
        }

        byte[] payload = builder.toString().getBytes(StandardCharsets.UTF_8);
        String filename = buildFilename(dataset.getName(), "csv");
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename)
                .contentType(MediaType.TEXT_PLAIN)
                .body(payload);
    }

    private List<DatasetField> orderedFields(Long datasetId) {
        return datasetFieldRepository.findAllByDataset_Id(datasetId).stream()
                .sorted(Comparator
                        .comparing((DatasetField field) -> Optional.ofNullable(field.getPosition()).orElse(Integer.MAX_VALUE))
                        .thenComparing(DatasetField::getId))
                .toList();
    }

    private Comparator<UnifiedRow> unifiedRowOrdering(List<DatasetField> fields) {
        return Comparator
                .comparing((UnifiedRow row) -> Optional.ofNullable(row.getId()).orElse(0L))
                .thenComparing(row -> Optional.ofNullable(row.getRecordKey()).orElse(""));
    }


    private String buildFilename(String datasetName, String extension) {
        String base = StringUtils.hasText(datasetName) ? datasetName : "dataset";
        String safe = base.replaceAll("[^a-zA-Z0-9-_]+", "-");
        return safe + "." + extension;
    }

    private DatasetDetailDTO toDetail(Dataset dataset) {
        return new DatasetDetailDTO(
                dataset.getId(),
                dataset.getName(),
                dataset.getDescription(),
                dataset.getPrimaryRecordType(),
                dataset.getStatus(),
                dataset.getCreatedAt(),
                dataset.getUpdatedAt()
        );
    }

    private List<TableSelectionDTO> extractSelections(IntegrationConnection connection) {
        List<Map<String, Object>> stored = connection.getTableSelection();
        if (stored == null || stored.isEmpty()) {
            return List.of();
        }
        return stored.stream()
                .map(entry -> {
                    String tableName = stringValue(entry.get("tableName"));
                    if (!StringUtils.hasText(tableName)) {
                        return null;
                    }
                    String schema = stringValue(entry.get("schema"));
                    List<String> columns = extractColumns(entry.get("columns"));
                    return new TableSelectionDTO(tableName, schema, columns);
                })
                .filter(Objects::nonNull)
                .toList();
    }

    private List<String> extractColumns(Object value) {
        if (value instanceof List<?> list) {
            return list.stream()
                    .map(this::stringValue)
                    .filter(StringUtils::hasText)
                    .map(String::trim)
                    .distinct()
                    .collect(Collectors.toList());
        }
        return List.of();
    }

    private String stringValue(Object value) {
        return value == null ? null : Objects.toString(value, null);
    }

    private Dataset getDataset(Long datasetId) {
        return datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset not found"));
    }

    private Dataset persistDataset(String name, String description, DatasetStatus datasetStatus, String primaryRecordType, ApplicationUser owner) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Dataset name is required");
        }

        Dataset dataset = new Dataset();
        dataset.setDatasetUid(AppUtils.generateUUID());
        dataset.setApplicationUser(owner);
        dataset.setName(name.trim());
        dataset.setDescription(description != null ? description.trim() : null);
        dataset.setPrimaryRecordType(StringUtils.hasText(primaryRecordType) ? primaryRecordType.trim() : null);
        dataset.setStatus(datasetStatus != null ? datasetStatus : DatasetStatus.ACTIVE);
        dataset.setCreatedAt(Instant.now());
        dataset.setUpdatedAt(Instant.now());

        return datasetRepository.save(dataset);
    }

    private ApplicationUser requireUser(String userEmail) {
        if (userEmail == null || userEmail.isBlank()) {
            throw new IllegalArgumentException("User email is required");
        }
        return userRepository.findByEmail(userEmail)
                .orElseThrow(() -> new IllegalArgumentException("User not found: " + userEmail));
    }

    private DataType parseDataType(String dtype) {
        if (dtype == null) {
            throw new IllegalArgumentException("Data type is required");
        }
        try {
            return DataType.valueOf(dtype.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unsupported data type: " + dtype, ex);
        }
    }
}
