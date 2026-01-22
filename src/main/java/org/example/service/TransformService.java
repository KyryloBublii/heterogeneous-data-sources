package org.example.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.models.entity.Dataset;
import org.example.models.entity.DatasetField;
import org.example.models.entity.DatasetMapping;
import org.example.models.entity.RawEvent;
import org.example.models.entity.Relationship;
import org.example.models.entity.Source;
import org.example.models.entity.TransformRun;
import org.example.models.entity.UnifiedRow;
import org.example.models.enums.RunStatus;
import org.example.models.enums.DatasetStatus;
import org.example.models.enums.TransformType;
import org.example.repository.DatasetFieldRepository;
import org.example.repository.DatasetMappingRepository;
import org.example.repository.DatasetRepository;
import org.example.repository.RelationshipRepository;
import org.example.repository.RawEventRepository;
import org.example.repository.TransformRunRepository;
import org.example.repository.UnifiedRowRepository;
import org.example.utils.AppUtils;
import org.example.service.transform.GraphBuilder;
import org.example.service.transform.RecordMerger;
import org.example.service.transform.TimestampNormalizer;
import org.example.service.transform.UnifiedPayloadNormalizer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransformService {

    private final DatasetRepository datasetRepository;
    private final DatasetMappingRepository datasetMappingRepository;
    private final DatasetFieldRepository datasetFieldRepository;
    private final RelationshipRepository relationshipRepository;
    private final RawEventRepository rawEventRepository;
    private final UnifiedRowRepository unifiedRowRepository;
    private final TransformRunRepository transformRunRepository;

    @Transactional
    public TransformRun startTransform(Long datasetId) {
        Dataset dataset = datasetRepository.findById(datasetId)
                .orElseThrow(() -> new IllegalArgumentException("Dataset not found: " + datasetId));

        List<RawEvent> events = rawEventRepository.findByDataset_Id(datasetId);
        Map<String, List<RecordContext>> recordsById = indexRecords(events);
        List<Relationship> relationships = relationshipRepository.findByDatasetId(datasetId);

        log.info("[transform] Loaded {} raw events and {} relationships for dataset {}", events.size(), relationships.size(), datasetId);

        String primaryRecordType = Optional.ofNullable(dataset.getPrimaryRecordType())
                .filter(StringUtils::hasText)
                .map(this::normalizeType)
                .orElse(null);

        if (!StringUtils.hasText(primaryRecordType)) {
            primaryRecordType = determinePrimaryType(relationships, recordsById, events)
                    .map(this::normalizeType)
                    .orElseGet(() -> fallbackPrimaryType(events));
        }

        if (!StringUtils.hasText(primaryRecordType)) {
            primaryRecordType = "default";
            log.warn("[transform] Falling back to default primary record type for dataset {} because none could be detected",
                    datasetId);
        }

        dataset.setPrimaryRecordType(primaryRecordType.trim());
        dataset.setUpdatedAt(Instant.now());
        datasetRepository.save(dataset);

        log.info("[transform] Using primaryRecordType='{}' for dataset {}", dataset.getPrimaryRecordType(), datasetId);

        TransformRun run = new TransformRun();
        run.setTransformRunUid(AppUtils.generateUUID());
        run.setDataset(dataset);
        run.setRunStatus(RunStatus.RUNNING);
        run.setStartedAt(Instant.now());
        run = transformRunRepository.save(run);

        int rowsIn = 0;
        int rowsOut = 0;
        try {
            unifiedRowRepository.deleteByDataset(dataset);
            List<DatasetField> fields = datasetFieldRepository.findAllByDataset_Id(datasetId)
                    .stream()
                    .sorted(Comparator.comparing(DatasetField::getPosition, Comparator.nullsLast(Integer::compareTo)))
                    .toList();

            List<DatasetMapping> mappingList = datasetMappingRepository.findAllByDataset(dataset);
            Map<Long, Map<Long, List<DatasetMapping>>> mappingsBySource = mappingList
                    .stream()
                    .collect(Collectors.groupingBy(mapping -> mapping.getSource().getId(),
                            Collectors.groupingBy(mapping -> mapping.getDatasetField().getId())));
            Map<Long, List<DatasetMapping>> mappingsByField = mappingList.stream()
                    .collect(Collectors.groupingBy(mapping -> mapping.getDatasetField().getId()));

            String normalizedPrimary = normalizeType(primaryRecordType);

            List<Relationship> relevantRelations = relationships.stream()
                    .sorted(Comparator
                            .comparing((Relationship rel) -> primaryKey(rel, normalizedPrimary), Comparator.nullsLast(String::compareTo))
                            .thenComparing(Relationship::getIngestedAt, Comparator.nullsLast(Comparator.naturalOrder())))
                    .toList();

            if (relevantRelations.isEmpty()) {
                log.warn("No relationships found for dataset {}. Generating unified rows directly from raw events.", datasetId);
                for (List<RecordContext> contexts : recordsById.values()) {
                    for (RecordContext ctx : contexts) {
                        rowsIn++;
                        Map<Long, Map<String, Object>> payloadBySource = new LinkedHashMap<>();
                        Map<String, Object> unifiedPayload = new LinkedHashMap<>();
                        mergePayload(payloadBySource, unifiedPayload, ctx);
                        Map<String, Object> unified = applyMappings(fields, mappingsBySource, mappingsByField, payloadBySource, unifiedPayload);
                        UnifiedRow row = new UnifiedRow();
                        row.setUnifiedRowUid(AppUtils.generateUUID());
                        row.setDataset(dataset);
                        row.setSource(ctx.source());
                        row.setRecordKey(ctx.recordKey());
                        row.setObservedAt(ctx.createdAt());
                        row.setIngestedAt(Instant.now());
                        row.setIsExcluded(false);
                        row.setData(unified);
                        unifiedRowRepository.save(row);
                        rowsOut++;
                        if (rowsOut <= 2) {
                            log.info("[transform] Sample unified row (no-rel): {}", unified);
                        }
                    }
                }
            }

            GraphBuilder.Graph graph = GraphBuilder.build(relevantRelations);
            Map<GraphBuilder.NodeRef, List<RecordContext>> contextsByNode = mapContextsByNode(recordsById);

            log.info("[CTX DEBUG] contextsByNode keys={}", contextsByNode.keySet());
            log.info("[GRAPH DEBUG] graph nodes={}", graph.nodes());
            for (GraphBuilder.NodeRef node : graph.nodes()) {
                log.info("[GRAPH DEBUG] neighbors of {} = {}", node, graph.neighbors(node));
            }

            List<GraphBuilder.NodeRef> primaryNodes = contextsByNode.keySet().stream()
                    .filter(node -> normalizedPrimary == null || normalizedPrimary.equals(node.type()))
                    .toList();

            Set<String> processedPrimaryIds = new HashSet<>();

            for (GraphBuilder.NodeRef primaryNode : primaryNodes) {
                String primaryId = primaryNode.id();
                if (!StringUtils.hasText(primaryId)) {
                    continue;
                }

                if (!processedPrimaryIds.add(primaryId)) {
                    continue;
                }

                Queue<GraphBuilder.NodeRef> queue = new ArrayDeque<>();
                Set<GraphBuilder.NodeRef> localVisited = new HashSet<>();

                queue.add(primaryNode);
                localVisited.add(primaryNode);

                List<RecordContext> primaryContexts = new ArrayList<>(contextsByNode.getOrDefault(primaryNode, List.of()));
                List<RecordContext> relatedContexts = new ArrayList<>();
                List<Map<String, Object>> relationPayloads = new ArrayList<>();

                while (!queue.isEmpty()) {
                    GraphBuilder.NodeRef current = queue.poll();
                    log.info("[BFS DEBUG] dequeue node={}", current);
                    log.info("[BFS DEBUG] neighbors={} for current={} ", graph.neighbors(current), current);
                    for (GraphBuilder.Edge edge : graph.neighbors(current)) {
                        GraphBuilder.NodeRef target = edge.target();

                        if (edge.relation().getPayload() != null) {
                            relationPayloads.add(UnifiedPayloadNormalizer.normalizePayload(edge.relation().getPayload()));
                        }

                        if (isOtherPrimary(normalizedPrimary, primaryNode, target)) {
                            // Skip merging other primary records into this row so each order stays isolated.
                            log.info("[BFS DEBUG] SKIPPED neighbor {} because isOtherPrimary", target);
                            continue;
                        }

                        if (!localVisited.add(target)) {
                            continue;
                        }

                        log.info("[BFS DEBUG] enqueue target={} from edge={} ", target, edge.relation().getRelationshipUid());

                        queue.add(target);

                        List<RecordContext> contexts = contextsByNode.get(target);
                        if (contexts == null || contexts.isEmpty()) {
                            contexts = ensureRecords(recordsById, target.id(), target.type(), edge.relation(), false);
                            if (contexts != null && !contexts.isEmpty()) {
                                contextsByNode.put(target, contexts);
                            }
                        }

                        if (contexts != null && !contexts.isEmpty() && !target.equals(primaryNode)) {
                            relatedContexts.addAll(contexts);
                        }
                    }
                }

                if (primaryContexts.isEmpty() && relatedContexts.isEmpty()) {
                    continue;
                }

                rowsIn += primaryContexts.size() + relatedContexts.size();

                Map<Long, Map<String, Object>> payloadBySource = new LinkedHashMap<>();
                Map<String, Object> unifiedPayload = new LinkedHashMap<>();

                RecordMerger.merge(payloadBySource, unifiedPayload, primaryContexts, relationPayloads, relatedContexts);

                log.info("[transform] Component nodes for primary {}: {}", primaryId, localVisited);
                log.debug("[transform] Unified payload before mapping for {}: {}", primaryId, unifiedPayload);

                RecordContext primaryCtx = primaryContexts.stream().findFirst().orElse(null);
                RecordContext relatedCtx = relatedContexts.stream().findFirst().orElse(null);

                String primaryRecordId = primaryCtx != null ? primaryCtx.recordId() : primaryId;
                String relatedRecordId = relatedCtx != null ? relatedCtx.recordId() : null;

                Map<String, Object> unified = applyMappings(fields, mappingsBySource, mappingsByField, payloadBySource, unifiedPayload);
                UnifiedRow row = new UnifiedRow();
                row.setUnifiedRowUid(AppUtils.generateUUID());
                row.setDataset(dataset);
                row.setSource(primaryCtx != null ? primaryCtx.source() : (relatedCtx != null ? relatedCtx.source() : null));
                row.setRecordKey(buildRecordKey(primaryRecordId, relatedRecordId,
                        primaryCtx != null ? primaryCtx.recordKey() : null,
                        relatedCtx != null ? relatedCtx.recordKey() : null));

                Instant observedAt = primaryCtx != null && primaryCtx.createdAt() != null
                        ? primaryCtx.createdAt()
                        : (relatedCtx != null ? relatedCtx.createdAt() : null);
                row.setObservedAt(observedAt);
                row.setIngestedAt(Instant.now());
                row.setIsExcluded(false);
                row.setData(unified);
                unifiedRowRepository.save(row);
                rowsOut++;
                if (rowsOut <= 2) {
                    log.info("[transform] Sample unified row (relation-primary): {}", unified);
                }
            }

            log.info("[transform] Completed dataset {} transform with rowsIn={} rowsOut={}", datasetId, rowsIn, rowsOut);

            run.setRowsIn(rowsIn);
            run.setRowsOut(rowsOut);
            run.setRunStatus(RunStatus.SUCCESS);
            run.setEndedAt(Instant.now());
            run.setErrorMessage(null);
            dataset.setStatus(DatasetStatus.FINISHED);
            dataset.setUpdatedAt(Instant.now());
            datasetRepository.save(dataset);
        } catch (Exception exception) {
            log.error("Transformation failed for dataset {}", datasetId, exception);
            run.setRowsIn(rowsIn);
            run.setRowsOut(rowsOut);
            run.setRunStatus(RunStatus.FAILED);
            run.setEndedAt(Instant.now());
            run.setErrorMessage(exception.getMessage());
            throw exception;
        } finally {
            transformRunRepository.save(run);
        }

        return run;
    }

    private Map<String, Object> applyMappings(List<DatasetField> fields,
                                              Map<Long, Map<Long, List<DatasetMapping>>> mappingsBySource,
                                              Map<Long, List<DatasetMapping>> mappingsByField,
                                              Map<Long, Map<String, Object>> payloadBySource,
                                              Map<String, Object> unifiedPayload) {
        Map<String, Object> result = new LinkedHashMap<>();
        Map<String, Object> flattenedUnified = flattenPayload(unifiedPayload);

        for (DatasetField field : fields) {
            Object value = null;
            List<MappingCandidate> candidates = new ArrayList<>();

            for (Map.Entry<Long, Map<String, Object>> entry : payloadBySource.entrySet()) {
                Map<Long, List<DatasetMapping>> byField = mappingsBySource.getOrDefault(entry.getKey(), Map.of());
                List<DatasetMapping> mappings = byField.getOrDefault(field.getId(), List.of())
                        .stream()
                        .sorted(Comparator.comparing(DatasetMapping::getPriority, Comparator.nullsLast(Integer::compareTo)))
                        .toList();
                for (DatasetMapping mapping : mappings) {
                    candidates.add(new MappingCandidate(mapping, entry.getValue()));
                }
            }

            for (MappingCandidate candidate : candidates) {
                DatasetMapping mapping = candidate.mapping();
                String path = StringUtils.hasText(mapping.getSrcPath()) ? mapping.getSrcPath() : mapping.getSrcJsonPath();
                TransformType transformType = Optional.ofNullable(mapping.getTransformType()).orElse(TransformType.NONE);
                value = applyTransform(extractValue(candidate.payload(), path), transformType);
                if (value != null || Boolean.TRUE.equals(mapping.getRequired())) {
                    break;
                }
            }

            if (value == null && mappingsByField != null) {
                List<DatasetMapping> fallbackMappings = mappingsByField.getOrDefault(field.getId(), List.of())
                        .stream()
                        .sorted(Comparator.comparing(DatasetMapping::getPriority, Comparator.nullsLast(Integer::compareTo)))
                        .toList();
                for (DatasetMapping mapping : fallbackMappings) {
                    String path = StringUtils.hasText(mapping.getSrcPath()) ? mapping.getSrcPath() : mapping.getSrcJsonPath();
                    TransformType transformType = Optional.ofNullable(mapping.getTransformType()).orElse(TransformType.NONE);
                    value = applyTransform(extractValue(unifiedPayload, path), transformType);
                    if (value != null || Boolean.TRUE.equals(mapping.getRequired())) {
                        break;
                    }
                }
            }

            if (value == null && unifiedPayload != null) {
                Object fallback = unifiedPayload.getOrDefault(field.getName(), extractValue(unifiedPayload, field.getName()));
                value = fallback;
            }

            if (value == null && unifiedPayload != null) {
                String caseInsensitiveKey = findCaseInsensitiveKey(unifiedPayload, field.getName());
                if (caseInsensitiveKey != null) {
                    value = unifiedPayload.get(caseInsensitiveKey);
                }
            }

            if (value == null) {
                value = fuzzyLookup(unifiedPayload, field.getName());
            }

            if (value == null && !flattenedUnified.isEmpty()) {
                List<String> targets = new ArrayList<>();
                targets.add(field.getName());
                if (mappingsByField != null) {
                    List<DatasetMapping> allMappings = mappingsByField.getOrDefault(field.getId(), List.of());
                    for (DatasetMapping mapping : allMappings) {
                        if (StringUtils.hasText(mapping.getSrcPath())) {
                            targets.add(mapping.getSrcPath());
                        }
                        if (StringUtils.hasText(mapping.getSrcJsonPath())) {
                            targets.add(mapping.getSrcJsonPath());
                        }
                    }
                }
                for (String target : targets) {
                    String normalized = normalizeKey(target);
                    if (flattenedUnified.containsKey(normalized)) {
                        value = flattenedUnified.get(normalized);
                        break;
                    }
                }
            }

            if (value == null && payloadBySource != null) {
                for (Map<String, Object> payload : payloadBySource.values()) {
                    value = fuzzyLookup(payload, field.getName());
                    if (value != null) {
                        break;
                    }
                }
            }

            result.put(String.valueOf(field.getId()), value);
        }

        boolean allNull = result.values().stream().allMatch(Objects::isNull);
        if (allNull && unifiedPayload != null) {
            for (DatasetField field : fields) {
                if (result.get(String.valueOf(field.getId())) != null) {
                    continue;
                }
                String key = findCaseInsensitiveKey(unifiedPayload, field.getName());
                if (key != null) {
                    result.put(String.valueOf(field.getId()), unifiedPayload.get(key));
                }
            }
        }

        return result;
    }

    private Map<String, Object> flattenPayload(Object payload) {
        Map<String, Object> flat = new LinkedHashMap<>();
        flattenRecursive(payload, flat);
        return flat;
    }

    @SuppressWarnings("unchecked")
    private void flattenRecursive(Object current, Map<String, Object> flat) {
        if (current == null) {
            return;
        }
        if (current instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                String normalizedKey = normalizeKey(entry.getKey().toString());
                Object value = TimestampNormalizer.normalizeValue(entry.getValue());
                if (value instanceof Map<?, ?> || value instanceof List<?>) {
                    flattenRecursive(value, flat);
                } else if (!flat.containsKey(normalizedKey)) {
                    flat.put(normalizedKey, value);
                }
            }
        } else if (current instanceof List<?> list) {
            for (Object item : list) {
                flattenRecursive(TimestampNormalizer.normalizeValue(item), flat);
            }
        }
    }

    private String findCaseInsensitiveKey(Map<String, Object> source, String targetName) {
        if (source == null || !StringUtils.hasText(targetName)) {
            return null;
        }
        String normalized = targetName.trim().toLowerCase();
        for (String key : source.keySet()) {
            if (key != null && key.trim().toLowerCase().equals(normalized)) {
                return key;
            }
        }
        return null;
    }

    private Object fuzzyLookup(Object payload, String targetName) {
        if (payload == null || !StringUtils.hasText(targetName)) {
            return null;
        }
        String normalizedTarget = normalizeKey(targetName);

        if (payload instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() == null) {
                    continue;
                }
                String key = entry.getKey().toString();
                Object normalizedValue = TimestampNormalizer.normalizeValue(entry.getValue());
                if (normalizeKey(key).equals(normalizedTarget)) {
                    return normalizedValue;
                }
                Object nested = normalizedValue;
                if (nested instanceof Map<?, ?> || nested instanceof List<?>) {
                    Object nestedResult = fuzzyLookup(nested, targetName);
                    if (nestedResult != null) {
                        return nestedResult;
                    }
                }
            }
        } else if (payload instanceof List<?> list) {
            for (Object item : list) {
                Object nestedResult = fuzzyLookup(item, targetName);
                if (nestedResult != null) {
                    return nestedResult;
                }
            }
        }

        return null;
    }

    private String normalizeKey(String key) {
        if (!StringUtils.hasText(key)) {
            return "";
        }
        return key.trim().toLowerCase().replaceAll("[^a-z0-9]", "");
    }

    private Optional<String> determinePrimaryType(List<Relationship> relationships,
                                                  Map<String, List<RecordContext>> recordsById,
                                                  List<RawEvent> events) {
        Map<String, TypeStats> stats = new HashMap<>();

        for (Relationship relation : relationships) {
            accumulateType(stats, normalizeType(relation.getFromType()), relation.getFromId());
            accumulateType(stats, normalizeType(relation.getToType()), relation.getToId());
        }

        Optional<String> bestByRelations = stats.entrySet().stream()
                .filter(entry -> StringUtils.hasText(entry.getKey()))
                .max(Comparator
                        .comparing((Map.Entry<String, TypeStats> e) -> e.getValue().distinctIds().size())
                        .thenComparing(e -> e.getValue().relationCount()))
                .map(Map.Entry::getKey);

        if (bestByRelations.isPresent()) {
            return bestByRelations;
        }

        Map<String, Long> typeCounts = new HashMap<>();
        for (List<RecordContext> contexts : recordsById.values()) {
            for (RecordContext ctx : contexts) {
                if (StringUtils.hasText(ctx.recordType())) {
                    typeCounts.merge(ctx.recordType(), 1L, Long::sum);
                }
            }
        }
        if (typeCounts.isEmpty()) {
            for (RawEvent event : events) {
                String type = resolveRecordType(event.getPayload());
                if (StringUtils.hasText(type)) {
                    typeCounts.merge(type, 1L, Long::sum);
                }
            }
        }

        return typeCounts.entrySet().stream()
                .filter(entry -> StringUtils.hasText(entry.getKey()))
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }

    private String fallbackPrimaryType(List<RawEvent> events) {
        for (RawEvent event : events) {
            String type = normalizeType(resolveRecordType(event.getPayload()));
            if (StringUtils.hasText(type)) {
                return type;
            }
        }
        return null;
    }

    private void accumulateType(Map<String, TypeStats> stats, String type, String id) {
        String normalizedId = normalizeId(id);
        if (!StringUtils.hasText(type) || !StringUtils.hasText(normalizedId)) {
            return;
        }
        TypeStats current = stats.computeIfAbsent(type, key -> new TypeStats(new HashSet<>(), 0));
        current.distinctIds().add(normalizedId);
        current.increment();
    }

    private Map<String, List<RecordContext>> indexRecords(List<RawEvent> events) {
        Map<String, List<RecordContext>> index = new HashMap<>();
        for (RawEvent event : events) {
            Map<String, Object> payload = Optional.ofNullable(event.getPayload()).orElse(Map.of());
            payload = UnifiedPayloadNormalizer.normalizePayload(payload);
            String recordType = normalizeType(resolveRecordType(payload));

            LinkedHashSet<String> candidateIds = collectCandidateIds(payload,
                    Optional.ofNullable(event.getRawEventUid()).orElseGet(() -> String.valueOf(event.getId())));
            if (!StringUtils.hasText(recordType) || candidateIds.isEmpty()) {
                continue;
            }

            String primaryId = candidateIds.iterator().next();
            log.info("[IDX DEBUG] recordType={}, candidateIds={}, chosenPrimaryId={}, payloadKeys={}",
                    recordType, candidateIds, primaryId, payload.keySet());
            log.info("[IDX DEBUG] meta __table__={}, __schema__={}, __record_uid__={}",
                    payload.get("__table__"), payload.get("__schema__"), payload.get("__record_uid__"));
            RecordContext context = new RecordContext(
                    event.getSource(),
                    payload,
                    event.getCreatedAt(),
                    recordType,
                    primaryId,
                    Optional.ofNullable(event.getRawEventUid()).orElseGet(() -> String.valueOf(event.getId()))
            );

            for (String candidate : candidateIds) {
                for (String key : canonicalIds(candidate)) {
                    List<RecordContext> list = index.computeIfAbsent(key, k -> new ArrayList<>());
                    if (!list.contains(context)) {
                        list.add(context);
                    }
                }
            }
        }
        return index;
    }

    private LinkedHashSet<String> collectCandidateIds(Map<String, Object> payload, String fallbackId) {
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        if (payload != null) {
            Object meta = payload.get("__meta__");
            if (meta instanceof Map<?, ?> metaMap) {
                for (String key : List.of("record_uid", "recordUid", "id", "uid")) {
                    Object val = metaMap.get(key);
                    if (val != null && StringUtils.hasText(val.toString())) {
                        candidates.add(normalizeId(val.toString()));
                    }
                }
            }

            for (String key : payload.keySet()) {
                Object val = payload.get(key);
                if (val == null) {
                    continue;
                }
                String lower = key.toLowerCase();
                if (lower.equals("id") || lower.endsWith("_id") || lower.endsWith("id")) {
                    String normalized = normalizeId(val.toString());
                    if (StringUtils.hasText(normalized)) {
                        candidates.add(normalized);
                    }
                }
            }

            Object direct = resolveRecordId(payload);
            if (direct != null && StringUtils.hasText(direct.toString())) {
                candidates.add(normalizeId(direct.toString()));
            }
        }

        if (StringUtils.hasText(fallbackId)) {
            candidates.add(normalizeId(fallbackId));
        }

        return candidates.stream()
                .filter(StringUtils::hasText)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private List<RecordContext> ensureRecords(Map<String, List<RecordContext>> index,
                                              String recordId,
                                              String recordType,
                                              Relationship relation,
                                              boolean primarySide) {
        List<RecordContext> found = findRecords(index, recordId, recordType);
        if (!found.isEmpty()) {
            return found;
        }

        List<RecordContext> contained = findByPayloadValue(index, recordId);
        if (!contained.isEmpty()) {
            return contained;
        }

        String normalizedType = normalizeType(recordType);

        if (StringUtils.hasText(normalizedType)) {
            List<RecordContext> byType = findByType(index, normalizedType);
            if (!byType.isEmpty()) {
                return byType;
            }
        }

        // As a last-resort, try any available contexts before synthesizing an empty placeholder so we retain
        // real payloads even when relationship ids/types do not line up perfectly (common with CSV-only datasets).
        final String normalizedTypeFinal = normalizedType;
        List<RecordContext> anyContexts = index.values().stream()
                .flatMap(List::stream)
                .distinct()
                .collect(Collectors.toList());
        if (!anyContexts.isEmpty()) {
            if (StringUtils.hasText(normalizedType)) {
                List<RecordContext> matchingType = anyContexts.stream()
                        .filter(ctx -> normalizedTypeFinal.equals(ctx.recordType()))
                        .toList();
                if (!matchingType.isEmpty()) {
                    return matchingType;
                }
            }
            return anyContexts;
        }

        String normalizedId = normalizeId(recordId);
        if (!StringUtils.hasText(normalizedId)) {
            normalizedId = normalizeId(primarySide ? relation.getFromId() : relation.getToId());
        }
        if (!StringUtils.hasText(normalizedId)) {
            normalizedId = Optional.ofNullable(relation.getRelationshipUid())
                    .orElseGet(() -> "relation-" + relation.getId());
        }

        if (!StringUtils.hasText(normalizedType)) {
            normalizedType = primarySide ? normalizeType(relation.getFromType()) : normalizeType(relation.getToType());
        }

        Map<String, Object> syntheticPayload = new LinkedHashMap<>();
        if (relation.getPayload() != null) {
            syntheticPayload.putAll(relation.getPayload());
        }

        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("record_uid", normalizedId);
        if (StringUtils.hasText(normalizedType)) {
            meta.put("type", normalizedType);
        }
        if (!meta.isEmpty()) {
            syntheticPayload.put("__meta__", meta);
        }
        if (StringUtils.hasText(normalizedType)) {
            syntheticPayload.put("__table__", normalizedType);
        }

        RecordContext placeholder = new RecordContext(
                relation.getSource(),
                syntheticPayload,
                relation.getIngestedAt(),
                normalizedType,
                normalizedId,
                relation.getRelationshipUid()
        );

        for (String variant : canonicalIds(normalizedId)) {
            List<RecordContext> list = index.computeIfAbsent(variant, k -> new ArrayList<>());
            if (!list.contains(placeholder)) {
                list.add(placeholder);
            }
        }

        return List.of(placeholder);
    }

    private List<RecordContext> findRecords(Map<String, List<RecordContext>> index, String recordId, String recordType) {
        if (!StringUtils.hasText(recordId)) {
            return List.of();
        }

        List<RecordContext> aggregate = new ArrayList<>();
        for (String variant : canonicalIds(recordId)) {
            List<RecordContext> contexts = index.getOrDefault(variant, List.of());
            aggregate.addAll(contexts);
        }

        if (aggregate.isEmpty()) {
            return List.of();
        }

        if (!StringUtils.hasText(recordType)) {
            return aggregate;
        }
        String normalized = normalizeType(recordType);
        List<RecordContext> matched = aggregate.stream()
                .filter(ctx -> ctx.recordType() != null && ctx.recordType().equals(normalized))
                .toList();
        return matched.isEmpty() ? aggregate : matched;
    }

    private List<RecordContext> findByType(Map<String, List<RecordContext>> index, String normalizedType) {
        if (!StringUtils.hasText(normalizedType)) {
            return List.of();
        }
        List<RecordContext> results = new ArrayList<>();
        for (List<RecordContext> contexts : index.values()) {
            for (RecordContext ctx : contexts) {
                if (normalizedType.equals(ctx.recordType()) && !results.contains(ctx)) {
                    results.add(ctx);
                }
            }
        }
        return results;
    }



    private boolean isPrimarySide(String type, String normalizedPrimary) {
        return normalizeType(type) != null && normalizeType(type).equals(normalizedPrimary);
    }

    private String normalizeId(String raw) {
        if (!StringUtils.hasText(raw)) {
            return null;
        }

        String trimmed = raw.trim();

        // Relationship rows occasionally store the full payload as the id (e.g.,
        // "{customer_id:CUST-100,first_name:Olivia,...}"). Extract a likely identifier
        // token to match the scalar ids used by record contexts.
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            String inner = trimmed.substring(1, trimmed.length() - 1);
            String[] parts = inner.split(",");
            for (String part : parts) {
                String[] kv = part.split(":", 2);
                if (kv.length != 2) {
                    continue;
                }
                String key = kv[0].trim().replace("\"", "").replace("`", "");
                String val = kv[1].trim().replace("\"", "").replace("`", "");
                if (!StringUtils.hasText(val)) {
                    continue;
                }
                if (key.endsWith("id") || key.endsWith("_id") || key.contains("id")) {
                    return val;
                }
            }
        }

        return trimmed.replace("\"", "").replace("`", "");
    }

    private Map<GraphBuilder.NodeRef, List<RecordContext>> mapContextsByNode(Map<String, List<RecordContext>> recordsById) {
        Map<GraphBuilder.NodeRef, List<RecordContext>> byNode = new LinkedHashMap<>();
        for (List<RecordContext> contexts : recordsById.values()) {
            for (RecordContext context : contexts) {
                GraphBuilder.NodeRef node = toNodeRef(context.recordType(), context.recordId());
                if (node == null) {
                    continue;
                }
                log.info("[CTX DEBUG] context key type={}, id={} (after TransformService normalize)", node.type(), node.id());
                byNode.computeIfAbsent(node, key -> new ArrayList<>()).add(context);
            }
        }
        return byNode;
    }


    private boolean isOtherPrimary(String normalizedPrimary, GraphBuilder.NodeRef primaryNode, GraphBuilder.NodeRef candidate) {
        if (normalizedPrimary == null || primaryNode == null || candidate == null) {
            return false;
        }
        return normalizedPrimary.equals(candidate.type()) && !primaryNode.id().equals(candidate.id());
    }

    private GraphBuilder.NodeRef toNodeRef(String recordType, String recordId) {
        String normalizedType = normalizeType(recordType);
        String normalizedId = normalizeId(recordId);
        if (!StringUtils.hasText(normalizedType) || !StringUtils.hasText(normalizedId)) {
            return null;
        }
        return new GraphBuilder.NodeRef(normalizedType, normalizedId);
    }

    private List<String> canonicalIds(String raw) {
        String normalized = normalizeId(raw);
        if (!StringUtils.hasText(normalized)) {
            return List.of();
        }
        LinkedHashSet<String> variants = new LinkedHashSet<>();
        variants.add(normalized);
        variants.add(normalized.toLowerCase());
        String collapsed = normalized.replaceAll("[^A-Za-z0-9_-]", "");
        if (StringUtils.hasText(collapsed)) {
            variants.add(collapsed);
            variants.add(collapsed.toLowerCase());
        }
        return new ArrayList<>(variants);
    }

    private String primaryKey(Relationship relation, String normalizedPrimary) {
        if (isPrimarySide(relation.getFromType(), normalizedPrimary)) {
            return relation.getFromId();
        }
        if (isPrimarySide(relation.getToType(), normalizedPrimary)) {
            return relation.getToId();
        }
        return "";
    }

    private void mergePayload(Map<Long, Map<String, Object>> target, Map<String, Object> unifiedPayload, RecordContext context) {
        if (context == null) {
            return;
        }
        Map<String, Object> normalized = UnifiedPayloadNormalizer.normalizePayload(context.payload());
        if (context.source() != null) {
            Map<String, Object> merged = new LinkedHashMap<>(target.getOrDefault(context.source().getId(), Map.of()));
            for (Map.Entry<String, Object> entry : normalized.entrySet()) {
                safePut(merged, entry.getKey(), entry.getValue());
            }
            target.put(context.source().getId(), merged);
        }
        for (Map.Entry<String, Object> entry : normalized.entrySet()) {
            safePut(unifiedPayload, entry.getKey(), entry.getValue());
        }
    }

    private void safePut(Map<String, Object> target, String key, Object value) {
        if (target == null || key == null) {
            return;
        }
        Object normalizedValue = TimestampNormalizer.normalizeValue(value);
        if (normalizedValue == null) {
            return;
        }
        target.put(key, normalizedValue);
    }

    private String buildRecordKey(String primaryId, String relatedId, String primaryKey, String relatedKey) {
        if (StringUtils.hasText(primaryKey) && StringUtils.hasText(relatedKey)) {
            return primaryKey + ":" + relatedKey;
        }
        if (StringUtils.hasText(primaryId) && StringUtils.hasText(relatedId)) {
            return primaryId + ":" + relatedId;
        }
        return Optional.ofNullable(primaryKey).orElse(primaryId);
    }


    private List<RecordContext> findByPayloadValue(Map<String, List<RecordContext>> index, String recordId) {
        String normalized = normalizeId(recordId);
        if (!StringUtils.hasText(normalized)) {
            return List.of();
        }

        List<RecordContext> matches = new ArrayList<>();
        for (List<RecordContext> contexts : index.values()) {
            for (RecordContext ctx : contexts) {
                if (ctx.payload() == null) {
                    continue;
                }
                Map<String, Object> flattened = flattenPayload(ctx.payload());
                if (flattened.values().stream().anyMatch(val -> normalized.equals(normalizeId(String.valueOf(val))))) {
                    if (!matches.contains(ctx)) {
                        matches.add(ctx);
                    }
                }
            }
        }
        return matches;
    }

    private String resolveRecordId(Map<String, Object> payload) {
        if (payload == null) {
            return null;
        }
        Object meta = payload.get("__meta__");
        if (meta instanceof Map<?, ?> metaMap) {
            for (String key : List.of("record_uid", "recordUid", "id")) {
                Object value = metaMap.get(key);
                if (value != null && StringUtils.hasText(value.toString())) {
                    return value.toString();
                }
            }
        }
        for (String key : List.of("id", "uid", "record_id", "recordId")) {
            Object value = payload.get(key);
            if (value != null && StringUtils.hasText(value.toString())) {
                return value.toString();
            }
        }
        return null;
    }

    private String resolveRecordType(Map<String, Object> payload) {
        if (payload == null) {
            return null;
        }
        Object table = payload.get("__table__");
        if (table != null) {
            return table.toString();
        }
        Object meta = payload.get("__meta__");
        if (meta instanceof Map<?, ?> map) {
            for (String key : List.of("destination_table", "record_type", "type")) {
                Object value = map.get(key);
                if (value != null && StringUtils.hasText(value.toString())) {
                    return value.toString();
                }
            }
        }
        Object fallback = payload.get("type");
        return fallback != null ? fallback.toString() : null;
    }

    private String normalizeType(String rawType) {
        if (!StringUtils.hasText(rawType)) {
            return null;
        }
        String normalized = rawType.trim().toLowerCase();
        if (normalized.contains(".")) {
            normalized = normalized.substring(normalized.lastIndexOf('.') + 1);
        }
        normalized = normalized.replace("\"", "").replace("`", "");
        return normalized;
    }

    public record MappingCandidate(DatasetMapping mapping, Map<String, Object> payload) {
    }

    public record RecordContext(Source source, Map<String, Object> payload, Instant createdAt, String recordType, String recordId,
                                 String recordKey) {
    }


    private static class TypeStats {
        private final HashSet<String> distinctIds;
        private int relationCount;

        private TypeStats(HashSet<String> distinctIds, int relationCount) {
            this.distinctIds = distinctIds;
            this.relationCount = relationCount;
        }

        public HashSet<String> distinctIds() {
            return distinctIds;
        }

        public int relationCount() {
            return relationCount;
        }

        public void increment() {
            this.relationCount++;
        }
    }

    private Object extractValue(Object current, String path) {
        if (current == null || !StringUtils.hasText(path)) {
            return null;
        }

        String[] segments = path.split("\\.");
        Object value = current;

        for (String rawSegment : segments) {
            if (value == null) {
                return null;
            }

            String segment = rawSegment;
            Integer index = null;
            if (rawSegment.contains("[")) {
                int start = rawSegment.indexOf('[');
                int end = rawSegment.indexOf(']', start);
                if (end > start) {
                    segment = rawSegment.substring(0, start);
                    String indexStr = rawSegment.substring(start + 1, end);
                    try {
                        index = Integer.parseInt(indexStr);
                    } catch (NumberFormatException ignored) {
                        index = null;
                    }
                }
            }

            if (value instanceof Map<?, ?> mapValue) {
                value = mapValue.get(segment);
            } else {
                return null;
            }

            if (index != null && value instanceof List<?> listValue) {
                value = index >= 0 && index < listValue.size() ? listValue.get(index) : null;
            }
        }

        return TimestampNormalizer.normalizeValue(value);
    }

    private Object applyTransform(Object value, TransformType transformType) {
        if (value == null) {
            return null;
        }
        try {
            return switch (transformType) {
                case LOWERCASE -> value.toString().toLowerCase();
                case UPPERCASE -> value.toString().toUpperCase();
                case TRIM -> value.toString().trim();
                case INT -> {
                    try {
                        yield Integer.parseInt(value.toString());
                    } catch (NumberFormatException ex) {
                        yield value;
                    }
                }
                case FLOAT -> {
                    try {
                        yield Double.parseDouble(value.toString());
                    } catch (NumberFormatException ex) {
                        yield value;
                    }
                }
                case NONE -> value;
            };
        } catch (Exception exception) {
            log.warn("Failed to apply transform '{}' on value {}", transformType, value, exception);
            return value;
        }
    }
}
