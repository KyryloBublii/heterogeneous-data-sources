package org.example.service.transform;

import org.example.service.TransformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class RecordMerger {

    private static final Logger log = LoggerFactory.getLogger(RecordMerger.class);

    private RecordMerger() {
    }

    public static void merge(Map<Long, Map<String, Object>> payloadBySource,
                             Map<String, Object> unifiedPayload,
                             List<TransformService.RecordContext> primaryContexts,
                             List<Map<String, Object>> relationPayloads,
                             List<TransformService.RecordContext> relatedContexts) {
        if (!CollectionUtils.isEmpty(primaryContexts)) {
            for (TransformService.RecordContext context : primaryContexts) {
                Map<String, Object> normalizedPayload = UnifiedPayloadNormalizer.normalizePayload(context.payload());
                log.info("[MERGE DEBUG] merging primary context for type={} id={} payload={}",
                        context.recordType(), context.recordId(), normalizedPayload);
                mergeContext(payloadBySource, unifiedPayload, context);
            }
        }

        if (!CollectionUtils.isEmpty(relationPayloads)) {
            for (Map<String, Object> relationPayload : relationPayloads) {
                if (relationPayload == null) {
                    continue;
                }
                log.info("[MERGE DEBUG] merging relation payload={}", relationPayload);
                mergePayload(unifiedPayload, UnifiedPayloadNormalizer.normalizePayload(relationPayload));
            }
        }

        if (!CollectionUtils.isEmpty(relatedContexts)) {
            for (TransformService.RecordContext context : relatedContexts) {
                Map<String, Object> normalizedPayload = UnifiedPayloadNormalizer.normalizePayload(context.payload());
                log.info("[MERGE DEBUG] merging related context for type={} id={} payload={}",
                        context.recordType(), context.recordId(), normalizedPayload);
                mergeContext(payloadBySource, unifiedPayload, context);
            }
        }
    }

    private static void mergeContext(Map<Long, Map<String, Object>> payloadBySource,
                                     Map<String, Object> unifiedPayload,
                                     TransformService.RecordContext context) {
        if (context == null) {
            return;
        }
        Map<String, Object> normalizedPayload = UnifiedPayloadNormalizer.normalizePayload(context.payload());
        if (context.source() != null) {
            Map<String, Object> merged = new LinkedHashMap<>(payloadBySource.getOrDefault(context.source().getId(), Map.of()));
            mergePayload(merged, normalizedPayload);
            payloadBySource.put(context.source().getId(), merged);
        }
        mergePayload(unifiedPayload, normalizedPayload);
    }

    private static void mergePayload(Map<String, Object> target, Map<String, Object> source) {
        if (target == null || source == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            safePut(target, entry.getKey(), entry.getValue());
        }
    }

    private static void safePut(Map<String, Object> target, String key, Object newValue) {
        if (target == null || key == null) {
            return;
        }
        Object normalizedValue = TimestampNormalizer.normalizeValue(newValue);
        if (normalizedValue == null) {
            return;
        }
        Object existing = target.get(key);
        if (existing != null) {
            log.info("[MERGE DEBUG] safePut skip key={} existing={} newVal={}", key, existing, normalizedValue);
            return;
        }
        log.info("[MERGE DEBUG] safePut put key={} value={}", key, normalizedValue);
        target.put(key, normalizedValue);
    }
}
