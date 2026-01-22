package org.example.service.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.example.models.entity.Relationship;
import org.example.models.entity.Source;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;

@Component
@RequiredArgsConstructor
public class RelationshipService {

    private final ObjectMapper objectMapper;

    public List<Relationship> derive(Source source, Map<String, Object> config, List<Map<String, Object>> records) {
        return deriveAcrossSources(Map.of(source, records));
    }

    /**
     * Derive relationships across *all* provided sources so that shared identifiers spanning
     * database and CSV records can generate edges in a single pass.
     */
    public List<Relationship> deriveAcrossSources(Map<Source, List<Map<String, Object>>> recordsBySource) {
        List<Relationship> relationships = new ArrayList<>();
        Map<String, List<RecordDescriptor>> index = new HashMap<>();
        int globalIndex = 0;

        for (Map.Entry<Source, List<Map<String, Object>>> entry : recordsBySource.entrySet()) {
            Source source = entry.getKey();
            List<Map<String, Object>> records = entry.getValue() == null ? List.of() : entry.getValue();
            for (int i = 0; i < records.size(); i++) {
                Map<String, Object> record = records.get(i);
                RecordDescriptor descriptor = buildDescriptor(source, record, globalIndex++);
                for (Map.Entry<String, Object> field : record.entrySet()) {
                    if (!isCandidate(field.getKey(), field.getValue())) {
                        continue;
                    }
                    String canonical = canonicalValue(field.getValue());
                    index.computeIfAbsent(field.getKey().toLowerCase(Locale.ROOT) + "::" + canonical, key -> new ArrayList<>())
                            .add(descriptor);
                }
            }
        }

        Set<String> seen = new HashSet<>();
        for (Map.Entry<String, List<RecordDescriptor>> entry : index.entrySet()) {
            if (entry.getValue().size() < 2) {
                continue;
            }
            String field = entry.getKey().split("::", 2)[0];
            String relationType = "shared_" + field;
            List<RecordDescriptor> descriptors = entry.getValue();
            for (int i = 0; i < descriptors.size(); i++) {
                for (int j = i + 1; j < descriptors.size(); j++) {
                    RecordDescriptor left = descriptors.get(i);
                    RecordDescriptor right = descriptors.get(j);
                    List<RecordDescriptor> ordered = Arrays.asList(left, right).stream()
                            .sorted(Comparator.comparing(RecordDescriptor::identity)
                                    .thenComparing(RecordDescriptor::recordType)
                                    .thenComparingInt(RecordDescriptor::index))
                            .toList();
                    RecordDescriptor from = ordered.get(0);
                    RecordDescriptor to = ordered.get(ordered.size() - 1);
                    String key = relationType + "|" + from.identity() + "|" + to.identity();
                    if (seen.contains(key)) {
                        continue;
                    }
                    seen.add(key);
                    Relationship relationship = new Relationship();
                    relationship.setSource(from.source());
                    relationship.setFromType(from.recordType());
                    relationship.setFromId(from.identity());
                    relationship.setToType(to.recordType());
                    relationship.setToId(to.identity());
                    relationship.setRelationType(relationType);
                    relationship.setPayload(buildPayload(field, descriptors));
                    relationship.setIngestedAt(Instant.now());
                    relationships.add(relationship);
                }
            }
        }
        return relationships;
    }

    private Map<String, Object> buildPayload(String field, List<RecordDescriptor> descriptors) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("field", field);
        payload.put("records", descriptors.stream().map(RecordDescriptor::identity).distinct().toList());
        return payload;
    }

    private RecordDescriptor buildDescriptor(Source source, Map<String, Object> record, int index) {
        return new RecordDescriptor(resolveIdentity(record), resolveRecordType(record), index, source);
    }

    private String resolveIdentity(Map<String, Object> record) {
        for (String key : List.of("id", "uid", "uuid", "guid", "identifier", "record_id", "global_id")) {
            Object value = record.get(key);
            if (value != null && !value.toString().isBlank()) {
                return value.toString();
            }
        }
        Object meta = record.get("__meta__");
        if (meta instanceof Map<?, ?> map) {
            for (String key : List.of("record_uid", "uid", "id")) {
                Object value = map.get(key);
                if (value != null && !value.toString().isBlank()) {
                    return value.toString();
                }
            }
        }
        return canonicalValue(record);
    }

    private String resolveRecordType(Map<String, Object> record) {
        Object meta = record.get("__meta__");
        if (meta instanceof Map<?, ?> map) {
            for (String key : List.of("destination_table", "wrapper_name", "record_type")) {
                Object value = map.get(key);
                if (value != null) {
                    return value.toString();
                }
            }
        }
        Object theme = record.get("__theme__");
        if (theme != null) {
            return theme.toString();
        }
        Object table = record.get("__table__");
        if (table != null) {
            return table.toString();
        }
        return "record";
    }

    private boolean isCandidate(String field, Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof String text && text.isBlank()) {
            return false;
        }
        String lowered = field.toLowerCase(Locale.ROOT);
        if (List.of("id", "uid", "name", "code", "number").contains(lowered)) {
            return true;
        }
        for (String suffix : List.of("_id", "_uid", "_name", "_code", "_number")) {
            if (lowered.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    private String canonicalValue(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return String.valueOf(value);
        }
    }

    private record RecordDescriptor(String identity, String recordType, int index, Source source) {
    }
}
