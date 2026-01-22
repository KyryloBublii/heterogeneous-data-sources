package org.example.service.transform;

import org.example.models.entity.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class GraphBuilder {

    private static final Logger log = LoggerFactory.getLogger(GraphBuilder.class);

    private GraphBuilder() {
    }

    public static Graph build(List<Relationship> relationships) {
        Map<NodeRef, List<Edge>> adjacency = new LinkedHashMap<>();
        if (relationships == null) {
            return new Graph(adjacency);
        }

        for (Relationship relation : relationships) {
            log.info("[REL DEBUG] raw rel: fromType={}, fromId={}, toType={}, toId={}",
                    relation.getFromType(), relation.getFromId(), relation.getToType(), relation.getToId());
            NodeRef from = nodeFrom(relation.getFromType(), relation.getFromId());
            NodeRef to = nodeFrom(relation.getToType(), relation.getToId());
            log.info("[REL DEBUG] normalized: from={}, to={}", from, to);
            if (from == null || to == null) {
                continue;
            }
            Edge forward = new Edge(to, relation);
            Edge backward = new Edge(from, relation);
            adjacency.computeIfAbsent(from, key -> new ArrayList<>()).add(forward);
            adjacency.computeIfAbsent(to, key -> new ArrayList<>()).add(backward);
        }
        return new Graph(adjacency);
    }

    private static NodeRef nodeFrom(String type, String id) {
        if (!StringUtils.hasText(type) || !StringUtils.hasText(id)) {
            return null;
        }
        String normalizedType = normalizeType(type);
        String normalizedId = normalizeId(id);

        if (!StringUtils.hasText(normalizedType) || !StringUtils.hasText(normalizedId)) {
            return null;
        }
        return new NodeRef(normalizedType, normalizedId);
    }

    private static String normalizeType(String rawType) {
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

    private static String normalizeId(String raw) {
        if (!StringUtils.hasText(raw)) {
            return null;
        }

        String trimmed = raw.trim();

        // Some relationship rows accidentally persist the full JSON payload as the id
        // (e.g., "{customer_id:CUST-100,first_name:Olivia,...}"). To align with
        // TransformService contexts (which use the scalar id like "CUST-100"), try to
        // extract a likely identifier token from the map-like string.
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

                // Prefer keys that look like identifiers
                if (key.endsWith("id") || key.endsWith("_id") || key.contains("id")) {
                    return val;
                }
            }
        }

        return trimmed.replace("\"", "").replace("`", "");
    }

    public record NodeRef(String type, String id) {
        public NodeRef {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(id, "id");
        }
    }

    public record Edge(NodeRef target, Relationship relation) {
    }

    public record Graph(Map<NodeRef, List<Edge>> adjacency) {
        public List<Edge> neighbors(NodeRef node) {
            return adjacency.getOrDefault(node, List.of());
        }

        public Set<NodeRef> nodes() {
            return adjacency.keySet();
        }
    }
}
