package org.example.service.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

@Component
@RequiredArgsConstructor
public class WrapperMappingService {

    private final ObjectMapper objectMapper;

    public Map<String, Object> applyMapping(Map<String, Object> record, Map<String, Object> config) {
        Map<String, Object> wrapperConfig = resolveWrapper(config);
        if (wrapperConfig == null) {
            return new LinkedHashMap<>(record);
        }

        Map<String, Object> transformed = new LinkedHashMap<>(record);
        Object attributeMappings = wrapperConfig.get("attribute_mappings");
        if (attributeMappings instanceof Map<?, ?> mapping) {
            for (Map.Entry<?, ?> entry : mapping.entrySet()) {
                String sourceAttr = String.valueOf(entry.getKey());
                if (!transformed.containsKey(sourceAttr)) {
                    continue;
                }
                Object target = entry.getValue();
                Object value = transformed.remove(sourceAttr);
                if (target == null) {
                    continue;
                }
                if (target instanceof String targetName) {
                    transformed.put(targetName, value);
                    continue;
                }
                if (target instanceof Map<?, ?> targetConfig) {
                    Object destination = targetConfig.containsKey("target")
                            ? targetConfig.get("target")
                            : sourceAttr;
                    String destinationField = destination == null ? sourceAttr : destination.toString();
                    Object transformedValue = applyTransform(value, targetConfig.get("transform"));
                    transformed.put(destinationField, transformedValue);
                    continue;
                }
                transformed.put(sourceAttr, value);
            }
        }

        Object theme = Optional.ofNullable(wrapperConfig.get("theme"))
                .orElse(wrapperConfig.get("thematic_aspect"));
        if (theme != null) {
            transformed.put("__theme__", theme);
        }

        Object themes = wrapperConfig.get("themes");
        if (themes instanceof Collection<?> collection) {
                List<String> normalized = collection.stream()
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .toList();
            if (!normalized.isEmpty()) {
                transformed.put("__themes__", normalized);
                transformed.putIfAbsent("__theme__", normalized.get(0));
            }
        }

        Map<String, Object> meta = new LinkedHashMap<>();
        Object existingMeta = transformed.get("__meta__");
        if (existingMeta instanceof Map<?, ?> map) {
            map.forEach((k, v) -> meta.put(String.valueOf(k), v));
        }

        Object wrapperName = Optional.ofNullable(wrapperConfig.get("name"))
                .orElse(wrapperConfig.get("wrapper_name"));
        if (wrapperName != null) {
            meta.put("wrapper_name", wrapperName.toString());
        }

        Object sourceType = Optional.ofNullable(config.get("source_type"))
                .orElse(config.get("format"));
        if (sourceType != null) {
            meta.put("source_type", sourceType.toString().toLowerCase());
        }

        Object schemaVersion = Optional.ofNullable(wrapperConfig.get("schema_version"))
                .orElse(wrapperConfig.get("schemaVersion"));
        if (schemaVersion == null) {
            schemaVersion = Optional.ofNullable(config.get("schema_version"))
                    .orElse(config.get("schemaVersion"));
        }
        if (schemaVersion != null) {
            meta.put("schema_version", schemaVersion);
        }

        Object destinationTable = Optional.ofNullable(wrapperConfig.get("destination_table"))
                .orElse(wrapperConfig.get("destinationTable"));
        if (destinationTable != null) {
            meta.put("destination_table", destinationTable);
        }

        transformed.put("__meta__", meta);
        String recordUid = computeFingerprint(transformed);
        meta.put("record_uid", recordUid);
        transformed.put("__meta__", meta);
        return transformed;
    }

    private Map<String, Object> resolveWrapper(Map<String, Object> config) {
        for (String key : List.of("wrapper", "wrapper_config", "wrapperMetadata", "wrapper_metadata")) {
            Object value = config.get(key);
            if (value instanceof Map<?, ?> map) {
                return new LinkedHashMap<>((Map<String, Object>) map);
            }
        }
        if (config.containsKey("theme") && config.containsKey("attribute_mappings")) {
            return new LinkedHashMap<>(config);
        }
        return null;
    }

    private Object applyTransform(Object value, Object transform) {
        if (transform == null) {
            return value;
        }
        if (transform instanceof String text) {
            String lowered = text.toLowerCase(Locale.ROOT);
            return switch (lowered) {
                case "lowercase" -> value != null ? value.toString().toLowerCase(Locale.ROOT) : null;
                case "uppercase" -> value != null ? value.toString().toUpperCase(Locale.ROOT) : null;
                case "titlecase" -> value != null ? toTitleCase(value.toString()) : null;
                case "strip" -> value != null ? value.toString().strip() : null;
                case "int" -> coerceInteger(value);
                case "float" -> coerceFloat(value);
                default -> value;
            };
        }
        return value;
    }

    private Object coerceInteger(Object value) {
        try {
            return value == null ? null : Integer.parseInt(value.toString());
        } catch (NumberFormatException ex) {
            return value;
        }
    }

    private Object coerceFloat(Object value) {
        try {
            return value == null ? null : Double.parseDouble(value.toString());
        } catch (NumberFormatException ex) {
            return value;
        }
    }

    private String toTitleCase(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }
        String[] parts = input.split(" ");
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].isEmpty()) {
                continue;
            }
            parts[i] = parts[i].substring(0, 1).toUpperCase(Locale.ROOT) + parts[i].substring(1).toLowerCase(Locale.ROOT);
        }
        return String.join(" ", parts);
    }

    private String computeFingerprint(Map<String, Object> record) {
        try {
            String canonical = objectMapper.writer().withDefaultPrettyPrinter().writeValueAsString(new TreeMap<>(record));
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder();
            for (byte b : hashed) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to compute record fingerprint", e);
        }
    }
}
