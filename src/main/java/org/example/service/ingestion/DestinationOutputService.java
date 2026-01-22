package org.example.service.ingestion;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.models.entity.Source;
import org.example.models.enums.SourceType;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class DestinationOutputService {

    private final DatabaseDestinationWriter databaseDestinationWriter;

    public void write(Source destination, List<Map<String, Object>> records) {
        if (destination == null || destination.getConfig() == null) {
            return;
        }

        SourceType type = destination.getType();
        Map<String, Object> config = destination.getConfig();

        if (type == SourceType.DB) {
            log.info("DestinationOutputService: writing unified rows to DB destination {}", destination.getName());
            databaseDestinationWriter.write(destination, config, records);
            return;
        }

        String defaultPath = stringValue(config.get("filePath"));
        String csvPath = stringValue(config.getOrDefault("csvFilePath", defaultPath));

        boolean wrote = false;
        if (type == SourceType.CSV && StringUtils.hasText(csvPath)) {
            writeCsv(Path.of(csvPath), records);
            wrote = true;
        } else {
            log.warn("Destination type {} not supported for file output", type);
        }

        if (!wrote) {
            log.warn("Destination file path missing or invalid, skipping output generation");
        }
    }

    private void writeCsv(Path path, List<Map<String, Object>> records) {
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            try (Writer writer = Files.newBufferedWriter(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                Set<String> headers = collectHeaders(records);
                writer.write(String.join(",", headers));
                writer.write("\n");

                for (Map<String, Object> record : records) {
                    boolean first = true;
                    for (String header : headers) {
                        if (!first) {
                            writer.write(',');
                        }
                        Object value = record.get(header);
                        writer.write(escapeCsv(value));
                        first = false;
                    }
                    writer.write("\n");
                }
            }
        } catch (IOException ioException) {
            throw new IllegalStateException("Failed to write CSV destination output", ioException);
        }
    }

    private Set<String> collectHeaders(List<Map<String, Object>> records) {
        Set<String> headers = new LinkedHashSet<>();
        for (Map<String, Object> record : records) {
            headers.addAll(record.keySet());
        }
        return headers;
    }

    private String escapeCsv(Object value) {
        if (value == null) {
            return "";
        }
        String text = value.toString();
        boolean needsQuotes = text.contains(",") || text.contains("\"") || text.contains("\n") || text.contains("\r");
        String escaped = text.replace("\"", "\"\"");
        return needsQuotes ? "\"" + escaped + "\"" : escaped;
    }

    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }
}
