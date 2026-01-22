package org.example.service.ingestion;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.example.models.entity.Source;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class CsvRecordExtractor implements RecordExtractor {

    @Override
    public boolean supports(String format) {
        return "csv".equalsIgnoreCase(format);
    }

    @Override
    public List<Map<String, Object>> extract(Source source, Map<String, Object> config) {
        Path path = resolvePath(config);
        String delimiter = stringValue(config.getOrDefault("delimiter", ","));
        Charset charset = Charset.forName(stringValue(config.getOrDefault("encoding", "UTF-8")));
        String tableLabel = resolveTableLabel(config, source);

        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(delimiter)
                .setSkipHeaderRecord(false)
                .setAllowMissingColumnNames(true)
                .build();

        try (Reader reader = Files.newBufferedReader(path, charset);
             CSVParser parser = new CSVParser(reader, format.withHeader())) {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (CSVRecord record : parser) {
                Map<String, Object> row = new LinkedHashMap<>();
                record.toMap().forEach(row::put);
                if (StringUtils.hasText(tableLabel)) {
                    row.putIfAbsent("__table__", tableLabel);
                }
                rows.add(row);
            }
            return rows;
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to read CSV source: " + exception.getMessage(), exception);
        }
    }

    private String resolveTableLabel(Map<String, Object> config, Source source) {
        String table = stringValue(config.get("table"));
        if (!StringUtils.hasText(table)) {
            table = stringValue(config.get("tableName"));
        }
        if (!StringUtils.hasText(table)) {
            table = source != null ? source.getName() : null;
        }
        return table;
    }

    private Path resolvePath(Map<String, Object> config) {
        String path = stringValue(config.get("filePath"));
        if (!StringUtils.hasText(path)) {
            path = stringValue(config.get("relativePath"));
        }
        if (!StringUtils.hasText(path)) {
            throw new IllegalStateException("CSV source missing filePath or relativePath");
        }
        return Path.of(path);
    }

    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }
}
