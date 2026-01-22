package org.example.service.ingestion;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.models.entity.Source;
import org.example.utils.DatabaseConnector;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class DatabaseDestinationWriter {

    private final DatabaseConnector databaseConnector;

    public int write(Source destination,
                     Map<String, Object> config,
                     List<Map<String, Object>> records) {
        if (records == null || records.isEmpty()) {
            return 0;
        }
        Map<String, Object> connection = resolveConnection(config);
        String jdbcUrl = resolveJdbcUrl(config, connection);
        String username = stringValue(connection.getOrDefault("username", connection.get("user")));
        String password = stringValue(connection.get("password"));

        if (!StringUtils.hasText(jdbcUrl)) {
            throw new IllegalStateException("DB destination requires jdbcUrl or connection parameters");
        }

        DataSource dataSource = databaseConnector.buildDataSource(jdbcUrl, username, password);
        TableReference tableReference = resolveTableReference(config, connection);
        Map<String, String> columnMapping = resolveColumnMapping(config);

        int totalWritten = 0;
        if (StringUtils.hasText(tableReference.table())) {
            totalWritten += writeToSingleTable(dataSource, tableReference, columnMapping, records);
        } else {
            Map<TableReference, List<Map<String, Object>>> grouped = groupRowsByTable(records, config, connection);
            if (grouped.isEmpty()) {
                throw new IllegalStateException("DB destination requires a table name or per-record destination_table metadata");
            }
            for (Map.Entry<TableReference, List<Map<String, Object>>> entry : grouped.entrySet()) {
                totalWritten += writeToSingleTable(dataSource, entry.getKey(), columnMapping, entry.getValue());
            }
        }
        return totalWritten;
    }

    private int writeToSingleTable(DataSource dataSource,
                                   TableReference tableReference,
                                   Map<String, String> columnMapping,
                                   List<Map<String, Object>> rowsForTable) {
        if (rowsForTable == null || rowsForTable.isEmpty()) {
            return 0;
        }
        if (!StringUtils.hasText(tableReference.table())) {
            return 0;
        }

        List<String> availableColumns = resolveDestinationColumns(dataSource, tableReference);
        if (availableColumns.isEmpty()) {
            throw new IllegalStateException("Unable to determine columns for destination table " + tableReference.qualified());
        }
        Map<String, String> destinationColumnLookup = buildColumnLookup(availableColumns);

        List<Map<String, Object>> normalizedRows = normalize(rowsForTable, columnMapping, destinationColumnLookup);
        if (normalizedRows.isEmpty()) {
            log.info("DestinationOutputService: no compatible columns for DB destination {}", tableReference.qualified());
            return 0;
        }

        List<String> orderedColumns = new ArrayList<>(collectColumnOrder(normalizedRows));
        for (Map<String, Object> row : normalizedRows) {
            for (String column : orderedColumns) {
                row.putIfAbsent(column, null);
            }
        }

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        insertRows(jdbcTemplate, tableReference.qualified(), orderedColumns, normalizedRows);
        log.info("DestinationOutputService: wrote {} rows to destination DB {}",
                normalizedRows.size(), tableReference.qualified());
        return normalizedRows.size();
    }

    private Map<String, Object> resolveConnection(Map<String, Object> config) {
        Object maybeConnection = config.get("connection");
        Map<String, Object> connection = new LinkedHashMap<>();
        if (maybeConnection instanceof Map<?, ?> map) {
            map.forEach((k, v) -> connection.put(String.valueOf(k), v));
        }
        for (String key : List.of("jdbcUrl", "url", "host", "port", "database", "dbname", "db", "username", "user",
                "password", "schema", "table", "tableName", "defaultTable")) {
            if (config.containsKey(key)) {
                connection.putIfAbsent(key, config.get(key));
            }
        }
        return connection;
    }

    private String resolveJdbcUrl(Map<String, Object> config, Map<String, Object> connection) {
        String jdbcUrl = stringValue(firstText(connection.get("jdbcUrl"), connection.get("url"), config.get("jdbcUrl")));
        if (StringUtils.hasText(jdbcUrl)) {
            return jdbcUrl;
        }
        String host = stringValue(connection.get("host"));
        String database = stringValue(firstText(connection.get("database"), connection.get("dbname"), connection.get("db")));
        if (!StringUtils.hasText(host) || !StringUtils.hasText(database)) {
            return null;
        }
        String port = stringValue(connection.get("port"));
        String resolvedPort = StringUtils.hasText(port) ? port : "5432";
        return "jdbc:postgresql://" + host + ":" + resolvedPort + "/" + database;
    }

    private TableReference resolveTableReference(Map<String, Object> config, Map<String, Object> connection) {
        String table = stringValue(firstText(config.get("table"), config.get("tableName"), config.get("destinationTable"),
                config.get("destination_table"), connection.get("table"), connection.get("tableName")));
        if (!StringUtils.hasText(table)) {
            table = stringValue(firstText(config.get("defaultTable"), connection.get("defaultTable")));
        }
        String schema = stringValue(firstText(config.get("schema"), config.get("defaultSchema"), connection.get("schema")));
        if (!StringUtils.hasText(schema) && StringUtils.hasText(table) && table.contains(".")) {
            String[] parts = table.split("\\.", 2);
            schema = parts[0];
            table = parts[1];
        }
        return new TableReference(schema, table);
    }

    private Map<TableReference, List<Map<String, Object>>> groupRowsByTable(List<Map<String, Object>> records,
                                                                           Map<String, Object> config,
                                                                           Map<String, Object> connection) {
        Map<TableReference, List<Map<String, Object>>> grouped = new LinkedHashMap<>();
        String tableFieldPath = stringValue(firstText(config.get("tableField"), config.get("table_field"),
                config.get("destinationTableField")));
        String schemaFieldPath = stringValue(firstText(config.get("schemaField"), config.get("schema_field"),
                config.get("destinationSchemaField")));
        String defaultSchema = stringValue(firstText(config.get("schema"), config.get("defaultSchema"), connection.get("schema")));

        for (Map<String, Object> record : records) {
            if (record == null || record.isEmpty()) {
                continue;
            }
            TableReference ref = resolveTableFromRecord(record, tableFieldPath, schemaFieldPath, defaultSchema);
            if (!StringUtils.hasText(ref.table())) {
                continue;
            }
            grouped.computeIfAbsent(ref, key -> new ArrayList<>()).add(record);
        }
        return grouped;
    }

    private TableReference resolveTableFromRecord(Map<String, Object> record,
                                                  String tableFieldPath,
                                                  String schemaFieldPath,
                                                  String defaultSchema) {
        Map<String, Object> meta = asMap(record.get("__meta__"));
        List<Object> tableCandidates = new ArrayList<>();
        if (StringUtils.hasText(tableFieldPath)) {
            tableCandidates.add(readField(record, tableFieldPath));
        }
        tableCandidates.add(record.get("destination_table"));
        tableCandidates.add(record.get("destinationTable"));
        tableCandidates.add(meta.get("destination_table"));
        tableCandidates.add(meta.get("destinationTable"));
        tableCandidates.add(record.get("__table__"));
        tableCandidates.add(record.get("table"));

        List<Object> schemaCandidates = new ArrayList<>();
        if (StringUtils.hasText(schemaFieldPath)) {
            schemaCandidates.add(readField(record, schemaFieldPath));
        }
        schemaCandidates.add(record.get("destination_schema"));
        schemaCandidates.add(meta.get("destination_schema"));
        schemaCandidates.add(record.get("schema"));
        schemaCandidates.add(meta.get("schema"));

        String table = firstText(tableCandidates.toArray());
        String schema = firstText(schemaCandidates.toArray());
        if (!StringUtils.hasText(table)) {
            return new TableReference(null, null);
        }
        if (table.contains(".")) {
            String[] parts = table.split("\\.", 2);
            schema = StringUtils.hasText(schema) ? schema : parts[0];
            table = parts[1];
        }
        if (!StringUtils.hasText(schema)) {
            schema = defaultSchema;
        }
        return new TableReference(schema, table);
    }

    private Map<String, Object> asMap(Object value) {
        Map<String, Object> result = new LinkedHashMap<>();
        if (value instanceof Map<?, ?> map) {
            map.forEach((k, v) -> result.put(String.valueOf(k), v));
        }
        return result;
    }

    private Object readField(Map<String, Object> source, String path) {
        if (!StringUtils.hasText(path)) {
            return null;
        }
        String[] parts = path.split("\\.");
        Object current = source;
        for (String part : parts) {
            if (!(current instanceof Map<?, ?> currentMap)) {
                return null;
            }
            if (!currentMap.containsKey(part)) {
                return null;
            }
            current = currentMap.get(part);
        }
        return current;
    }

    private List<String> resolveDestinationColumns(DataSource dataSource, TableReference tableReference) {
        List<String> columns = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet resultSet = metaData.getColumns(connection.getCatalog(), tableReference.schema(), tableReference.table(), null)) {
                while (resultSet.next()) {
                    columns.add(resultSet.getString("COLUMN_NAME"));
                }
            }
            if (columns.isEmpty() && StringUtils.hasText(tableReference.table())) {
                for (String candidate : alternateTableNames(tableReference.table())) {
                    try (ResultSet resultSet = metaData.getColumns(connection.getCatalog(), tableReference.schema(), candidate, null)) {
                        while (resultSet.next()) {
                            columns.add(resultSet.getString("COLUMN_NAME"));
                        }
                    }
                    if (!columns.isEmpty()) {
                        break;
                    }
                }
            }
            if (!columns.isEmpty()) {
                return columns;
            }
            try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + tableReference.qualified() + " WHERE 1=0");
                 ResultSet rs = statement.executeQuery()) {
                ResultSetMetaData metaDataRs = rs.getMetaData();
                for (int i = 1; i <= metaDataRs.getColumnCount(); i++) {
                    columns.add(metaDataRs.getColumnLabel(i));
                }
            }
            return columns;
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to inspect destination table metadata: " + exception.getMessage(), exception);
        }
    }

    private List<String> alternateTableNames(String table) {
        List<String> alternates = new ArrayList<>();
        String upper = table.toUpperCase(Locale.ROOT);
        String lower = table.toLowerCase(Locale.ROOT);
        if (!upper.equals(table)) {
            alternates.add(upper);
        }
        if (!lower.equals(table)) {
            alternates.add(lower);
        }
        return alternates;
    }

    private Map<String, String> buildColumnLookup(List<String> columns) {
        Map<String, String> lookup = new LinkedHashMap<>();
        for (String column : columns) {
            if (column != null) {
                lookup.put(column.toLowerCase(Locale.ROOT), column);
            }
        }
        return lookup;
    }

    private Map<String, String> resolveColumnMapping(Map<String, Object> config) {
        for (String key : List.of("columnMapping", "column_mapping", "columnMap", "columnMappings")) {
            Object mapping = config.get(key);
            if (mapping instanceof Map<?, ?> map) {
                Map<String, String> resolved = new LinkedHashMap<>();
                map.forEach((k, v) -> resolved.put(String.valueOf(k), stringValue(v)));
                return resolved;
            }
        }
        return Map.of();
    }

    private List<Map<String, Object>> normalize(List<Map<String, Object>> records,
                                                Map<String, String> columnMapping,
                                                Map<String, String> destinationLookup) {
        List<Map<String, Object>> normalized = new ArrayList<>();
        for (Map<String, Object> record : records) {
            if (record == null || record.isEmpty()) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : record.entrySet()) {
                String sourceKey = entry.getKey();
                if (!StringUtils.hasText(sourceKey)) {
                    continue;
                }
                String desiredColumn = columnMapping.containsKey(sourceKey)
                        ? columnMapping.get(sourceKey)
                        : sourceKey;
                if (!StringUtils.hasText(desiredColumn)) {
                    continue;
                }
                String resolvedColumn = destinationLookup.get(desiredColumn.toLowerCase(Locale.ROOT));
                if (resolvedColumn != null) {
                    row.put(resolvedColumn, entry.getValue());
                }
            }
            if (!row.isEmpty()) {
                normalized.add(row);
            }
        }
        return normalized;
    }

    private Set<String> collectColumnOrder(List<Map<String, Object>> rows) {
        Set<String> ordered = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            ordered.addAll(row.keySet());
        }
        return ordered;
    }

    private void insertRows(JdbcTemplate jdbcTemplate,
                             String table,
                             List<String> columns,
                             List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            return;
        }
        String columnList = String.join(", ", columns);
        String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + table + " (" + columnList + ") VALUES (" + placeholders + ")";
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                Map<String, Object> row = rows.get(i);
                for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                    String column = columns.get(columnIndex);
                    ps.setObject(columnIndex + 1, row.get(column));
                }
            }

            @Override
            public int getBatchSize() {
                return rows.size();
            }
        });
    }

    private String firstText(Object... values) {
        for (Object value : values) {
            String text = stringValue(value);
            if (StringUtils.hasText(text)) {
                return text;
            }
        }
        return null;
    }

    private String stringValue(Object value) {
        return Objects.toString(value, null);
    }

    private record TableReference(String schema, String table) {
        String qualified() {
            if (StringUtils.hasText(schema)) {
                return schema + "." + table;
            }
            return table;
        }
    }
}
