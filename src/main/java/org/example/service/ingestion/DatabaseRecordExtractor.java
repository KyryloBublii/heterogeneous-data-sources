package org.example.service.ingestion;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.models.entity.Source;
import org.example.utils.DatabaseConnector;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class DatabaseRecordExtractor implements RecordExtractor {

    private final DatabaseConnector databaseConnector;

    @Override
    public boolean supports(String format) {
        return "db".equalsIgnoreCase(format) || "database".equalsIgnoreCase(format);
    }

    @Override
    public List<Map<String, Object>> extract(Source source, Map<String, Object> config) {
        Map<String, Object> connection = resolveConnection(config);
        String jdbcUrl = resolveJdbcUrl(config, connection);
        String username = stringValue(connection.getOrDefault("username", connection.get("user")));
        String password = stringValue(connection.get("password"));

        if (!StringUtils.hasText(jdbcUrl)) {
            throw new IllegalStateException("Database ingestion requires jdbcUrl or connection parameters");
        }

        JdbcTemplate jdbcTemplate = databaseConnector.buildJdbcTemplate(jdbcUrl, username, password);

        Object tableConfigs = config.get("tables");
        if (shouldIngestAllTables(tableConfigs, config)) {
            return extractAllTables(jdbcTemplate, config, connection);
        }
        if (tableConfigs instanceof List<?> selections && !selections.isEmpty()) {
            return extractMultipleTables(jdbcTemplate, selections, config, connection);
        }

        String query = firstText(config.get("query"), config.get("sql"));
        if (!StringUtils.hasText(query)) {
            String table = resolveSingleTable(config, connection);
            if (!StringUtils.hasText(table)) {
                log.info("No explicit query or table provided; defaulting to ingest all tables");
                return extractAllTables(jdbcTemplate, config, connection);
            }
            query = buildSelect(table, config.get("columns"), config);
        }

        return executeQuery(jdbcTemplate, query, resolveTableLabel(config, connection));
    }

    private boolean shouldIngestAllTables(Object tableConfig, Map<String, Object> config) {
        if (booleanValue(config.get("useAllTables"))
                || booleanValue(config.get("loadAllTables"))
                || booleanValue(config.get("ingestAllTables"))
                || booleanValue(config.get("includeAllTables"))) {
            return true;
        }
        if (tableConfig instanceof String text) {
            String normalized = text.trim();
            return "*".equals(normalized) || "all".equalsIgnoreCase(normalized);
        }
        return false;
    }

    private Map<String, Object> resolveConnection(Map<String, Object> config) {
        Object maybeConnection = config.get("connection");
        Map<String, Object> connection = new LinkedHashMap<>();
        if (maybeConnection instanceof Map<?, ?> map) {
            map.forEach((k, v) -> connection.put(String.valueOf(k), v));
        }
        // allow top-level shorthands
        for (String key : List.of("jdbcUrl", "host", "port", "database", "dbname", "db", "username", "user", "password", "table", "tableName")) {
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

    private List<Map<String, Object>> extractMultipleTables(JdbcTemplate jdbcTemplate,
                                                            List<?> selections,
                                                            Map<String, Object> config,
                                                            Map<String, Object> connectionConfig) {
        List<Map<String, Object>> allRows = new ArrayList<>();
        for (Object selection : selections) {
            Map<String, Object> tableConfig = new LinkedHashMap<>();
            if (selection instanceof Map<?, ?> map) {
                map.forEach((k, v) -> tableConfig.put(String.valueOf(k), v));
            } else if (selection instanceof String text) {
                tableConfig.put("table", text);
            } else {
                throw new IllegalArgumentException("Table entry must be a table name or object with table/query");
            }

            String query = firstText(tableConfig.get("query"), tableConfig.get("sql"));
            String tableName = resolveSingleTable(tableConfig, connectionConfig);
            if (!StringUtils.hasText(query)) {
                if (!StringUtils.hasText(tableName)) {
                    throw new IllegalArgumentException("Table entry is missing table name and query");
                }
                String schema = stringValue(tableConfig.get("schema"));
                String qualifiedTable = StringUtils.hasText(schema) ? schema + "." + tableName : tableName;
                query = buildSelect(qualifiedTable, tableConfig.get("columns"), tableConfig);
            }

            String alias = stringValue(tableConfig.get("alias"));
            String tableLabel = StringUtils.hasText(alias) ? alias : (StringUtils.hasText(tableName) ? tableName : "query");
            allRows.addAll(executeQuery(jdbcTemplate, query, tableLabel));
        }
        return allRows;
    }

    private List<Map<String, Object>> extractAllTables(JdbcTemplate jdbcTemplate,
                                                       Map<String, Object> config,
                                                       Map<String, Object> connectionConfig) {
        DataSource dataSource = Objects.requireNonNull(jdbcTemplate.getDataSource(),
                "Database ingestion requires a DataSource");
        String schemaFilter = stringValue(firstText(config.get("schema"),
                config.get("defaultSchema"),
                connectionConfig.get("schema")));

        List<Map<String, Object>> collected = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet tables = metaData.getTables(connection.getCatalog(), schemaFilter, "%", new String[]{"TABLE", "VIEW"})) {
                while (tables.next()) {
                    String tableSchema = tables.getString("TABLE_SCHEM");
                    String tableName = tables.getString("TABLE_NAME");
                    if (!StringUtils.hasText(tableName) || isSystemSchema(tableSchema)) {
                        continue;
                    }
                    String qualified = qualifyTable(tableSchema, tableName);
                    String label = StringUtils.hasText(tableName) ? tableName : qualified;
                    collected.addAll(executeQuery(jdbcTemplate, "SELECT * FROM " + qualified, label));
                }
            }
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to enumerate database tables: " + exception.getMessage(), exception);
        }
        if (collected.isEmpty()) {
            log.warn("DatabaseRecordExtractor: no tables found when useAllTables flag enabled (schema filter: {})", schemaFilter);
        }
        return collected;
    }

    private String resolveSingleTable(Map<String, Object> config, Map<String, Object> fallback) {
        String table = stringValue(firstText(config.get("table"), config.get("tableName"), fallback.get("table"), fallback.get("tableName")));
        return StringUtils.hasText(table) ? table : stringValue(firstText(config.get("defaultTable"), fallback.get("defaultTable")));
    }

    private String buildSelect(String table, Object columnsSpec, Map<String, Object> context) {
        List<String> columns = normalizeColumns(columnsSpec);
        boolean selectAll = columns.isEmpty()
                || columns.contains("*")
                || booleanValue(context.get("selectAllColumns"))
                || booleanValue(context.get("includeAllColumns"))
                || booleanValue(context.get("allColumns"));

        String projection = selectAll ? "*" : String.join(", ", columns);
        return "SELECT " + projection + " FROM " + table;
    }

    private List<String> normalizeColumns(Object columnsSpec) {
        if (columnsSpec == null) {
            return List.of();
        }
        if (columnsSpec instanceof String text) {
            if (!StringUtils.hasText(text)) {
                return List.of();
            }
            if ("*".equals(text.trim())) {
                return List.of("*");
            }
            return List.of(text.split(","))
                    .stream()
                    .map(String::trim)
                    .filter(StringUtils::hasText)
                    .toList();
        }
        if (columnsSpec instanceof List<?> list) {
            return list.stream()
                    .map(this::stringValue)
                    .filter(StringUtils::hasText)
                    .map(String::trim)
                    .toList();
        }
        throw new IllegalArgumentException("columns must be a list of column names");
    }

    private List<Map<String, Object>> executeQuery(JdbcTemplate jdbcTemplate,
                                                   String query,
                                                   String tableLabel) {
        if (!StringUtils.hasText(query)) {
            throw new IllegalArgumentException("Query cannot be empty for database ingestion");
        }
        try {
            List<Map<String, Object>> results = jdbcTemplate.query(query, (resultSet, rowNum) -> {
                Map<String, Object> row = new LinkedHashMap<>();
                int columnCount = resultSet.getMetaData().getColumnCount();
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    String columnName = resultSet.getMetaData().getColumnLabel(columnIndex);
                    row.put(columnName, resultSet.getObject(columnIndex));
                }
                return row;
            });
            for (Map<String, Object> row : results) {
                if (StringUtils.hasText(tableLabel)) {
                    row.put("__table__", tableLabel);
                }
            }
            return results;
        } catch (Exception exception) {
            throw new IllegalStateException("Failed to execute database query: " + exception.getMessage(), exception);
        }
    }

    private String resolveTableLabel(Map<String, Object> config, Map<String, Object> connection) {
        String alias = stringValue(config.get("alias"));
        if (StringUtils.hasText(alias)) {
            return alias;
        }
        String table = resolveSingleTable(config, connection);
        return StringUtils.hasText(table) ? table : "table";
    }

    private boolean isSystemSchema(String schema) {
        if (!StringUtils.hasText(schema)) {
            return false;
        }
        String lower = schema.toLowerCase(Locale.ROOT);
        return lower.startsWith("pg_") || lower.startsWith("information_schema");
    }

    private String qualifyTable(String schema, String table) {
        if (StringUtils.hasText(schema)) {
            return schema + "." + table;
        }
        return table;
    }

    private boolean booleanValue(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof String text) {
            return Boolean.parseBoolean(text);
        }
        return false;
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
        return value == null ? null : value.toString();
    }
}
