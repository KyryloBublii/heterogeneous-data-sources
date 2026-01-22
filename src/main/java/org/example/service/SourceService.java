package org.example.service;

import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.models.dto.ColumnSchemaResponse;
import org.example.models.dto.SourceDTO;
import org.example.models.dto.TableSchemaResponse;
import org.example.models.dto.TableSelectionDTO;
import org.example.models.dto.ConnectionTestRequest;
import org.example.models.entity.ApplicationUser;
import org.example.models.entity.Dataset;
import org.example.models.entity.IngestionRun;
import org.example.models.entity.IntegrationConnection;
import org.example.models.entity.Source;
import org.example.models.enums.RunStatus;
import org.example.models.enums.SourceRole;
import org.example.models.enums.SourceStatus;
import org.example.models.enums.SourceType;
import org.example.repository.IntegrationConnectionRepository;
import org.example.repository.IngestionRunRepository;
import org.example.repository.DatasetRepository;
import org.example.repository.SourceRepository;
import org.example.repository.UserRepository;
import org.example.service.ingestion.IngestionService;
import org.example.utils.AppUtils;
import org.example.utils.DatabaseConnector;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class SourceService {

    private final SourceRepository sourceRepository;
    private final IngestionRunRepository ingestionRunRepository;
    private final IngestionService ingestionService;
    private final FileStorageService fileStorageService;
    private final DatabaseConnector databaseConnector;
    private final UserRepository userRepository;
    private final IntegrationConnectionRepository connectionRepository;
    private final DatasetRepository datasetRepository;

    public List<Source> listReusableSources(String userEmail) {
        ApplicationUser owner = requireUser(userEmail);
        return sourceRepository.findAllByApplicationUser_Email(owner.getEmail());
    }

    public List<Source> getAllSources(SourceRole role, Long datasetId, String userEmail) {
        ApplicationUser owner = requireUser(userEmail);
        if (datasetId != null) {
            if (role == null) {
                return sourceRepository.findAllByDataset_IdAndApplicationUser_Email(datasetId, owner.getEmail());
            }
            return sourceRepository.findAllByDataset_IdAndApplicationUser_EmailAndRole(datasetId, owner.getEmail(), role);
        }
        if (role == null) {
            return sourceRepository.findAllByApplicationUser_Email(owner.getEmail());
        }
        return sourceRepository.findAllByApplicationUser_EmailAndRole(owner.getEmail(), role);
    }

    public Source getSourceById(String id, String userEmail) {
        ApplicationUser owner = requireUser(userEmail);
        return sourceRepository.findBySourceUidAndApplicationUser_Email(id, owner.getEmail())
                .orElseThrow(() -> new RuntimeException("Source not found"));
    }

    @Transactional
    public Source createSource(SourceDTO dto, String userEmail) {
        if (!StringUtils.hasText(dto.name())) {
            throw new IllegalArgumentException("Source name is required");
        }
        if (dto.type() == null) {
            throw new IllegalArgumentException("Source type is required");
        }

        ApplicationUser owner = requireUser(userEmail);
        Dataset dataset = null;
        if (dto.datasetId() != null) {
            dataset = datasetRepository.findByIdAndApplicationUser_Email(dto.datasetId(), owner.getEmail())
                    .orElseThrow(() -> new IllegalArgumentException("Dataset not found: " + dto.datasetId()));
        }

        Source source = new Source();
        source.setSourceUid(AppUtils.generateUUID());
        source.setName(dto.name());
        source.setType(dto.type());
        source.setRole(dto.role() != null ? dto.role() : SourceRole.SOURCE);
        source.setStatus(SourceStatus.ACTIVE);
        source.setCreatedAt(Instant.now());
        source.setUpdatedAt(Instant.now());
        source.setApplicationUser(owner);
        source.setDataset(dataset);

        Map<String, Object> config = dto.config() != null ? new HashMap<>(dto.config()) : new HashMap<>();
        prepareConnectorConfig(source, config);
        source.setConfig(config);

        return sourceRepository.save(source);
    }

    @Transactional
    public Source updateSource(String id, SourceDTO dto, String userEmail) {
        Source source = getSourceById(id, userEmail);

        if (dto.name() != null) {
            source.setName(dto.name());
        }
        if (dto.type() != null) {
            source.setType(dto.type());
        }
        if (dto.role() != null) {
            source.setRole(dto.role());
        }

        if (dto.datasetId() != null) {
            Dataset dataset = datasetRepository.findByIdAndApplicationUser_Email(dto.datasetId(), requireUser(userEmail).getEmail())
                    .orElseThrow(() -> new IllegalArgumentException("Dataset not found: " + dto.datasetId()));
            source.setDataset(dataset);
        }

        Map<String, Object> config = dto.config() != null
                ? new HashMap<>(dto.config())
                : new HashMap<>(source.getConfig() != null ? source.getConfig() : Map.of());
        prepareConnectorConfig(source, config);
        source.setConfig(config);

        if (dto.status() != null) {
            source.setStatus(dto.status());
        }

        source.setUpdatedAt(Instant.now());

        return sourceRepository.save(source);
    }

    @Transactional
    public Source assignSourceToDataset(String sourceUid, Long datasetId, String userEmail) {
        Source source = getSourceById(sourceUid, userEmail);
        if (datasetId == null) {
            source.setDataset(null);
        } else {
            Dataset dataset = datasetRepository.findByIdAndApplicationUser_Email(datasetId, requireUser(userEmail).getEmail())
                    .orElseThrow(() -> new IllegalArgumentException("Dataset not found: " + datasetId));
            Dataset currentDataset = source.getDataset();
            if (currentDataset != null && !currentDataset.getId().equals(dataset.getId())) {
                return cloneSourceForDataset(source, dataset);
            }
            source.setDataset(dataset);
        }
        source.setUpdatedAt(Instant.now());
        return sourceRepository.save(source);
    }

    private Source cloneSourceForDataset(Source original, Dataset dataset) {
        Source copy = new Source();
        copy.setSourceUid(AppUtils.generateUUID());
        copy.setApplicationUser(original.getApplicationUser());
        copy.setDataset(dataset);
        copy.setName(original.getName());
        copy.setType(original.getType());
        copy.setRole(original.getRole());
        copy.setStatus(original.getStatus());
        copy.setConfig(original.getConfig() != null ? new HashMap<>(original.getConfig()) : Map.of());
        copy.setCreatedAt(Instant.now());
        copy.setUpdatedAt(Instant.now());
        return sourceRepository.save(copy);
    }

    @Transactional
    public void deleteSource(String id, String userEmail) {
        Source source = getSourceById(id, userEmail);
        sourceRepository.delete(source);
    }

    @Transactional
    public IngestionRun triggerIngestion(String sourceId, String userEmail) {
        Source source = getSourceById(sourceId, userEmail);
        return triggerIngestion(source, null, null);
    }


    @Transactional
    public IngestionRun triggerIngestion(Source source,
                                         Source destination,
                                         List<TableSelectionDTO> tableSelections) {
        if (source.getDataset() != null) {
            List<Source> datasetSources = sourceRepository.findAllByDataset_Id(source.getDataset().getId()).stream()
                    .filter(s -> s.getRole() == SourceRole.SOURCE)
                    .toList();
            if (datasetSources.size() > 1) {
                Map<Source, IngestionRun> runs = ingestionService.ingestDataset(source.getDataset().getId());
                return runs.getOrDefault(source, runs.values().stream().findFirst().orElse(null));
            }
        }

        List<TableSelectionDTO> effectiveSelections = tableSelections;
        if ((effectiveSelections == null || effectiveSelections.isEmpty())
                && source.getType() == SourceType.DB) {
            effectiveSelections = loadStoredSelections(source, destination);
        }

        IngestionRun run = new IngestionRun();
        run.setIngestionUid(AppUtils.generateUUID());
        run.setSource(source);
        run.setDestination(destination);
        run.setDataset(source.getDataset() != null ? source.getDataset() : destination != null ? destination.getDataset() : null);
        run.setRunStatus(RunStatus.QUEUED);
        run.setStartedAt(Instant.now());

        IngestionRun persisted = ingestionRunRepository.save(run);
        Map<String, Object> overrides = buildIngestionOverrides(effectiveSelections);
        if (overrides.isEmpty()) {
            ingestionService.startIngestionAsync(persisted.getId());
        } else {
            ingestionService.startIngestionAsync(persisted.getId(), overrides);
        }
        return persisted;
    }

    private List<TableSelectionDTO> loadStoredSelections(Source source, Source destination) {
        IntegrationConnection connection = null;
        if (destination != null && destination.getId() != null) {
            connection = connectionRepository
                    .findFirstBySource_IdAndDestination_IdOrderByCreatedAtDesc(source.getId(), destination.getId())
                    .orElse(null);
        }
        if (connection == null) {
            connection = connectionRepository
                    .findFirstBySource_IdOrderByCreatedAtDesc(source.getId())
                    .orElse(null);
        }
        if (connection == null || connection.getTableSelection() == null || connection.getTableSelection().isEmpty()) {
            return List.of();
        }

        List<TableSelectionDTO> selections = new ArrayList<>();
        for (Map<String, Object> entry : connection.getTableSelection()) {
            String tableName = stringValue(entry.get("tableName"));
            if (!StringUtils.hasText(tableName)) {
                continue;
            }
            String schema = stringValue(entry.get("schema"));
            List<String> columns = extractColumns(entry.get("columns"));
            selections.add(new TableSelectionDTO(tableName, schema, columns));
        }
        return selections;
    }

    private List<String> extractColumns(Object value) {
        if (value instanceof List<?> list) {
            return list.stream()
                    .map(this::stringValue)
                    .filter(StringUtils::hasText)
                    .map(String::trim)
                    .distinct()
                    .toList();
        }
        return List.of();
    }

    public List<IngestionRun> getSourceRuns(String sourceId, String userEmail) {
        Source source = getSourceById(sourceId, userEmail);
        return ingestionRunRepository.findBySourceOrderByStartedAtDesc(source);
    }


    public List<TableSchemaResponse> describeSourceSchema(String sourceId, String userEmail) {
        Source source = getSourceById(sourceId, userEmail);
        if (source.getType() != SourceType.DB) {
            return List.of(buildFileTableSchema(source));
        }

        Map<String, Object> config = source.getConfig();
        String jdbcUrl = stringValue(config.get("jdbcUrl"));
        String username = stringValue(config.get("username"));
        String password = stringValue(config.get("password"));

        if (!StringUtils.hasText(jdbcUrl)) {
            throw new IllegalStateException("Database source is missing jdbcUrl");
        }

        try (var connection = databaseConnector.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            String catalog = connection.getCatalog();
            List<TableSchemaResponse> tables = new ArrayList<>();
            try (ResultSet tableResult = metaData.getTables(catalog, null, "%", new String[]{"TABLE", "VIEW"})) {
                while (tableResult.next()) {
                    String schema = tableResult.getString("TABLE_SCHEM");
                    if (isSystemSchema(schema)) {
                        continue;
                    }
                    String tableName = tableResult.getString("TABLE_NAME");
                    List<ColumnSchemaResponse> columns = describeColumns(metaData, catalog, schema, tableName);
                    tables.add(new TableSchemaResponse(schema, tableName, columns));
                }
            }
            return tables;
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to inspect database schema: " + exception.getMessage(), exception);
        }
    }

    public List<TableSchemaResponse> discoverSchema(ConnectionTestRequest request, String userEmail) {
        requireUser(userEmail);

        if (request == null || request.type() != SourceType.DB) {
            throw new IllegalArgumentException("Schema discovery is only supported for database sources");
        }

        Map<String, Object> config = request.config() != null ? new HashMap<>(request.config()) : new HashMap<>();
        validateDatabaseConfig(config);
        config.put("jdbcUrl", buildJdbcUrl(config));

        String jdbcUrl = stringValue(config.get("jdbcUrl"));
        String username = stringValue(config.get("username"));
        String password = stringValue(config.get("password"));
        String requestedSchema = stringValue(config.get("schema"));

        try (var connection = databaseConnector.getConnection(jdbcUrl, username, password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            String catalog = connection.getCatalog();
            List<TableSchemaResponse> tables = new ArrayList<>();
            String schemaPattern = StringUtils.hasText(requestedSchema) ? requestedSchema : null;
            try (ResultSet tableResult = metaData.getTables(catalog, schemaPattern, "%", new String[]{"TABLE", "VIEW"})) {
                while (tableResult.next()) {
                    String schema = tableResult.getString("TABLE_SCHEM");
                    if (!StringUtils.hasText(requestedSchema) && isSystemSchema(schema)) {
                        continue;
                    }
                    String tableName = tableResult.getString("TABLE_NAME");
                    List<ColumnSchemaResponse> columns = describeColumns(metaData, catalog, schema, tableName);
                    tables.add(new TableSchemaResponse(schema, tableName, columns));
                }
            }
            return tables;
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to inspect database schema: " + exception.getMessage(), exception);
        }
    }

    private TableSchemaResponse buildFileTableSchema(Source source) {
        Map<String, Object> config = source.getConfig();
        String tableName = stringValue(config.get("displayFilename"));
        if (!StringUtils.hasText(tableName)) {
            tableName = stringValue(config.get("storedFilename"));
        }
        if (!StringUtils.hasText(tableName)) {
            tableName = source.getName();
        }
        if (!StringUtils.hasText(tableName)) {
            tableName = "uploaded-file";
        }

        List<ColumnSchemaResponse> columns = inferFileColumns(source);
        return new TableSchemaResponse(null, tableName, columns);
    }

    private List<ColumnSchemaResponse> inferFileColumns(Source source) {
        Map<String, Object> config = source.getConfig();
        if (config == null) {
            return List.of();
        }

        String configuredPath = stringValue(config.get("filePath"));
        String relativePath = stringValue(config.get("relativePath"));

        Path resolvedPath = null;
        try {
            if (StringUtils.hasText(configuredPath)) {
                resolvedPath = Paths.get(configuredPath).normalize();
            } else if (StringUtils.hasText(relativePath)) {
                resolvedPath = Paths.get(relativePath).normalize();
            }
        } catch (Exception exception) {
            log.warn("Invalid file path for source {}: {}", source.getSourceUid(), exception.getMessage());
            return List.of();
        }

        if (resolvedPath == null) {
            return List.of();
        }

        if (!resolvedPath.isAbsolute()) {
            resolvedPath = fileStorageService.getRootDirectory().resolve(resolvedPath).normalize();
        }

        if (!Files.exists(resolvedPath)) {
            log.warn("File not found for source {} at {}", source.getSourceUid(), resolvedPath);
            return List.of();
        }

        try {
        if (source.getType() == SourceType.CSV) {
            return parseCsvColumns(resolvedPath);
        }
        } catch (IOException exception) {
            log.warn("Failed to read file schema for source {}: {}", source.getSourceUid(), exception.getMessage());
        }

        return List.of();
    }

    private List<ColumnSchemaResponse> parseCsvColumns(Path path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String headerLine;
            while ((headerLine = reader.readLine()) != null) {
                headerLine = headerLine.trim();
                if (!headerLine.isEmpty()) {
                    break;
                }
            }

            if (headerLine == null || headerLine.isEmpty()) {
                return List.of();
            }

            headerLine = headerLine.replace("\uFEFF", "");
            String[] headers = headerLine.split(",");
            AtomicInteger counter = new AtomicInteger(1);
            List<ColumnSchemaResponse> columns = new ArrayList<>();
            for (String rawHeader : headers) {
                String cleaned = rawHeader.trim();
                if (cleaned.startsWith("\"") && cleaned.endsWith("\"")) {
                    cleaned = cleaned.substring(1, cleaned.length() - 1);
                }
                if (cleaned.isEmpty()) {
                    cleaned = "column_" + counter.getAndIncrement();
                }
                columns.add(new ColumnSchemaResponse(cleaned, "string", true));
            }
            return columns;
        }
    }

    public boolean testConnection(SourceType type, Map<String, Object> config) {
        if (type != SourceType.DB) {
            return true;
        }
        validateDatabaseConfig(config);
        String url = buildJdbcUrl(config);
        String username = stringValue(config.get("username"));
        String password = stringValue(config.get("password"));
        try (var ignored = databaseConnector.getConnection(url, username, password)) {
            return true;
        } catch (SQLException exception) {
            throw new IllegalStateException("Failed to connect to database: " + exception.getMessage(), exception);
        }
    }

    private Map<String, Object> buildIngestionOverrides(List<TableSelectionDTO> selections) {
        if (selections == null || selections.isEmpty()) {
            return Map.of();
        }
        List<Map<String, Object>> tables = new ArrayList<>();
        for (TableSelectionDTO selection : selections) {
            if (selection == null || !StringUtils.hasText(selection.tableName())) {
                continue;
            }
            Map<String, Object> tableSpec = new HashMap<>();
            tableSpec.put("table", selection.tableName());
            String schema = selection.schema();
            if (StringUtils.hasText(schema)) {
                tableSpec.put("schema", schema);
                tableSpec.put("alias", schema + "." + selection.tableName());
            } else {
                tableSpec.put("alias", selection.tableName());
            }
            if (selection.columns() != null && !selection.columns().isEmpty()) {
                tableSpec.put("columns", new ArrayList<>(selection.columns()));
            }
            tables.add(tableSpec);
        }
        if (tables.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("tables", tables);
        return overrides;
    }

    private List<ColumnSchemaResponse> describeColumns(DatabaseMetaData metaData,
                                                       String catalog,
                                                       String schema,
                                                       String tableName) throws SQLException {
        List<ColumnSchemaResponse> columns = new ArrayList<>();
        try (ResultSet columnResult = metaData.getColumns(catalog, schema, tableName, "%")) {
            while (columnResult.next()) {
                String columnName = columnResult.getString("COLUMN_NAME");
                String typeName = columnResult.getString("TYPE_NAME");
                boolean nullable = "YES".equalsIgnoreCase(columnResult.getString("IS_NULLABLE"));
                columns.add(new ColumnSchemaResponse(columnName, typeName, nullable));
            }
        }
        return columns;
    }

    private boolean isSystemSchema(String schema) {
        if (!StringUtils.hasText(schema)) {
            return false;
        }
        String normalized = schema.toLowerCase();
        return normalized.startsWith("pg_") || "information_schema".equals(normalized);
    }

    private void prepareConnectorConfig(Source source, Map<String, Object> config) {
        if (source.getType() == SourceType.DB) {
            validateDatabaseConfig(config);
            config.put("jdbcUrl", buildJdbcUrl(config));
        }

        if (source.getRole() == SourceRole.DESTINATION && source.getType() == SourceType.CSV) {
            String preferredName = stringValue(config.get("fileName"));
            FileStorageService.StoredFileMetadata metadata = fileStorageService.prepareDestinationFile(
                    source.getSourceUid(),
                    StringUtils.hasText(preferredName) ? preferredName : source.getName(),
                    source.getType());
            config.put("filePath", metadata.absolutePath().toString());
            config.put("relativePath", metadata.relativePath());
            config.put("storedFilename", metadata.storedFilename());
            config.put("displayFilename", metadata.displayFilename());
            config.put("extension", metadata.extension());
            config.put("fileName", metadata.displayFilename());
        }
    }

    private void validateDatabaseConfig(Map<String, Object> config) {
        String host = stringValue(config.get("host"));
        String database = stringValue(config.get("database"));
        String username = stringValue(config.get("username"));
        String password = stringValue(config.get("password"));
        int port = intValue(config.get("port"), 5432);

        if (!StringUtils.hasText(host) || !StringUtils.hasText(database)
                || !StringUtils.hasText(username) || !StringUtils.hasText(password)) {
            throw new IllegalArgumentException("Database configuration requires host, database, username, password");
        }

        config.put("host", host);
        config.put("database", database);
        config.put("username", username);
        config.put("password", password);
        config.put("port", port);
    }

    private String buildJdbcUrl(Map<String, Object> config) {
        String host = stringValue(config.get("host"));
        int port = intValue(config.get("port"), 5432);
        String database = stringValue(config.get("database"));
        return String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    }

    private String stringValue(Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value).trim();
    }

    private int intValue(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private ApplicationUser requireUser(String userEmail) {
        if (!StringUtils.hasText(userEmail)) {
            throw new IllegalArgumentException("User email is required");
        }
        return userRepository.findByEmail(userEmail)
                .orElseThrow(() -> new IllegalArgumentException("User not found: " + userEmail));
    }
}
