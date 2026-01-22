package org.example.service;

import jakarta.transaction.Transactional;
import org.example.models.dto.ConnectionRequest;
import org.example.models.dto.ConnectionResponse;
import org.example.models.dto.TableSelectionDTO;
import org.example.models.dto.TableSelectionUpdateRequest;
import org.example.models.entity.ApplicationUser;
import org.example.models.entity.Dataset;
import org.example.models.entity.IntegrationConnection;
import org.example.models.entity.Source;
import org.example.models.enums.SourceType;
import org.example.repository.IntegrationConnectionRepository;
import org.example.repository.DatasetRepository;
import org.example.repository.UserRepository;
import org.example.utils.AppUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class ConnectionService {

    private final IntegrationConnectionRepository connectionRepository;
    private final SourceService sourceService;
    private final UserRepository userRepository;
    private final DatasetRepository datasetRepository;

    public ConnectionService(IntegrationConnectionRepository connectionRepository,
                             SourceService sourceService,
                             UserRepository userRepository,
                             DatasetRepository datasetRepository) {
        this.connectionRepository = connectionRepository;
        this.sourceService = sourceService;
        this.userRepository = userRepository;
        this.datasetRepository = datasetRepository;
    }

    @Transactional
    public ConnectionResponse createConnection(ConnectionRequest request, String userEmail, String createdBy) {
        Dataset dataset = null;
        if (request.datasetId() != null) {
            dataset = datasetRepository.findByIdAndApplicationUser_Email(request.datasetId(), userEmail)
                    .orElseThrow(() -> new IllegalArgumentException("Dataset not found: " + request.datasetId()));
        }

        Source source = sourceService.getSourceById(request.sourceId(), userEmail);
        Source destination = null;
        if (StringUtils.hasText(request.destinationId())) {
            destination = sourceService.getSourceById(request.destinationId(), userEmail);
        }
        if (dataset == null) {
            dataset = source.getDataset();
        }
        if (dataset == null) {
            throw new IllegalArgumentException("Connection requires a dataset");
        }
        if (source.getDataset() != null && !dataset.getId().equals(source.getDataset().getId())) {
            throw new IllegalArgumentException("Source does not belong to the dataset");
        }
        if (destination != null && destination.getDataset() != null && !dataset.getId().equals(destination.getDataset().getId())) {
            throw new IllegalArgumentException("Destination does not belong to the dataset");
        }

        List<TableSelectionDTO> normalizedSelections = normalizeSelections(request.tableSelections());

        IntegrationConnection connection = new IntegrationConnection();
        connection.setConnectionUid(AppUtils.generateUUID());
        connection.setDataset(dataset);
        connection.setSource(source);
        connection.setDestination(destination);
        connection.setRelation(StringUtils.hasText(request.relation()) ? request.relation() : "LOAD");
        connection.setCreatedBy(StringUtils.hasText(createdBy) ? createdBy : "system");
        connection.setCreatedAt(Instant.now());
        connection.setTableSelection(serializeSelections(normalizedSelections));

        IntegrationConnection saved = connectionRepository.save(connection);

        boolean hasExplicitSelection = normalizedSelections != null && !normalizedSelections.isEmpty();
        boolean skipImmediateIngestion = SourceType.DB.equals(source.getType()) && !hasExplicitSelection;

        if (!skipImmediateIngestion) {
            sourceService.triggerIngestion(source, destination, normalizedSelections);
        }

        return toResponse(saved);
    }

    @Transactional
    public ConnectionResponse updateTableSelection(String connectionUid, TableSelectionUpdateRequest request, String userEmail) {
        IntegrationConnection connection = connectionRepository
                .findByConnectionUidAndSource_ApplicationUser_Email(connectionUid, userEmail);
        if (connection == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionUid);
        }
        List<TableSelectionDTO> normalizedSelections = normalizeSelections(request.tableSelections());
        connection.setTableSelection(serializeSelections(normalizedSelections));
        IntegrationConnection saved = connectionRepository.save(connection);

        Source source = saved.getSource();
        Source destination = saved.getDestination();
        sourceService.triggerIngestion(source, destination, normalizedSelections);

        return toResponse(saved);
    }

    public List<ConnectionResponse> listConnections(String userEmail, Long datasetId) {
        List<IntegrationConnection> connections;
        if (datasetId != null) {
            connections = connectionRepository.findAllByDataset_IdAndSource_ApplicationUser_Email(datasetId, userEmail);
        } else {
            connections = connectionRepository.findAllBySource_ApplicationUser_Email(userEmail);
        }
        return connections.stream()
                .map(this::toResponse)
                .collect(Collectors.toList());
    }

    private ConnectionResponse toResponse(IntegrationConnection connection) {
        Source source = connection.getSource();
        Source destination = connection.getDestination();
        return new ConnectionResponse(
                connection.getConnectionUid(),
                source != null ? source.getSourceUid() : null,
                source != null ? source.getName() : null,
                destination != null ? destination.getSourceUid() : null,
                destination != null ? destination.getName() : null,
                connection.getRelation(),
                extractSelections(connection),
                connection.getCreatedBy(),
                connection.getCreatedAt(),
                connection.getDataset() != null ? connection.getDataset().getId() : null
        );
    }

    private List<TableSelectionDTO> normalizeSelections(List<TableSelectionDTO> selections) {
        if (selections == null || selections.isEmpty()) {
            return List.of();
        }
        List<TableSelectionDTO> normalized = new ArrayList<>();
        for (TableSelectionDTO selection : selections) {
            if (selection == null || !StringUtils.hasText(selection.tableName())) {
                continue;
            }
            String tableName = selection.tableName().trim();
            String schema = StringUtils.hasText(selection.schema()) ? selection.schema().trim() : null;
            List<String> columns = selection.columns() == null ? List.of() : selection.columns().stream()
                    .filter(StringUtils::hasText)
                    .map(String::trim)
                    .distinct()
                    .collect(Collectors.toList());
            normalized.add(new TableSelectionDTO(tableName, schema, columns));
        }
        return normalized;
    }

    private List<Map<String, Object>> serializeSelections(List<TableSelectionDTO> selections) {
        if (selections == null || selections.isEmpty()) {
            return null;
        }
        List<Map<String, Object>> serialized = new ArrayList<>();
        for (TableSelectionDTO selection : selections) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("tableName", selection.tableName());
            if (StringUtils.hasText(selection.schema())) {
                payload.put("schema", selection.schema());
            }
            if (selection.columns() != null && !selection.columns().isEmpty()) {
                payload.put("columns", new ArrayList<>(selection.columns()));
            }
            serialized.add(payload);
        }
        return serialized;
    }

    private List<TableSelectionDTO> extractSelections(IntegrationConnection connection) {
        List<Map<String, Object>> stored = connection.getTableSelection();
        if (stored == null || stored.isEmpty()) {
            return List.of();
        }
        List<TableSelectionDTO> selections = new ArrayList<>();
        for (Map<String, Object> entry : stored) {
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
                    .collect(Collectors.toList());
        }
        return List.of();
    }

    private String stringValue(Object value) {
        return value == null ? null : Objects.toString(value, null);
    }

    private ApplicationUser requireUser(String userEmail) {
        if (!StringUtils.hasText(userEmail)) {
            throw new IllegalArgumentException("User email is required");
        }
        return userRepository.findByEmail(userEmail)
                .orElseThrow(() -> new IllegalArgumentException("User not found: " + userEmail));
    }
}
