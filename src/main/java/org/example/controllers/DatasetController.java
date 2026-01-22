package org.example.controllers;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.models.dto.*;
import org.example.models.entity.Dataset;
import org.example.models.entity.DatasetField;
import org.example.models.entity.RawEvent;
import org.example.models.entity.TransformRun;
import org.example.models.entity.UnifiedRow;
import org.example.models.dto.MappingEditorDataResponse;
import org.example.models.dto.MappingEditorSaveRequest;
import org.example.models.dto.PipelineStatusResponse;
import org.example.models.entity.IngestionRun;
import org.example.models.enums.RunStatus;
import org.example.repository.RawEventRepository;
import org.example.repository.UnifiedRowRepository;
import org.example.service.DatasetService;
import org.example.service.TransformService;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.*;
import java.util.Comparator;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/datasets")
@RequiredArgsConstructor
public class DatasetController {

    private final DatasetService datasetService;
    private final UnifiedRowRepository unifiedRowRepository;
    private final RawEventRepository rawEventRepository;
    private final TransformService transformService;

    @GetMapping("/{datasetId}/mapping-editor/data")
    public MappingEditorDataResponse mappingEditorData(@PathVariable Long datasetId,
                                                       @RequestParam(defaultValue = "200") int limit,
                                                       Authentication authentication) {
        return datasetService.loadMappingEditorData(datasetId, requireUserEmail(authentication), limit);
    }

    @PostMapping("/{datasetId}/mapping-editor/save")
    public ResponseEntity<Map<String, Object>> saveMappingEditor(@PathVariable Long datasetId,
                                                                 @RequestBody MappingEditorSaveRequest request,
                                                                 Authentication authentication) {
        datasetService.saveMappingEditor(datasetId, request, requireUserEmail(authentication));
        return ResponseEntity.ok(Map.of(
                "status", "saved",
                "datasetId", datasetId
        ));
    }

    @GetMapping
    public List<DatasetSummaryDTO> listDatasets(Authentication authentication) {
        return datasetService.listAllForUser(requireUserEmail(authentication)).stream()
                .map(dataset -> {
                    long count = unifiedRowRepository.countByDataset(dataset);
                    Instant lastUpdated = unifiedRowRepository.findFirstByDatasetOrderByIngestedAtDesc(dataset)
                            .map(UnifiedRow::getIngestedAt)
                            .orElse(null);
                    String owner = dataset.getApplicationUser() != null ? dataset.getApplicationUser().getName() : null;
                    return new DatasetSummaryDTO(
                            dataset.getId(),
                            dataset.getName(),
                            dataset.getStatus(),
                            owner,
                            dataset.getDescription() != null ? dataset.getDescription() : "Managed dataset",
                            count,
                            lastUpdated
                    );
                })
                .collect(Collectors.toList());
    }

    @PostMapping
    public ResponseEntity<DatasetSummaryDTO> createDataset(@Valid @RequestBody CreateDatasetRequest request,
                                                           Authentication authentication) {
        Dataset dataset = datasetService.createDatasetForUser(request.name(), request.description(), request.primaryRecordType(), requireUserEmail(authentication));
        long count = unifiedRowRepository.countByDataset(dataset);
        Instant lastUpdated = unifiedRowRepository.findFirstByDatasetOrderByIngestedAtDesc(dataset)
                .map(UnifiedRow::getIngestedAt)
                .orElse(null);
        String owner = dataset.getApplicationUser() != null ? dataset.getApplicationUser().getName() : null;

        DatasetSummaryDTO summary = new DatasetSummaryDTO(
                dataset.getId(),
                dataset.getName(),
                dataset.getStatus(),
                owner,
                dataset.getDescription() != null ? dataset.getDescription() : "Managed dataset",
                count,
                lastUpdated
        );

        return ResponseEntity.status(HttpStatus.CREATED).body(summary);
    }

    @GetMapping("/{datasetId}")
    public Map<String, Object> getDataset(@PathVariable Long datasetId, Authentication authentication) {
        Dataset dataset = datasetService.getDatasetForUser(datasetId, requireUserEmail(authentication));
        boolean hasSources = datasetService.countSources(datasetId, requireUserEmail(authentication)) > 0;
        boolean hasRaw = rawEventRepository.countByDataset_Id(datasetId) > 0;
        boolean hasMappings = datasetService.countMappings(datasetId, requireUserEmail(authentication)) > 0;
        boolean hasUnified = unifiedRowRepository.countByDatasetAndIsExcludedFalse(dataset) > 0;
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", dataset.getId());
        response.put("name", dataset.getName());
        response.put("description", dataset.getDescription());
        response.put("primaryRecordType", dataset.getPrimaryRecordType());
        response.put("owner", dataset.getApplicationUser() != null ? dataset.getApplicationUser().getName() : null);
        response.put("hasSources", hasSources);
        response.put("hasRawData", hasRaw);
        response.put("hasMappings", hasMappings);
        response.put("hasUnifiedData", hasUnified);
        return response;
    }

    @GetMapping("/{datasetId}/pipeline")
    public PipelineStatusResponse pipelineStatus(@PathVariable Long datasetId, Authentication authentication) {
        return datasetService.getPipelineStatus(datasetId, requireUserEmail(authentication));
    }

    @PutMapping("/{datasetId}")
    public DatasetDetailDTO updateDataset(@PathVariable Long datasetId,
                                          @RequestBody UpdateDatasetRequest request,
                                          Authentication authentication) {
        return datasetService.updateDataset(datasetId, request, requireUserEmail(authentication));
    }

    @DeleteMapping("/{datasetId}")
    public ResponseEntity<Void> deleteDataset(@PathVariable Long datasetId, Authentication authentication) {
        datasetService.deleteDataset(datasetId, requireUserEmail(authentication));
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{datasetId}/ingest")
    public List<IngestionStatusDTO> ingestDataset(@PathVariable Long datasetId, Authentication authentication) {
        List<IngestionRun> runs = datasetService.ingestDataset(datasetId, requireUserEmail(authentication));
        return runs.stream()
                .map(run -> new IngestionStatusDTO(
                        run.getIngestionUid(),
                        run.getSource() != null ? run.getSource().getSourceUid() : null,
                        run.getRunStatus(),
                        run.getRowsRead() != null ? run.getRowsRead() : 0,
                        run.getRowsStored() != null ? run.getRowsStored() : 0,
                        run.getErrorMessage(),
                        run.getStartedAt(),
                        run.getEndedAt()
                ))
                .toList();
    }

    @GetMapping("/{datasetId}/fields")
    public List<DatasetFieldView> listFields(@PathVariable Long datasetId, Authentication authentication) {
        return datasetService.listFields(datasetId, requireUserEmail(authentication));
    }

    @PostMapping("/{datasetId}/fields")
    public DatasetFieldView addField(@PathVariable Long datasetId,
                                     @RequestBody DatasetFieldDTO request,
                                     Authentication authentication) {
        var field = datasetService.addField(datasetId, request, requireUserEmail(authentication));
        return new DatasetFieldView(
                field.getId(),
                field.getDatasetFieldUid(),
                field.getName(),
                field.getDtype() != null ? field.getDtype().name() : null,
                Boolean.TRUE.equals(field.getIsNullable()),
                Boolean.TRUE.equals(field.getIsUnique()),
                field.getPosition(),
                field.getDefaultExpr()
        );
    }

    @PutMapping("/{datasetId}/fields/{fieldId}")
    public DatasetFieldView updateField(@PathVariable Long datasetId,
                                        @PathVariable Long fieldId,
                                        @RequestBody DatasetFieldDTO request,
                                        Authentication authentication) {
        DatasetField updated = datasetService.updateField(datasetId, fieldId, request, requireUserEmail(authentication));
        return new DatasetFieldView(
                updated.getId(),
                updated.getDatasetFieldUid(),
                updated.getName(),
                updated.getDtype() != null ? updated.getDtype().name() : null,
                Boolean.TRUE.equals(updated.getIsNullable()),
                Boolean.TRUE.equals(updated.getIsUnique()),
                updated.getPosition(),
                updated.getDefaultExpr()
        );
    }

    @DeleteMapping("/{datasetId}/fields/{fieldId}")
    public ResponseEntity<Void> deleteField(@PathVariable Long datasetId,
                                            @PathVariable Long fieldId,
                                            Authentication authentication) {
        datasetService.deleteField(datasetId, fieldId, requireUserEmail(authentication));
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{datasetId}/mappings")
    public List<DatasetMappingView> listMappings(@PathVariable Long datasetId, Authentication authentication) {
        return datasetService.listMappings(datasetId, requireUserEmail(authentication));
    }

    @PostMapping("/{datasetId}/mappings")
    public DatasetMappingView addMapping(@PathVariable Long datasetId,
                                         @RequestBody AddMappingRequest request,
                                         Authentication authentication) {
        datasetService.addMapping(datasetId, request, requireUserEmail(authentication));
        return datasetService.listMappings(datasetId, requireUserEmail(authentication)).stream()
                .reduce((first, second) -> second)
                .orElse(null);
    }

    @PutMapping("/{datasetId}/mappings/{mappingId}")
    public DatasetMappingView updateMapping(@PathVariable Long datasetId,
                                            @PathVariable Long mappingId,
                                            @RequestBody AddMappingRequest request,
                                            Authentication authentication) {
        var dto = new DatasetMappingDTO(
                request.datasetFieldUid(),
                request.srcPath(),
                request.srcJsonPath(),
                request.transformType(),
                request.transformSql(),
                request.required()
        );
        datasetService.updateMapping(datasetId, mappingId, dto, requireUserEmail(authentication));
        return datasetService.listMappings(datasetId, requireUserEmail(authentication)).stream()
                .filter(m -> Objects.equals(m.id(), mappingId))
                .findFirst()
                .orElse(null);
    }

    @DeleteMapping("/{datasetId}/mappings/{mappingId}")
    public ResponseEntity<Void> deleteMapping(@PathVariable Long datasetId,
                                              @PathVariable Long mappingId,
                                              Authentication authentication) {
        datasetService.deleteMapping(datasetId, mappingId, requireUserEmail(authentication));
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{datasetId}/transform")
    public ResponseEntity<Map<String, Object>> triggerTransform(@PathVariable Long datasetId,
                                                                Authentication authentication) {
        Dataset dataset = datasetService.getDatasetForUser(datasetId, requireUserEmail(authentication));
        TransformRun run = transformService.startTransform(dataset.getId());

        Map<String, Object> response = Map.of(
                "status", run.getRunStatus(),
                "rowsIn", run.getRowsIn(),
                "rowsOut", run.getRowsOut(),
                "endedAt", run.getEndedAt(),
                "runUid", run.getTransformRunUid()
        );

        return run.getRunStatus() == RunStatus.SUCCESS ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    @PostMapping("/{datasetId}/ingest-transform")
    public ResponseEntity<Map<String, Object>> ingestAndTransform(@PathVariable Long datasetId,
                                                                  Authentication authentication) {
        TransformRun run = datasetService.ingestAndTransform(datasetId, requireUserEmail(authentication));
        Map<String, Object> response = Map.of(
                "status", run.getRunStatus(),
                "rowsIn", run.getRowsIn(),
                "rowsOut", run.getRowsOut(),
                "endedAt", run.getEndedAt(),
                "runUid", run.getTransformRunUid()
        );
        return run.getRunStatus() == RunStatus.SUCCESS ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
    }

    @GetMapping("/{datasetId}/raw/preview")
    public Map<String, Object> rawPreview(@PathVariable Long datasetId,
                                          @RequestParam(defaultValue = "100") int limit,
                                          Authentication authentication) {
        Dataset dataset = datasetService.getDatasetForUser(datasetId, requireUserEmail(authentication));
        List<RawEvent> events = rawEventRepository.findByDataset_Id(dataset.getId(), PageRequest.of(0, Math.max(limit, 1))).getContent();
        Map<String, List<Map<String, Object>>> byTable = new LinkedHashMap<>();
        for (RawEvent event : events) {
            Map<String, Object> payload = event.getPayload();
            if (payload == null) continue;
            String tableName = Optional.ofNullable(payload.get("__table__")).map(Object::toString).orElse("Unknown");
            Map<String, Object> sanitized = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : payload.entrySet()) {
                if (entry.getKey().startsWith("__") && !"__table__".equals(entry.getKey())) continue;
                sanitized.put(entry.getKey().equals("__table__") ? "Table" : entry.getKey(), entry.getValue());
            }
            sanitized.putIfAbsent("Table", tableName);
            byTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(sanitized);
        }
        List<Map<String, Object>> tables = new ArrayList<>();
        byTable.forEach((name, rows) -> {
            Set<String> cols = new LinkedHashSet<>();
            rows.forEach(r -> cols.addAll(r.keySet()));
            List<String> orderedCols = new ArrayList<>(cols);
            tables.add(Map.of(
                    "name", name,
                    "columns", orderedCols,
                    "rows", rows
            ));
        });
        return Map.of(
                "datasetName", dataset.getName(),
                "tables", tables
        );
    }

    @GetMapping("/{datasetId}/unified/preview")
    public Map<String, Object> unifiedPreview(@PathVariable Long datasetId,
                                              @RequestParam(defaultValue = "20") int limit,
                                              Authentication authentication) {
        Dataset dataset = datasetService.getDatasetForUser(datasetId, requireUserEmail(authentication));
        List<DatasetField> fields = datasetService.listFieldEntities(datasetId, requireUserEmail(authentication));
        List<UnifiedRow> rows = unifiedRowRepository.findByDatasetAndIsExcludedFalse(dataset, PageRequest.of(0, Math.max(limit, 1))).getContent();
        List<Map<String, Object>> fieldDtos = fields.stream()
                .sorted(Comparator.comparing(DatasetField::getPosition, Comparator.nullsLast(Integer::compareTo)))
                .map(f -> {
                    Map<String, Object> dto = new LinkedHashMap<>();
                    dto.put("id", f.getId());
                    dto.put("name", f.getName());
                    dto.put("dtype", f.getDtype() != null ? f.getDtype().name() : null);
                    dto.put("required", !Boolean.TRUE.equals(f.getIsNullable()));
                    dto.put("position", f.getPosition());
                    return dto;
                })
                .toList();
        List<Map<String, Object>> rowDtos = rows.stream().map(r -> {
            Map<String, Object> dto = new LinkedHashMap<>();
            dto.put("id", r.getId());
            dto.put("isExcluded", Boolean.TRUE.equals(r.getIsExcluded()));
            dto.put("values", Optional.ofNullable(r.getData()).orElse(Map.of()));
            return dto;
        }).toList();
        return Map.of(
                "fields", fieldDtos,
                "rows", rowDtos
        );
    }

    @PatchMapping("/{datasetId}/unified/{rowId}/exclude")
    public Map<String, Object> toggleRow(@PathVariable Long datasetId,
                                         @PathVariable Long rowId,
                                         @RequestBody Map<String, Object> body,
                                         Authentication authentication) {
        Dataset dataset = datasetService.getDatasetForUser(datasetId, requireUserEmail(authentication));
        UnifiedRow row = unifiedRowRepository.findById(rowId).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
        if (!row.getDataset().getId().equals(dataset.getId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Row not in dataset");
        }
        boolean excluded = Boolean.TRUE.equals(body.get("excluded"));
        row.setIsExcluded(excluded);
        unifiedRowRepository.save(row);
        return Map.of("id", row.getId(), "isExcluded", row.getIsExcluded());
    }

    @GetMapping("/{datasetId}/transform-runs/latest")
    public TransformRunStatusDTO latestTransform(@PathVariable Long datasetId, Authentication authentication) {
        return datasetService.latestTransformStatus(datasetId, requireUserEmail(authentication));
    }

    @PostMapping("/{datasetId}/export")
    public ExportResultDTO exportUnified(@PathVariable Long datasetId, Authentication authentication) {
        return datasetService.exportUnified(datasetId, requireUserEmail(authentication));
    }

    @GetMapping("/{datasetId}/export/download")
    public ResponseEntity<byte[]> downloadUnified(@PathVariable Long datasetId,
                                                  Authentication authentication) {
        String userEmail = requireUserEmail(authentication);
        return datasetService.exportUnifiedAsCsv(datasetId, userEmail);
    }

    private String requireUserEmail(Authentication authentication) {
        if (authentication == null || !authentication.isAuthenticated()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Authentication required");
        }
        return authentication.getName();
    }
}
