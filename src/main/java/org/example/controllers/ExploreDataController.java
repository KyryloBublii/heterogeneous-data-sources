package org.example.controllers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.models.entity.Dataset;
import org.example.models.entity.UnifiedRow;
import org.example.service.DatasetService;
import org.example.repository.UnifiedRowRepository;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/data")
@RequiredArgsConstructor
public class ExploreDataController {

    private static final int MAX_RESULTS = 500;

    private final DatasetService datasetService;
    private final UnifiedRowRepository unifiedRowRepository;

    @GetMapping("/{datasetId}")
    public List<Map<String, Object>> loadDataset(@PathVariable Long datasetId,
                                                 @RequestParam(value = "filter", required = false) String filter,
                                                 Authentication authentication) {
        Dataset dataset = datasetService.getDatasetForUser(datasetId, requireUserEmail(authentication));
        List<UnifiedRow> rows = unifiedRowRepository.findByDataset(dataset);

        return applyFilter(rows, filter).stream()
                .limit(MAX_RESULTS)
                .map(UnifiedRow::getData)
                .collect(Collectors.toList());
    }

    @GetMapping("/export")
    public ResponseEntity<byte[]> exportDataset(@RequestParam("dataset") Long datasetId,
                                                @RequestParam(value = "filter", required = false) String filter,
                                                Authentication authentication) {
        Dataset dataset = datasetService.getDatasetForUser(datasetId, requireUserEmail(authentication));
        List<UnifiedRow> rows = unifiedRowRepository.findByDataset(dataset);
        List<UnifiedRow> filtered = applyFilter(rows, filter);

        return exportCsv(dataset.getName(), filtered);
    }

    private List<UnifiedRow> applyFilter(List<UnifiedRow> rows, String filter) {
        if (filter == null || filter.isBlank()) {
            return rows;
        }
        int equalsIndex = filter.indexOf('=');
        if (equalsIndex <= 0) {
            return rows;
        }
        String field = filter.substring(0, equalsIndex);
        String value = filter.substring(equalsIndex + 1);
        return rows.stream()
                .filter(row -> {
                    Object fieldValue = row.getData().get(field);
                    return fieldValue != null && fieldValue.toString().equalsIgnoreCase(value);
                })
                .collect(Collectors.toList());
    }

    private ResponseEntity<byte[]> exportCsv(String datasetName, List<UnifiedRow> rows) {
        if (rows.isEmpty()) {
            return buildDownload(datasetName, "csv", new byte[0], MediaType.TEXT_PLAIN);
        }

        Set<String> headers = new LinkedHashSet<>();
        rows.stream().map(UnifiedRow::getData).forEach(map -> headers.addAll(map.keySet()));

        StringBuilder builder = new StringBuilder();
        builder.append(String.join(",", headers)).append('\n');

        for (UnifiedRow row : rows) {
            Map<String, Object> data = row.getData();
            List<String> values = new ArrayList<>();
            for (String header : headers) {
                Object value = data.get(header);
                String sanitized = value == null ? "" : value.toString().replace("\"", "\"\"");
                if (sanitized.contains(",")) {
                    sanitized = '"' + sanitized + '"';
                }
                values.add(sanitized);
            }
            builder.append(String.join(",", values)).append('\n');
        }

        byte[] payload = builder.toString().getBytes(StandardCharsets.UTF_8);
        return buildDownload(datasetName, "csv", payload, MediaType.TEXT_PLAIN);
    }

    private ResponseEntity<byte[]> buildDownload(String datasetName, String extension, byte[] payload, MediaType mediaType) {
        String filename = datasetName + "-" + DateTimeFormatter.ISO_INSTANT.format(java.time.Instant.now()) + "." + extension;
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename)
                .header(HttpHeaders.CONTENT_TYPE, mediaType.toString())
                .body(payload);
    }

    private String requireUserEmail(Authentication authentication) {
        if (authentication == null || !authentication.isAuthenticated()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Authentication required");
        }
        return authentication.getName();
    }
}
