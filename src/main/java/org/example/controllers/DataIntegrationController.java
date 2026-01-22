package org.example.controllers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.models.dto.IntegrationConfigDTO;
import org.example.models.entity.Dataset;
import org.example.models.entity.UnifiedRow;
import org.example.repository.UnifiedRowRepository;
import org.example.service.DataIntegrationService;
import org.example.service.DatasetService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class DataIntegrationController {

    private final DataIntegrationService integrationService;
    private final UnifiedRowRepository unifiedRowRepository;
    private final DatasetService datasetService;

    @PostMapping("/integrate")
    public ResponseEntity<Map<String, Object>> runIntegration(@RequestBody IntegrationConfigDTO config,
                                                              Authentication authentication) {
        log.info("Received integration request for dataset: {}", config.getDatasetId());

        try {
            Map<String, Object> result = integrationService.runIntegration(config, requireUserEmail(authentication));

            if ((Boolean) result.get("success")) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT).body(result);
            }

        } catch (Exception e) {
            log.error("Error during integration: {}", e.getMessage(), e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("error", e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping("/unified-data")
    public ResponseEntity<Map<String, Object>> getUnifiedData(
            @RequestParam(required = false) Long datasetId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size,
            Authentication authentication) {

        log.info("Fetching unified data - datasetId: {}, page: {}, size: {}", datasetId, page, size);

        try {
            String userEmail = requireUserEmail(authentication);
            Pageable pageable = PageRequest.of(page, size);
            Page<UnifiedRow> unifiedDataPage;

            if (datasetId != null) {
                Dataset dataset = datasetService.getDatasetForUser(datasetId, userEmail);

                unifiedDataPage = unifiedRowRepository.findByDataset(dataset, pageable);
            } else {
                List<Dataset> datasets = datasetService.listAllForUser(userEmail);
                if (datasets.isEmpty()) {
                    unifiedDataPage = new PageImpl<>(Collections.emptyList(), pageable, 0);
                } else {
                    unifiedDataPage = unifiedRowRepository.findByDatasetIn(datasets, pageable);
                }
            }

            Map<String, Object> response = new HashMap<>();
            response.put("data", unifiedDataPage.getContent());
            response.put("currentPage", unifiedDataPage.getNumber());
            response.put("totalPages", unifiedDataPage.getTotalPages());
            response.put("totalRecords", unifiedDataPage.getTotalElements());
            response.put("pageSize", unifiedDataPage.getSize());

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error fetching unified data: {}", e.getMessage(), e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping("/unified-data/count")
    public ResponseEntity<Map<String, Object>> getUnifiedDataCount(@RequestParam(required = false) Long datasetId,
                                                                   Authentication authentication) {
        log.info("Getting unified data count for dataset: {}", datasetId);

        try {
            String userEmail = requireUserEmail(authentication);
            long count;

            if (datasetId != null) {
                Dataset dataset = datasetService.getDatasetForUser(datasetId, userEmail);

                count = unifiedRowRepository.countByDataset(dataset);
            } else {
                List<Dataset> datasets = datasetService.listAllForUser(userEmail);
                count = datasets.isEmpty() ? 0 : unifiedRowRepository.countByDatasetIn(datasets);
            }

            Map<String, Object> response = new HashMap<>();
            response.put("count", count);
            response.put("datasetId", datasetId);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error counting unified data: {}", e.getMessage(), e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @DeleteMapping("/unified-data")
    public ResponseEntity<Map<String, Object>> clearUnifiedData(@RequestParam Long datasetId,
                                                                Authentication authentication) {
        log.info("Clearing unified data for dataset: {}", datasetId);

        try {
            Dataset dataset = datasetService.getDatasetForUser(datasetId, requireUserEmail(authentication));

            long countBefore = unifiedRowRepository.countByDataset(dataset);
            unifiedRowRepository.deleteByDataset(dataset);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("deletedRecords", countBefore);
            response.put("datasetId", datasetId);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            log.error("Error clearing unified data: {}", e.getMessage(), e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("error", e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    private String requireUserEmail(Authentication authentication) {
        if (authentication == null || !authentication.isAuthenticated()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Authentication required");
        }
        return authentication.getName();
    }
}

