package org.example.controllers;

import org.example.models.dto.ConnectionTestRequest;
import org.example.models.dto.SourceDTO;
import org.example.models.dto.TableSchemaResponse;
import org.example.models.entity.IngestionRun;
import org.example.models.entity.Source;
import org.example.models.enums.SourceRole;
import org.example.models.enums.SourceType;
import org.example.service.FileStorageService;
import org.example.service.SourceService;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/sources")
@CrossOrigin(origins = "*")
public class SourceController {
    private final SourceService sourceService;
    private final FileStorageService fileStorageService;

    public SourceController(SourceService sourceService, FileStorageService fileStorageService) {
        this.sourceService = sourceService;
        this.fileStorageService = fileStorageService;
    }

    @PostMapping
    public ResponseEntity<SourceDTO> addSource(@RequestBody SourceDTO dto, Authentication authentication) {
        Source source = sourceService.createSource(dto, requireUserEmail(authentication));
        return ResponseEntity.ok(toDto(source));
    }

    @PostMapping("/test-connection")
    public ResponseEntity<Map<String, Object>> testConnection(@RequestBody ConnectionTestRequest request) {
        boolean success = sourceService.testConnection(request.type(), request.config());
        return ResponseEntity.ok(Map.of("success", success));
    }

    @PostMapping("/discover-schema")
    public ResponseEntity<List<TableSchemaResponse>> discoverSchema(@RequestBody ConnectionTestRequest request,
                                                                    Authentication authentication) {
        return ResponseEntity.ok(sourceService.discoverSchema(request, requireUserEmail(authentication)));
    }

    @GetMapping("/available")
    public ResponseEntity<List<SourceDTO>> getAvailableSources(Authentication authentication) {
        List<SourceDTO> sources = sourceService.listReusableSources(requireUserEmail(authentication)).stream()
                .map(SourceController::toDto)
                .collect(Collectors.toList());
        return ResponseEntity.ok(sources);
    }

    @GetMapping
    public ResponseEntity<List<SourceDTO>> getAllSources(@RequestParam(value = "role", required = false) SourceRole role,
                                                         @RequestParam(value = "datasetId", required = false) Long datasetId,
                                                         Authentication authentication) {
        List<SourceDTO> sources = sourceService.getAllSources(role, datasetId, requireUserEmail(authentication)).stream()
                .map(SourceController::toDto)
                .collect(Collectors.toList());
        return ResponseEntity.ok(sources);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Source> getSource(@PathVariable String id, Authentication authentication) {
        return ResponseEntity.ok(sourceService.getSourceById(id, requireUserEmail(authentication)));
    }

    @GetMapping("/{id}/schema")
    public ResponseEntity<List<TableSchemaResponse>> describeSchema(@PathVariable String id,
                                                                    Authentication authentication) {
        return ResponseEntity.ok(sourceService.describeSourceSchema(id, requireUserEmail(authentication)));
    }

    @PutMapping("/{id}")
    public ResponseEntity<Source> updateSource(@PathVariable String id,
                                               @RequestBody SourceDTO dto,
                                               Authentication authentication) {
        return ResponseEntity.ok(sourceService.updateSource(id, dto, requireUserEmail(authentication)));
    }

    @PatchMapping("/{id}/dataset")
    public ResponseEntity<SourceDTO> updateSourceDataset(@PathVariable String id,
                                                         @RequestBody(required = false) Map<String, Long> body,
                                                         Authentication authentication) {
        Long datasetId = body != null ? body.get("datasetId") : null;
        Source updated = sourceService.assignSourceToDataset(id, datasetId, requireUserEmail(authentication));
        return ResponseEntity.ok(toDto(updated));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteSource(@PathVariable String id, Authentication authentication) {
        sourceService.deleteSource(id, requireUserEmail(authentication));
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{id}/refresh")
    public ResponseEntity<IngestionRun> ingestSource(@PathVariable String id, Authentication authentication) {
        IngestionRun run = sourceService.triggerIngestion(id, requireUserEmail(authentication));
        return ResponseEntity.ok(run);
    }

    @GetMapping("/{id}/runs")
    public ResponseEntity<List<IngestionRun>> getSourceRuns(@PathVariable String id, Authentication authentication) {
        return ResponseEntity.ok(sourceService.getSourceRuns(id, requireUserEmail(authentication)));
    }

    @GetMapping("/{id}/download")
    public ResponseEntity<Resource> downloadDestinationFile(@PathVariable String id, Authentication authentication) {
        Source destination = sourceService.getSourceById(id, requireUserEmail(authentication));

        if (destination.getRole() != SourceRole.DESTINATION) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Source is not configured as a destination");
        }

        SourceType type = destination.getType();
        if (type != SourceType.CSV) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Destination type does not support downloads");
        }

        Map<String, Object> config = destination.getConfig();
        if (config == null || config.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Destination has no file configuration");
        }

        String configuredPath = stringValue(config.get("filePath"));
        String relativePath = stringValue(config.get("relativePath"));

        if (!StringUtils.hasText(configuredPath) && !StringUtils.hasText(relativePath)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Destination file path is not available");
        }

        Path root = fileStorageService.getRootDirectory();
        Path resolvedPath = resolveDestinationPath(configuredPath, relativePath, root);

        if (!Files.exists(resolvedPath)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Destination file not found");
        }

        String downloadName = determineDownloadFilename(destination, config, type);
        MediaType mediaType = MediaType.parseMediaType("text/csv");

        Resource resource = new PathResource(resolvedPath);
        return ResponseEntity.ok()
                .contentType(mediaType)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + downloadName + "\"")
                .body(resource);
    }

    private String requireUserEmail(Authentication authentication) {
        if (authentication == null || !authentication.isAuthenticated()) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Authentication required");
        }
        return authentication.getName();
    }

    private static SourceDTO toDto(Source source) {
        return new SourceDTO(
                source.getSourceUid(),
                source.getName(),
                source.getType(),
                source.getRole(),
                source.getConfig(),
                source.getStatus(),
                source.getCreatedAt(),
                source.getUpdatedAt(),
                source.getDataset() != null ? source.getDataset().getId() : null
        );
    }

    private Path resolveDestinationPath(String configuredPath, String relativePath, Path rootDirectory) {
        Path resolved;
        try {
            if (StringUtils.hasText(configuredPath)) {
                resolved = Paths.get(configuredPath).normalize();
            } else {
                resolved = Paths.get(relativePath).normalize();
            }
        } catch (InvalidPathException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Destination file path is invalid", exception);
        }

        if (!resolved.isAbsolute()) {
            resolved = rootDirectory.resolve(resolved).normalize();
        }

        if (!resolved.startsWith(rootDirectory)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Destination file path is outside of the storage directory");
        }

        return resolved;
    }

    private String determineDownloadFilename(Source destination, Map<String, Object> config, SourceType type) {
        String configuredName = stringValue(config.get("displayFilename"));
        if (StringUtils.hasText(configuredName)) {
            return sanitizeFilename(configuredName);
        }

        String baseName = StringUtils.hasText(destination.getName()) ? destination.getName() : "destination-output";
        return sanitizeFilename(baseName + ".csv");
    }

    private String sanitizeFilename(String filename) {
        String sanitized = filename.replaceAll("[\\\\/\\r\\n]", "-");
        sanitized = sanitized.replaceAll("[\\\"<>:|?*]", "-");
        return sanitized;
    }

    private String stringValue(Object value) {
        return value == null ? null : Objects.toString(value, null);
    }
}
