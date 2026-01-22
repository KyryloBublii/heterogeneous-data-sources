package org.example.controllers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.service.FileStorageService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/upload")
@RequiredArgsConstructor
public class FileUploadController {

    private final FileStorageService fileStorageService;

    @PostMapping("/csv")
    public ResponseEntity<Map<String, Object>> uploadCSV(@RequestParam("file") MultipartFile file,
                                                         @RequestParam("sourceKey") String sourceKey,
                                                         @RequestParam(value = "delimiter", defaultValue = ",") String delimiter,
                                                         @RequestParam(value = "encoding", defaultValue = "UTF-8") String encoding) {
        log.info("Received CSV file upload: {} for source {}", file.getOriginalFilename(), sourceKey);

        return handleFileUpload(file, sourceKey, file.getOriginalFilename(), Map.of(
                "format", "csv",
                "delimiter", delimiter,
                "encoding", encoding
        ));
    }

    private ResponseEntity<Map<String, Object>> handleFileUpload(MultipartFile file,
                                                                 String sourceKey,
                                                                 String preferredName,
                                                                 Map<String, Object> extraConfig) {
        if (file == null || file.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("error", "File is empty"));
        }

        try {
            FileStorageService.StoredFileMetadata metadata = fileStorageService.storeSourceFile(
                    file,
                    sourceKey,
                    preferredName
            );

            String relativePath = metadata.relativePath();

            Map<String, Object> config = new HashMap<>();
            config.put("filePath", metadata.absolutePath().toString());
            config.put("relativePath", relativePath);
            config.put("storedFilename", metadata.storedFilename());
            config.put("displayFilename", metadata.displayFilename());
            config.put("sha256", metadata.hash());
            config.put("extension", metadata.extension());
            config.put("size", metadata.sizeBytes());
            config.putAll(extraConfig);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("filePath", metadata.absolutePath().toString());
            response.put("relativePath", relativePath);
            response.put("storedFilename", metadata.storedFilename());
            response.put("displayFilename", metadata.displayFilename());
            response.put("originalFilename", metadata.originalFilename());
            response.put("size", metadata.sizeBytes());
            response.put("sha256", metadata.hash());
            response.put("config", config);

            extraConfig.forEach((key, value) -> {
                if (!"format".equals(key)) {
                    response.put(key, value);
                }
            });

            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException exception) {
            log.error("Invalid file upload request: {}", exception.getMessage());
            return ResponseEntity.badRequest().body(Map.of("error", exception.getMessage()));
        } catch (IOException e) {
            log.error("Error uploading file: {}", e.getMessage(), e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to upload file: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    @GetMapping("/files")
    public ResponseEntity<Map<String, Object>> listUploadedFiles() {
        try {
            Path root = fileStorageService.getRootDirectory();
            if (!Files.exists(root)) {
                return ResponseEntity.ok(Map.of("files", new String[0]));
            }

            String[] files;
            try (var stream = Files.walk(root, 3)) {
                files = stream
                        .filter(Files::isRegularFile)
                        .map(root::relativize)
                        .map(Path::toString)
                        .filter(name -> name.toLowerCase().endsWith(".csv"))
                        .toArray(String[]::new);
            }

            Map<String, Object> response = new HashMap<>();
            response.put("files", files);
            response.put("count", files.length);
            response.put("root", root.toString());

            return ResponseEntity.ok(response);

        } catch (IOException e) {
            log.error("Error listing uploaded files: {}", e.getMessage(), e);

            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to list files: " + e.getMessage());

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
