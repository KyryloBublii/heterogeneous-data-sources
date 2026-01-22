package org.example.service;

import lombok.extern.slf4j.Slf4j;
import org.example.models.enums.SourceType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Normalizer;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;

@Slf4j
@Service
public class FileStorageService {

    private static final String SOURCES_BUCKET = "sources";
    private static final String DESTINATIONS_BUCKET = "destinations";

    private final Map<String, String> mimeToExtension = Map.of(
            "text/csv", ".csv"
    );

    private final Path rootDirectory;

    public FileStorageService(@Value("${data.storage.root:}") String rootDirectory) {
        if (StringUtils.hasText(rootDirectory)) {
            this.rootDirectory = Paths.get(rootDirectory).toAbsolutePath().normalize();
        } else {
            this.rootDirectory = defaultMacOptimizedPath();
        }
        try {
            Files.createDirectories(this.rootDirectory);
        } catch (IOException ioException) {
            throw new IllegalStateException("Failed to create storage directory: " + this.rootDirectory, ioException);
        }
    }

    public Path getRootDirectory() {
        return rootDirectory;
    }

    public StoredFileMetadata storeSourceFile(MultipartFile file,
                                              String sourceKey,
                                              String preferredName) throws IOException {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("File must not be empty");
        }

        String sanitizedName = sanitizeFilename(StringUtils.hasText(preferredName) ? preferredName : file.getOriginalFilename());
        if (!StringUtils.hasText(sanitizedName)) {
            sanitizedName = "source-file";
        }

        String extension = detectExtension(file, sanitizedName);
        if (!sanitizedName.toLowerCase(Locale.ROOT).endsWith(extension)) {
            sanitizedName = sanitizedName + extension;
        }

        ContentAddressedFile contentAddressed = persistContentAddressed(file, extension);

        Path sourceDirectory = resolveDirectory(SOURCES_BUCKET, sourceKey);
        Files.createDirectories(sourceDirectory);
        Path storagePath = sourceDirectory.resolve(contentAddressed.hash() + extension);
        Path finalPath = storagePath;
        if (!Files.exists(storagePath)) {
            boolean aliasCreated = false;
            try {
                Files.createLink(storagePath, contentAddressed.absolutePath());
                aliasCreated = true;
            } catch (UnsupportedOperationException | IOException linkError) {
                log.debug("Hard link creation failed ({}), attempting symbolic link", linkError.getMessage());
                try {
                    Files.createSymbolicLink(storagePath, contentAddressed.absolutePath());
                    aliasCreated = true;
                } catch (UnsupportedOperationException | IOException | SecurityException symlinkError) {
                    log.debug("Symbolic link creation failed: {}", symlinkError.getMessage());
                }
            }
            if (!aliasCreated) {
                finalPath = contentAddressed.absolutePath();
            }
        }

        String relativePath = rootDirectory.relativize(finalPath).toString();
        log.info("Stored source file at {} (hash: {})", finalPath, contentAddressed.hash());

        return new StoredFileMetadata(
                contentAddressed.absolutePath(),
                finalPath,
                file.getOriginalFilename(),
                finalPath.getFileName().toString(),
                sanitizedName,
                extension,
                contentAddressed.sizeBytes(),
                contentAddressed.hash(),
                relativePath
        );
    }

    public StoredFileMetadata prepareDestinationFile(String destinationKey,
                                                     String preferredName,
                                                     SourceType type) {
        String sanitizedKey = sanitizePathSegment(destinationKey);
        String sanitizedName = sanitizeFilename(preferredName);
        if (!StringUtils.hasText(sanitizedName)) {
            sanitizedName = sanitizedKey;
        }
        String extension = type == SourceType.CSV ? ".csv" : ".dat";
        if (!sanitizedName.toLowerCase(Locale.ROOT).endsWith(extension)) {
            sanitizedName = sanitizedName + extension;
        }

        try {
            Path destinationDirectory = resolveDirectory(DESTINATIONS_BUCKET, destinationKey);
            Files.createDirectories(destinationDirectory);
            Path target = destinationDirectory.resolve(sanitizedName);
            if (!Files.exists(target)) {
                Files.createFile(target);
            }
            String relativePath = rootDirectory.relativize(target).toString();
            return new StoredFileMetadata(
                    target,
                    target,
                    sanitizedName,
                    sanitizedName,
                    sanitizedName,
                    extension,
                    Files.size(target),
                    null,
                    relativePath
            );
        } catch (IOException ioException) {
            throw new IllegalStateException("Failed to prepare destination file", ioException);
        }
    }

    private Path resolveDirectory(String bucket, String key) {
        String bucketSegment = sanitizePathSegment(bucket);
        String keySegment = sanitizePathSegment(key);
        return rootDirectory.resolve(Paths.get(bucketSegment, keySegment));
    }

    private String sanitizeFilename(String filename) {
        if (!StringUtils.hasText(filename)) {
            return "";
        }

        String normalized = Normalizer.normalize(filename, Normalizer.Form.NFD)
                .replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
        normalized = normalized.replaceAll("[^a-zA-Z0-9._-]", "-");
        return normalized.replaceAll("-+", "-");
    }

    private String sanitizePathSegment(String value) {
        if (!StringUtils.hasText(value)) {
            return "default";
        }
        String sanitized = sanitizeFilename(value);
        return sanitized.isEmpty() ? "default" : sanitized.toLowerCase(Locale.ROOT);
    }

    private String detectExtension(MultipartFile file, String filename) {
        if (filename != null && filename.contains(".")) {
            return filename.substring(filename.lastIndexOf('.'));
        }
        String mimeType = file.getContentType();
        if (mimeType != null && mimeToExtension.containsKey(mimeType)) {
            return mimeToExtension.get(mimeType);
        }
        return ".dat";
    }

    private ContentAddressedFile persistContentAddressed(MultipartFile file, String extension) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            Path tempFile = Files.createTempFile("upload", extension);
            long written;
            try (InputStream inputStream = file.getInputStream();
                 DigestInputStream digestStream = new DigestInputStream(inputStream, digest)) {
                written = Files.copy(digestStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            }

            String hash = HexFormat.of().formatHex(digest.digest());
            Path hashedDirectory = rootDirectory.resolve(Paths.get(hash.substring(0, 2), hash.substring(2, 4)));
            Files.createDirectories(hashedDirectory);
            Path hashedPath = hashedDirectory.resolve(hash + extension);

            if (Files.exists(hashedPath)) {
                Files.deleteIfExists(tempFile);
            } else {
                Files.move(tempFile, hashedPath, StandardCopyOption.REPLACE_EXISTING);
            }

            return new ContentAddressedFile(hashedPath, written, hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest algorithm not available", e);
        }
    }

    private Path defaultMacOptimizedPath() {
        Path projectRoot = locateProjectRoot(Paths.get("").toAbsolutePath().normalize());
        if (projectRoot != null) {
            return projectRoot.resolve("data").toAbsolutePath().normalize();
        }

        String userHome = System.getProperty("user.home", ".");
        Path macPath = Paths.get(userHome, "Library", "Application Support", "HeterogeneousSources", "data");
        return macPath.toAbsolutePath().normalize();
    }

    private Path locateProjectRoot(Path start) {
        Path current = start;
        while (current != null) {
            if (Files.exists(current.resolve("settings.gradle")) || Files.exists(current.resolve("build.gradle")) ||
                    Files.exists(current.resolve(".git"))) {
                return current;
            }
            current = current.getParent();
        }
        return null;
    }

    public record StoredFileMetadata(Path absolutePath,
                                     Path storagePath,
                                     String originalFilename,
                                     String storedFilename,
                                     String displayFilename,
                                     String extension,
                                     long sizeBytes,
                                     String hash,
                                     String relativePath) {
    }

    private record ContentAddressedFile(Path absolutePath, long sizeBytes, String hash) {
    }
}
