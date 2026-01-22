package service;

import org.example.models.enums.SourceType;
import org.example.service.FileStorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mock.web.MockMultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class FileStorageServiceTest {

    @TempDir
    Path tempDir;

    private FileStorageService fileStorageService;

    @BeforeEach
    void setUp() {
        fileStorageService = new FileStorageService(tempDir.toString());
    }

    @Test
    void constructor_CreatesRootDirectory() {
        assertTrue(Files.exists(tempDir));
        assertTrue(Files.isDirectory(tempDir));
    }

    @Test
    void getRootDirectory_ReturnsConfiguredPath() {
        Path rootDir = fileStorageService.getRootDirectory();

        assertNotNull(rootDir);
        assertEquals(tempDir.toAbsolutePath().normalize(), rootDir);
    }

    @Test
    void storeSourceFile_Success() throws IOException {
        byte[] content = "test,data\n1,2\n3,4".getBytes();
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                content
        );

        FileStorageService.StoredFileMetadata metadata =
                fileStorageService.storeSourceFile(file, "source-1", "test.csv");

        assertNotNull(metadata);
        assertNotNull(metadata.absolutePath());
        assertTrue(Files.exists(metadata.absolutePath()));
        assertEquals("test.csv", metadata.originalFilename());
        assertEquals(".csv", metadata.extension());
        assertEquals(content.length, metadata.sizeBytes());
        assertNotNull(metadata.hash());
    }

    @Test
    void storeSourceFile_WithPreferredName_UsesSanitizedName() throws IOException {
        byte[] content = "test data".getBytes();
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "original.csv",
                "text/csv",
                content
        );

        FileStorageService.StoredFileMetadata metadata =
                fileStorageService.storeSourceFile(file, "source-1", "preferred name.csv");

        assertNotNull(metadata);
        assertEquals("preferred-name.csv", metadata.displayFilename());
    }

    @Test
    void storeSourceFile_EmptyFile_ThrowsException() {
        MockMultipartFile emptyFile = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                new byte[0]
        );

        assertThrows(IllegalArgumentException.class, () ->
                fileStorageService.storeSourceFile(emptyFile, "source-1", "test.csv")
        );
    }

    @Test
    void storeSourceFile_NullFile_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                fileStorageService.storeSourceFile(null, "source-1", "test.csv")
        );
    }

    @Test
    void storeSourceFile_WithoutPreferredName_UsesOriginalFilename() throws IOException {
        byte[] content = "test data".getBytes();
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "original.csv",
                "text/csv",
                content
        );

        FileStorageService.StoredFileMetadata metadata =
                fileStorageService.storeSourceFile(file, "source-1", null);

        assertNotNull(metadata);
        assertEquals("original.csv", metadata.originalFilename());
    }

    @Test
    void storeSourceFile_SpecialCharactersInFilename_Sanitizes() throws IOException {
        byte[] content = "test data".getBytes();
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test file!@#$%.csv",
                "text/csv",
                content
        );

        FileStorageService.StoredFileMetadata metadata =
                fileStorageService.storeSourceFile(file, "source-1", "test file!@#$%.csv");

        assertNotNull(metadata);
        assertTrue(metadata.displayFilename().matches("[a-zA-Z0-9._-]+"));
    }

    @Test
    void prepareDestinationFile_CSV_Success() {
        FileStorageService.StoredFileMetadata metadata =
                fileStorageService.prepareDestinationFile("dest-1", "output", SourceType.CSV);

        assertNotNull(metadata);
        assertTrue(Files.exists(metadata.absolutePath()));
        assertEquals(".csv", metadata.extension());
        assertTrue(metadata.displayFilename().endsWith(".csv"));
    }

    @Test
    void prepareDestinationFile_WithExtension_DoesNotDuplicate() {
        FileStorageService.StoredFileMetadata metadata =
                fileStorageService.prepareDestinationFile("dest-1", "output.csv", SourceType.CSV);

        assertNotNull(metadata);
        assertEquals(".csv", metadata.extension());
        assertEquals("output.csv", metadata.displayFilename());
        assertFalse(metadata.displayFilename().endsWith(".csv.csv"));
    }

    @Test
    void prepareDestinationFile_Database_UsesDataExtension() {
        FileStorageService.StoredFileMetadata metadata =
                fileStorageService.prepareDestinationFile("dest-1", "output", SourceType.DB);

        assertNotNull(metadata);
        assertEquals(".dat", metadata.extension());
        assertTrue(metadata.displayFilename().endsWith(".dat"));
    }

    @Test
    void prepareDestinationFile_EmptyName_UsesDefaultName() {
        FileStorageService.StoredFileMetadata metadata =
                fileStorageService.prepareDestinationFile("dest-1", "", SourceType.CSV);

        assertNotNull(metadata);
        assertNotNull(metadata.displayFilename());
        assertTrue(metadata.displayFilename().endsWith(".csv"));
    }

    @Test
    void prepareDestinationFile_SpecialCharactersInKey_Sanitizes() {
        FileStorageService.StoredFileMetadata metadata =
                fileStorageService.prepareDestinationFile("dest!@#$%", "output", SourceType.CSV);

        assertNotNull(metadata);
        assertTrue(Files.exists(metadata.absolutePath()));
    }

    @Test
    void storeSourceFile_SameContentTwice_SharesSameHash() throws IOException {
        byte[] content = "identical content".getBytes();

        MockMultipartFile file1 = new MockMultipartFile(
                "file",
                "file1.csv",
                "text/csv",
                content
        );

        MockMultipartFile file2 = new MockMultipartFile(
                "file",
                "file2.csv",
                "text/csv",
                content
        );

        FileStorageService.StoredFileMetadata metadata1 =
                fileStorageService.storeSourceFile(file1, "source-1", "file1.csv");

        FileStorageService.StoredFileMetadata metadata2 =
                fileStorageService.storeSourceFile(file2, "source-2", "file2.csv");

        assertNotNull(metadata1.hash());
        assertNotNull(metadata2.hash());
        assertEquals(metadata1.hash(), metadata2.hash());
    }
}
