package controller;

import org.example.controllers.FileUploadController;
import org.example.service.FileStorageService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FileUploadControllerTest {

    @Mock
    private FileStorageService fileStorageService;

    @InjectMocks
    private FileUploadController fileUploadController;

    @TempDir
    Path tempDir;

    private FileStorageService.StoredFileMetadata testMetadata;

    @BeforeEach
    void setUp() throws IOException {
        Path testFile = tempDir.resolve("test.csv");

        testMetadata = new FileStorageService.StoredFileMetadata(
                testFile,
                testFile,
                "test.csv",
                "test.csv",
                "test.csv",
                ".csv",
                100L,
                "abc123hash",
                "test.csv"
        );
    }

    @Test
    void uploadCSV_Success() throws IOException {
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                "test,data\n1,2".getBytes()
        );

        when(fileStorageService.storeSourceFile(any(MultipartFile.class), eq("source-1"), eq("test.csv")))
                .thenReturn(testMetadata);

        ResponseEntity<Map<String, Object>> response =
                fileUploadController.uploadCSV(file, "source-1", ",", "UTF-8");

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue((Boolean) response.getBody().get("success"));
        assertEquals("test.csv", response.getBody().get("storedFilename"));
        assertEquals("abc123hash", response.getBody().get("sha256"));
        assertEquals(",", response.getBody().get("delimiter"));
        assertEquals("UTF-8", response.getBody().get("encoding"));

        verify(fileStorageService).storeSourceFile(any(MultipartFile.class), eq("source-1"), eq("test.csv"));
    }

    @Test
    void uploadCSV_WithCustomDelimiter_Success() throws IOException {
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.tsv",
                "text/tab-separated-values",
                "test\tdata\n1\t2".getBytes()
        );

        when(fileStorageService.storeSourceFile(any(MultipartFile.class), anyString(), anyString()))
                .thenReturn(testMetadata);

        ResponseEntity<Map<String, Object>> response =
                fileUploadController.uploadCSV(file, "source-1", "\t", "UTF-8");

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals("\t", response.getBody().get("delimiter"));

        verify(fileStorageService).storeSourceFile(any(MultipartFile.class), eq("source-1"), anyString());
    }

    @Test
    void uploadCSV_EmptyFile_ReturnsBadRequest() throws IOException {
        MockMultipartFile emptyFile = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                new byte[0]
        );

        ResponseEntity<Map<String, Object>> response =
                fileUploadController.uploadCSV(emptyFile, "source-1", ",", "UTF-8");

        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().containsKey("error"));

        verify(fileStorageService, never()).storeSourceFile(any(), anyString(), anyString());
    }

    @Test
    void uploadCSV_StorageServiceThrowsIOException_ReturnsInternalServerError() throws IOException {
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                "test,data".getBytes()
        );

        when(fileStorageService.storeSourceFile(any(), anyString(), anyString()))
                .thenThrow(new IOException("Disk full"));

        ResponseEntity<Map<String, Object>> response =
                fileUploadController.uploadCSV(file, "source-1", ",", "UTF-8");

        assertNotNull(response);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertTrue(response.getBody().containsKey("error"));
        assertTrue(response.getBody().get("error").toString().contains("Disk full"));
    }

    @Test
    void uploadCSV_StorageServiceThrowsIllegalArgumentException_ReturnsBadRequest() throws IOException {
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "test.csv",
                "text/csv",
                "test,data".getBytes()
        );

        when(fileStorageService.storeSourceFile(any(), anyString(), anyString()))
                .thenThrow(new IllegalArgumentException("Invalid file format"));

        ResponseEntity<Map<String, Object>> response =
                fileUploadController.uploadCSV(file, "source-1", ",", "UTF-8");

        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertTrue(response.getBody().containsKey("error"));
        assertTrue(response.getBody().get("error").toString().contains("Invalid file format"));
    }

    @Test
    void listUploadedFiles_WithFiles_ReturnsFileList() throws IOException {
        Path csvFile = tempDir.resolve("test.csv");
        Files.write(csvFile, "test".getBytes());

        when(fileStorageService.getRootDirectory()).thenReturn(tempDir);

        ResponseEntity<Map<String, Object>> response = fileUploadController.listUploadedFiles();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().containsKey("files"));
        assertTrue(response.getBody().containsKey("count"));
        assertTrue(response.getBody().containsKey("root"));

        String[] files = (String[]) response.getBody().get("files");
        assertEquals(1, files.length);
        assertEquals("test.csv", files[0]);

        verify(fileStorageService).getRootDirectory();
    }

    @Test
    void listUploadedFiles_EmptyDirectory_ReturnsEmptyList() {
        when(fileStorageService.getRootDirectory()).thenReturn(tempDir);

        ResponseEntity<Map<String, Object>> response = fileUploadController.listUploadedFiles();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());

        String[] files = (String[]) response.getBody().get("files");
        assertEquals(0, files.length);
        assertEquals(0, response.getBody().get("count"));
    }

    @Test
    void listUploadedFiles_RootDoesNotExist_ReturnsEmptyList() {
        Path nonExistentPath = tempDir.resolve("nonexistent");

        when(fileStorageService.getRootDirectory()).thenReturn(nonExistentPath);

        ResponseEntity<Map<String, Object>> response = fileUploadController.listUploadedFiles();

        assertNotNull(response);
        assertEquals(HttpStatus.OK, response.getStatusCode());

        String[] files = (String[]) response.getBody().get("files");
        assertEquals(0, files.length);
    }
}
