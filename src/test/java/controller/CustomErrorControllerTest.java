package controller;

import org.example.controllers.CustomErrorController;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class CustomErrorControllerTest {

    @InjectMocks
    private CustomErrorController customErrorController;

    @Test
    void handleError_ReturnsNotFoundWithHtml() throws IOException {
        ResponseEntity<String> response = customErrorController.handleError();

        assertNotNull(response);
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertEquals(MediaType.TEXT_HTML, response.getHeaders().getContentType());
        assertNotNull(response.getBody());
    }

    @Test
    void handleError_ReturnsHtmlContent() throws IOException {
        ResponseEntity<String> response = customErrorController.handleError();

        assertNotNull(response.getBody());
        assertTrue(response.getBody().length() > 0);
    }
}
