package controller;

import org.example.controllers.UserController;
import org.example.models.dto.DeleteAccountRequest;
import org.example.service.UserService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserControllerTest {

    @Mock
    private UserService userService;

    @Mock
    private Authentication authentication;

    @InjectMocks
    private UserController userController;

    @Test
    void deleteAccount_Success() {
        DeleteAccountRequest request = new DeleteAccountRequest("password123");

        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("test@example.com");
        doNothing().when(userService).deleteWithPassword("test@example.com", "password123");

        ResponseEntity<Void> response = userController.deleteAccount(request, authentication);

        assertNotNull(response);
        assertEquals(HttpStatus.NO_CONTENT, response.getStatusCode());

        verify(userService).deleteWithPassword("test@example.com", "password123");
    }

    @Test
    void deleteAccount_NoAuthentication_ThrowsUnauthorized() {
        DeleteAccountRequest request = new DeleteAccountRequest("password123");
        when(authentication.isAuthenticated()).thenReturn(false);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                userController.deleteAccount(request, authentication)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(userService, never()).deleteWithPassword(anyString(), anyString());
    }

    @Test
    void deleteAccount_NullAuthentication_ThrowsUnauthorized() {
        DeleteAccountRequest request = new DeleteAccountRequest("password123");

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                userController.deleteAccount(request, null)
        );

        assertEquals(HttpStatus.UNAUTHORIZED, exception.getStatusCode());
        verify(userService, never()).deleteWithPassword(anyString(), anyString());
    }

    @Test
    void deleteAccount_NullRequest_ThrowsBadRequest() {
        when(authentication.isAuthenticated()).thenReturn(true);

        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
                userController.deleteAccount(null, authentication)
        );

        assertEquals(HttpStatus.BAD_REQUEST, exception.getStatusCode());
        verify(userService, never()).deleteWithPassword(anyString(), anyString());
    }
}
