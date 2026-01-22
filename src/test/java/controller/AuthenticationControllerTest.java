package controller;

import org.example.controllers.AuthenticationController;
import org.example.models.dto.LoginRequestDTO;
import org.example.models.dto.LoginResponseDTO;
import org.example.models.dto.RegistrationDTO;
import org.example.models.entity.ApplicationUser;
import org.example.service.AuthenticationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuthenticationControllerTest {

    @Mock
    private AuthenticationService authenticationService;

    @InjectMocks
    private AuthenticationController authenticationController;

    private ApplicationUser testUser;
    private LoginResponseDTO testLoginResponse;

    @BeforeEach
    void setUp() {
        testUser = new ApplicationUser();
        testUser.setId(1L);
        testUser.setEmail("test@example.com");
        testUser.setName("Test User");

        testLoginResponse = new LoginResponseDTO(
                "test@example.com",
                "Test User",
                "jwt-token-123"
        );
    }

    @Test
    void registerUser_Success() {
        RegistrationDTO registrationDTO = new RegistrationDTO(
                "Test User",
                "test@example.com",
                "password123"
        );

        when(authenticationService.registerUser("Test User", "test@example.com", "password123"))
                .thenReturn(testUser);

        ApplicationUser result = authenticationController.registerUser(registrationDTO);

        assertNotNull(result);
        assertEquals("test@example.com", result.getEmail());
        assertEquals("Test User", result.getName());

        verify(authenticationService).registerUser("Test User", "test@example.com", "password123");
    }

    @Test
    void registerUser_WithoutName_UsesEmailAsName() {
        RegistrationDTO registrationDTO = new RegistrationDTO(
                null,
                "test@example.com",
                "password123"
        );

        when(authenticationService.registerUser(anyString(), eq("test@example.com"), eq("password123")))
                .thenReturn(testUser);

        ApplicationUser result = authenticationController.registerUser(registrationDTO);

        assertNotNull(result);
        verify(authenticationService).registerUser(anyString(), eq("test@example.com"), eq("password123"));
    }

    @Test
    void registerUser_ServiceThrowsException_PropagatesException() {
        RegistrationDTO registrationDTO = new RegistrationDTO(
                "Test User",
                "test@example.com",
                "password123"
        );

        when(authenticationService.registerUser(anyString(), anyString(), anyString()))
                .thenThrow(new IllegalArgumentException("User already exists"));

        assertThrows(IllegalArgumentException.class, () ->
                authenticationController.registerUser(registrationDTO)
        );
    }

    @Test
    void loginUser_Success() {
        LoginRequestDTO loginRequest = new LoginRequestDTO(
                "test@example.com",
                "password123"
        );

        when(authenticationService.loginUser("test@example.com", "password123"))
                .thenReturn(testLoginResponse);

        LoginResponseDTO result = authenticationController.loginUser(loginRequest);

        assertNotNull(result);
        assertEquals("test@example.com", result.email());
        assertEquals("Test User", result.name());
        assertEquals("jwt-token-123", result.token());

        verify(authenticationService).loginUser("test@example.com", "password123");
    }

    @Test
    void loginUser_InvalidCredentials_PropagatesException() {
        LoginRequestDTO loginRequest = new LoginRequestDTO(
                "test@example.com",
                "wrongpassword"
        );

        when(authenticationService.loginUser("test@example.com", "wrongpassword"))
                .thenThrow(new IllegalArgumentException("Invalid credentials"));

        assertThrows(IllegalArgumentException.class, () ->
                authenticationController.loginUser(loginRequest)
        );
    }

    @Test
    void loginUser_UserNotFound_PropagatesException() {
        LoginRequestDTO loginRequest = new LoginRequestDTO(
                "nonexistent@example.com",
                "password123"
        );

        when(authenticationService.loginUser("nonexistent@example.com", "password123"))
                .thenThrow(new IllegalArgumentException("User not found"));

        assertThrows(IllegalArgumentException.class, () ->
                authenticationController.loginUser(loginRequest)
        );
    }
}
