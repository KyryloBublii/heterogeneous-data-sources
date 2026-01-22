package service;

import org.example.models.entity.ApplicationUser;
import org.example.service.TokenService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TokenServiceTest {

    @Mock
    private JwtEncoder jwtEncoder;

    private TokenService tokenService;

    private ApplicationUser testUser;

    @BeforeEach
    void setUp() {
        tokenService = new TokenService(jwtEncoder, "test-issuer");

        testUser = new ApplicationUser();
        testUser.setId(1L);
        testUser.setUserUid("test-uid");
        testUser.setName("Test User");
        testUser.setEmail("test@example.com");
        testUser.setPasswordHash("encoded-password");
    }

    @Test
    void generateJwt_Success() {
        Jwt mockJwt = mock(Jwt.class);
        when(mockJwt.getTokenValue()).thenReturn("mocked-jwt-token");
        when(jwtEncoder.encode(any(JwtEncoderParameters.class))).thenReturn(mockJwt);

        String token = tokenService.generateJwt(testUser);

        assertNotNull(token);
        assertEquals("mocked-jwt-token", token);
        verify(jwtEncoder).encode(any(JwtEncoderParameters.class));
    }

    @Test
    void generateJwt_WithNullUser_ThrowsException() {
        assertThrows(NullPointerException.class, () -> tokenService.generateJwt(null));
        verify(jwtEncoder, never()).encode(any());
    }

    @Test
    void generateJwt_ContainsUserInformation() {
        Jwt mockJwt = mock(Jwt.class);
        when(mockJwt.getTokenValue()).thenReturn("token-with-claims");
        when(jwtEncoder.encode(any(JwtEncoderParameters.class))).thenReturn(mockJwt);

        String token = tokenService.generateJwt(testUser);

        assertNotNull(token);
        verify(jwtEncoder).encode(any(JwtEncoderParameters.class));
    }

    @Test
    void generateJwt_MultipleCallsProduceDifferentTokens() {
        Jwt mockJwt1 = mock(Jwt.class);
        Jwt mockJwt2 = mock(Jwt.class);
        when(mockJwt1.getTokenValue()).thenReturn("token-1");
        when(mockJwt2.getTokenValue()).thenReturn("token-2");
        when(jwtEncoder.encode(any(JwtEncoderParameters.class)))
                .thenReturn(mockJwt1)
                .thenReturn(mockJwt2);

        String token1 = tokenService.generateJwt(testUser);
        String token2 = tokenService.generateJwt(testUser);

        assertNotNull(token1);
        assertNotNull(token2);
        verify(jwtEncoder, times(2)).encode(any(JwtEncoderParameters.class));
    }
}
