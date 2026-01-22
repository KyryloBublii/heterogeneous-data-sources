package utility;

import org.example.utils.KeyGeneratorUtility;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

import static org.junit.jupiter.api.Assertions.*;

class KeyGeneratorUtilityTest {

    @Test
    void testGenerateRsaKey_shouldReturnNonNullKeyPair() {
        KeyPair keyPair = KeyGeneratorUtility.generateRsaKey();
        assertNotNull(keyPair, "Generated KeyPair should not be null");
    }

    @Test
    void testGenerateRsaKey_shouldHavePublicKey() {
        KeyPair keyPair = KeyGeneratorUtility.generateRsaKey();
        assertNotNull(keyPair.getPublic(), "Public key should not be null");
    }

    @Test
    void testGenerateRsaKey_shouldHavePrivateKey() {
        KeyPair keyPair = KeyGeneratorUtility.generateRsaKey();
        assertNotNull(keyPair.getPrivate(), "Private key should not be null");
    }

    @Test
    void testGenerateRsaKey_shouldGenerateRSAPublicKey() {
        KeyPair keyPair = KeyGeneratorUtility.generateRsaKey();
        assertInstanceOf(RSAPublicKey.class, keyPair.getPublic(),
            "Public key should be an RSA public key");
    }

    @Test
    void testGenerateRsaKey_shouldGenerateRSAPrivateKey() {
        KeyPair keyPair = KeyGeneratorUtility.generateRsaKey();
        assertInstanceOf(RSAPrivateKey.class, keyPair.getPrivate(),
            "Private key should be an RSA private key");
    }

    @Test
    void testGenerateRsaKey_shouldHaveCorrectKeySize() {
        KeyPair keyPair = KeyGeneratorUtility.generateRsaKey();
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();

        // Key size should be 2048 bits
        assertEquals(2048, publicKey.getModulus().bitLength(),
            "RSA key should be 2048 bits");
    }

    @Test
    void testGenerateRsaKey_shouldGenerateUniqueKeys() {
        KeyPair keyPair1 = KeyGeneratorUtility.generateRsaKey();
        KeyPair keyPair2 = KeyGeneratorUtility.generateRsaKey();

        assertNotEquals(keyPair1.getPublic(), keyPair2.getPublic(),
            "Generated public keys should be unique");
        assertNotEquals(keyPair1.getPrivate(), keyPair2.getPrivate(),
            "Generated private keys should be unique");
    }

    @Test
    void testGenerateRsaKey_shouldUseRSAAlgorithm() {
        KeyPair keyPair = KeyGeneratorUtility.generateRsaKey();

        assertEquals("RSA", keyPair.getPublic().getAlgorithm(),
            "Public key algorithm should be RSA");
        assertEquals("RSA", keyPair.getPrivate().getAlgorithm(),
            "Private key algorithm should be RSA");
    }

    @Test
    void testGenerateRsaKey_publicAndPrivateKeyShouldBeRelated() {
        KeyPair keyPair = KeyGeneratorUtility.generateRsaKey();
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();

        // Public and private keys should share the same modulus
        assertEquals(publicKey.getModulus(), privateKey.getModulus(),
            "Public and private keys should share the same modulus");
    }

    @Test
    void testGenerateRsaKey_shouldNotThrowException() {
        assertDoesNotThrow(() -> KeyGeneratorUtility.generateRsaKey(),
            "Key generation should not throw exception");
    }
}
