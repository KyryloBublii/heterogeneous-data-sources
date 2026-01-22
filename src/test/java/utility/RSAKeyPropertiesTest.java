package utility;

import org.example.utils.RSAKeyProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;

import static org.junit.jupiter.api.Assertions.*;

class RSAKeyPropertiesTest {

    private RSAKeyProperties rsaKeyProperties;

    @BeforeEach
    void setUp() {
        rsaKeyProperties = new RSAKeyProperties();
    }

    @Test
    void testConstructor_shouldInitializePublicKey() {
        assertNotNull(rsaKeyProperties.getPublicKey(), "Public key should be initialized");
    }

    @Test
    void testConstructor_shouldInitializePrivateKey() {
        assertNotNull(rsaKeyProperties.getPrivateKey(), "Private key should be initialized");
    }

    @Test
    void testGetPublicKey_shouldReturnRSAPublicKey() {
        RSAPublicKey publicKey = rsaKeyProperties.getPublicKey();
        assertInstanceOf(RSAPublicKey.class, publicKey, "Should return RSA public key");
    }

    @Test
    void testGetPrivateKey_shouldReturnRSAPrivateKey() {
        RSAPrivateKey privateKey = rsaKeyProperties.getPrivateKey();
        assertInstanceOf(RSAPrivateKey.class, privateKey, "Should return RSA private key");
    }

    @Test
    void testKeys_shouldHaveCorrectAlgorithm() {
        assertEquals("RSA", rsaKeyProperties.getPublicKey().getAlgorithm(),
            "Public key should use RSA algorithm");
        assertEquals("RSA", rsaKeyProperties.getPrivateKey().getAlgorithm(),
            "Private key should use RSA algorithm");
    }

    @Test
    void testKeys_shouldHaveCorrectKeySize() {
        RSAPublicKey publicKey = rsaKeyProperties.getPublicKey();
        assertEquals(2048, publicKey.getModulus().bitLength(),
            "Key size should be 2048 bits");
    }

    @Test
    void testKeys_publicAndPrivateKeyShouldBeRelated() {
        RSAPublicKey publicKey = rsaKeyProperties.getPublicKey();
        RSAPrivateKey privateKey = rsaKeyProperties.getPrivateKey();

        assertEquals(publicKey.getModulus(), privateKey.getModulus(),
            "Public and private keys should share the same modulus");
    }

    @Test
    void testConstructor_shouldGenerateUniqueKeysForDifferentInstances() {
        RSAKeyProperties instance1 = new RSAKeyProperties();
        RSAKeyProperties instance2 = new RSAKeyProperties();

        assertNotEquals(instance1.getPublicKey(), instance2.getPublicKey(),
            "Different instances should have different public keys");
        assertNotEquals(instance1.getPrivateKey(), instance2.getPrivateKey(),
            "Different instances should have different private keys");
    }

    @Test
    void testGetPublicKey_shouldReturnSameInstanceOnMultipleCalls() {
        RSAPublicKey publicKey1 = rsaKeyProperties.getPublicKey();
        RSAPublicKey publicKey2 = rsaKeyProperties.getPublicKey();

        assertSame(publicKey1, publicKey2, "Should return the same public key instance");
    }

    @Test
    void testGetPrivateKey_shouldReturnSameInstanceOnMultipleCalls() {
        RSAPrivateKey privateKey1 = rsaKeyProperties.getPrivateKey();
        RSAPrivateKey privateKey2 = rsaKeyProperties.getPrivateKey();

        assertSame(privateKey1, privateKey2, "Should return the same private key instance");
    }
}
