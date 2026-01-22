package utility;

import org.example.utils.AppUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AppUtilsTest {

    @Test
    void testGenerateUUID_shouldReturnNonNullValue() {
        String uuid = AppUtils.generateUUID();
        assertNotNull(uuid, "Generated UUID should not be null");
    }

    @Test
    void testGenerateUUID_shouldReturnNonEmptyValue() {
        String uuid = AppUtils.generateUUID();
        assertFalse(uuid.isEmpty(), "Generated UUID should not be empty");
    }

    @Test
    void testGenerateUUID_shouldNotContainDashes() {
        String uuid = AppUtils.generateUUID();
        assertFalse(uuid.contains("-"), "Generated UUID should not contain dashes");
    }

    @Test
    void testGenerateUUID_shouldHaveCorrectLength() {
        String uuid = AppUtils.generateUUID();
        // UUID without dashes should be 32 characters (128 bits in hex)
        assertEquals(32, uuid.length(), "Generated UUID should be 32 characters long");
    }

    @Test
    void testGenerateUUID_shouldGenerateUniqueValues() {
        String uuid1 = AppUtils.generateUUID();
        String uuid2 = AppUtils.generateUUID();
        assertNotEquals(uuid1, uuid2, "Generated UUIDs should be unique");
    }

    @Test
    void testGenerateUUID_shouldOnlyContainHexCharacters() {
        String uuid = AppUtils.generateUUID();
        assertTrue(uuid.matches("[0-9a-f]+"), "Generated UUID should only contain hex characters");
    }
}
