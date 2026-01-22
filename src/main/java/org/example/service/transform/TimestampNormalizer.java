package org.example.service.transform;

import java.time.Instant;

public final class TimestampNormalizer {

    private static final long MILLIS_THRESHOLD = 100_000_000_000L; // ~1973 in ms

    private TimestampNormalizer() {
    }

    public static Object normalizeValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number number) {
            long millis = number.longValue();
            if (looksLikeEpochMillis(millis)) {
                return Instant.ofEpochMilli(millis).toString();
            }
            return value;
        }

        if (value instanceof String str) {
            String trimmed = str.trim();
            if (trimmed.matches("^-?\\d{10,}$")) {
                try {
                    long millis = Long.parseLong(trimmed);
                    if (looksLikeEpochMillis(millis)) {
                        return Instant.ofEpochMilli(millis).toString();
                    }
                } catch (NumberFormatException ignored) {
                    return value;
                }
            }
        }

        return value;
    }

    private static boolean looksLikeEpochMillis(long value) {
        return Math.abs(value) >= MILLIS_THRESHOLD;
    }
}
