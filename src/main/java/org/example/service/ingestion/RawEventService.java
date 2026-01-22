package org.example.service.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.example.models.entity.IngestionRun;
import org.example.models.entity.RawEvent;
import org.example.models.entity.Source;
import org.example.repository.RawEventRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class RawEventService {

    private final RawEventRepository rawEventRepository;
    private final ObjectMapper objectMapper;

    public int write(Source source, IngestionRun run, List<Map<String, Object>> records) {
        List<RawEvent> events = new ArrayList<>();
        Set<String> batchHashes = new HashSet<>();
        List<String> candidateHashes = new ArrayList<>();
        List<Map<String, Object>> candidateRecords = new ArrayList<>();

        for (Map<String, Object> record : records) {
            String payloadHash = hash(record);
            if (batchHashes.add(payloadHash)) {
                candidateHashes.add(payloadHash);
                candidateRecords.add(record);
            }
        }

        if (candidateHashes.isEmpty()) {
            return 0;
        }

        Set<String> existingHashes = rawEventRepository.findExistingPayloadHashes(source, candidateHashes);

        for (int i = 0; i < candidateRecords.size(); i++) {
            String payloadHash = candidateHashes.get(i);
            if (existingHashes.contains(payloadHash)) {
                continue; // already persisted for this source
            }
            Map<String, Object> record = candidateRecords.get(i);
            RawEvent event = new RawEvent();
            event.setRawEventUid(UUID.randomUUID().toString());
            event.setSource(source);
            event.setIngestionRun(run);
            event.setDataset(source.getDataset());
            event.setPayload(record);
            event.setPayloadHash(payloadHash);
            event.setCreatedAt(Instant.now());
            events.add(event);
        }

        if (events.isEmpty()) {
            return 0;
        }

        try {
            rawEventRepository.saveAll(events);
            return events.size();
        } catch (RuntimeException ex) {
            // Fall back to per-event persistence to honor dedupe constraint under concurrent ingestions.
            int persisted = 0;
            for (RawEvent event : events) {
                if (rawEventRepository.existsBySourceAndPayloadHash(source, event.getPayloadHash())) {
                    continue;
                }
                try {
                    rawEventRepository.save(event);
                    persisted++;
                } catch (DataIntegrityViolationException ignored) {
                    // Another thread/process inserted the same payload hash meanwhile; skip.
                }
            }
            return persisted;
        }
    }

    private String hash(Map<String, Object> record) {
        try {
            String json = objectMapper.writeValueAsString(record);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(json.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder();
            for (byte b : hashed) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (JsonProcessingException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Failed to hash raw event", e);
        }
    }
}
