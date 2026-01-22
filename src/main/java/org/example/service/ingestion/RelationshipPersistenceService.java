package org.example.service.ingestion;

import lombok.RequiredArgsConstructor;
import org.example.models.entity.IngestionRun;
import org.example.models.entity.Relationship;
import org.example.models.entity.Source;
import org.example.repository.RelationshipRepository;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class RelationshipPersistenceService {

    private final RelationshipRepository relationshipRepository;

    public int persist(Source source, IngestionRun run, List<Relationship> relationships) {
        List<Relationship> prepared = prepareForPersistence(relationships);
        for (Relationship relationship : prepared) {
            relationship.setSource(source);
            relationship.setIngestionRun(run);
        }
        relationshipRepository.saveAll(prepared);
        return prepared.size();
    }

    public int persist(List<Relationship> relationships, Map<Source, IngestionRun> runBySource) {
        List<Relationship> prepared = prepareForPersistence(relationships);
        for (Relationship relationship : prepared) {
            IngestionRun run = runBySource != null ? runBySource.get(relationship.getSource()) : null;
            if (run != null) {
                relationship.setIngestionRun(run);
            }
        }
        relationshipRepository.saveAll(prepared);
        return prepared.size();
    }

    private List<Relationship> prepareForPersistence(List<Relationship> relationships) {
        if (relationships == null) {
            return List.of();
        }
        List<Relationship> toSave = new ArrayList<>();
        for (Relationship relationship : relationships) {
            if (relationship == null) {
                continue;
            }
            relationship.setRelationshipUid(UUID.randomUUID().toString());
            toSave.add(relationship);
        }
        return toSave;
    }
}
