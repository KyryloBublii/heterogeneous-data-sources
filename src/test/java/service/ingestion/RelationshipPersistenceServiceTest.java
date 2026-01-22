package service.ingestion;

import org.example.models.entity.IngestionRun;
import org.example.models.entity.Relationship;
import org.example.models.entity.Source;
import org.example.repository.RelationshipRepository;
import org.example.service.ingestion.RelationshipPersistenceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RelationshipPersistenceServiceTest {

    @Mock
    private RelationshipRepository relationshipRepository;

    private RelationshipPersistenceService service;

    @BeforeEach
    void setUp() {
        service = new RelationshipPersistenceService(relationshipRepository);
    }

    @Test
    void testPersist_withSingleSource_shouldSetSourceAndRun() {
        Source source = new Source();
        source.setId(1L);
        IngestionRun run = new IngestionRun();

        Relationship rel = new Relationship();
        rel.setFromType("users");
        rel.setToType("orders");

        when(relationshipRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.persist(source, run, List.of(rel));

        assertEquals(1, result);
        ArgumentCaptor<List<Relationship>> captor = ArgumentCaptor.forClass(List.class);
        verify(relationshipRepository).saveAll(captor.capture());

        Relationship saved = captor.getValue().get(0);
        assertEquals(source, saved.getSource());
        assertEquals(run, saved.getIngestionRun());
    }

    @Test
    void testPersist_withMultipleRelationships_shouldPersistAll() {
        Source source = new Source();
        IngestionRun run = new IngestionRun();

        List<Relationship> relationships = Arrays.asList(
                new Relationship(),
                new Relationship(),
                new Relationship()
        );

        when(relationshipRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.persist(source, run, relationships);

        assertEquals(3, result);
    }

    @Test
    void testPersist_shouldGenerateUniqueUids() {
        Source source = new Source();
        IngestionRun run = new IngestionRun();

        List<Relationship> relationships = Arrays.asList(
                new Relationship(),
                new Relationship()
        );

        when(relationshipRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        service.persist(source, run, relationships);

        ArgumentCaptor<List<Relationship>> captor = ArgumentCaptor.forClass(List.class);
        verify(relationshipRepository).saveAll(captor.capture());

        List<Relationship> saved = captor.getValue();
        assertNotNull(saved.get(0).getRelationshipUid());
        assertNotNull(saved.get(1).getRelationshipUid());
        assertNotEquals(saved.get(0).getRelationshipUid(), saved.get(1).getRelationshipUid());
    }

    @Test
    void testPersist_withNullList_shouldReturnZero() {
        Source source = new Source();
        IngestionRun run = new IngestionRun();

        int result = service.persist(source, run, null);

        assertEquals(0, result);
        verify(relationshipRepository).saveAll(Collections.emptyList());
    }

    @Test
    void testPersist_withEmptyList_shouldReturnZero() {
        Source source = new Source();
        IngestionRun run = new IngestionRun();

        int result = service.persist(source, run, Collections.emptyList());

        assertEquals(0, result);
        verify(relationshipRepository).saveAll(Collections.emptyList());
    }

    @Test
    void testPersist_withNullRelationshipInList_shouldSkipNull() {
        Source source = new Source();
        IngestionRun run = new IngestionRun();

        List<Relationship> relationships = Arrays.asList(
                new Relationship(),
                null,
                new Relationship()
        );

        when(relationshipRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.persist(source, run, relationships);

        assertEquals(2, result); // Only 2 non-null relationships
    }

    @Test
    void testPersist_withMultipleSources_shouldSetRunForEachSource() {
        Source source1 = new Source();
        source1.setId(1L);
        Source source2 = new Source();
        source2.setId(2L);

        IngestionRun run1 = new IngestionRun();
        IngestionRun run2 = new IngestionRun();

        Map<Source, IngestionRun> runBySource = new HashMap<>();
        runBySource.put(source1, run1);
        runBySource.put(source2, run2);

        Relationship rel1 = new Relationship();
        rel1.setSource(source1);

        Relationship rel2 = new Relationship();
        rel2.setSource(source2);

        when(relationshipRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.persist(Arrays.asList(rel1, rel2), runBySource);

        assertEquals(2, result);

        ArgumentCaptor<List<Relationship>> captor = ArgumentCaptor.forClass(List.class);
        verify(relationshipRepository).saveAll(captor.capture());

        List<Relationship> saved = captor.getValue();
        assertEquals(run1, saved.get(0).getIngestionRun());
        assertEquals(run2, saved.get(1).getIngestionRun());
    }

    @Test
    void testPersist_withMultipleSources_andNullRunMap_shouldHandleGracefully() {
        Relationship rel = new Relationship();
        rel.setSource(new Source());

        when(relationshipRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.persist(List.of(rel), null);

        assertEquals(1, result);
        // Should not throw exception even with null runBySource
    }

    @Test
    void testPersist_withMultipleSources_andMissingRunForSource_shouldHandleGracefully() {
        Source source1 = new Source();
        source1.setId(1L);
        Source source2 = new Source();
        source2.setId(2L);

        IngestionRun run1 = new IngestionRun();

        Map<Source, IngestionRun> runBySource = new HashMap<>();
        runBySource.put(source1, run1);
        // source2 has no run in map

        Relationship rel1 = new Relationship();
        rel1.setSource(source1);

        Relationship rel2 = new Relationship();
        rel2.setSource(source2);

        when(relationshipRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.persist(Arrays.asList(rel1, rel2), runBySource);

        assertEquals(2, result);

        ArgumentCaptor<List<Relationship>> captor = ArgumentCaptor.forClass(List.class);
        verify(relationshipRepository).saveAll(captor.capture());

        List<Relationship> saved = captor.getValue();
        assertEquals(run1, saved.get(0).getIngestionRun());
        assertNull(saved.get(1).getIngestionRun()); // No run for source2
    }

    @Test
    void testPersist_withMultipleSources_withNullInList_shouldSkip() {
        Map<Source, IngestionRun> runBySource = new HashMap<>();

        List<Relationship> relationships = Arrays.asList(
                new Relationship(),
                null,
                new Relationship()
        );

        when(relationshipRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.persist(relationships, runBySource);

        assertEquals(2, result);
    }

    @Test
    void testPersist_shouldReturnNumberOfPersistedRelationships() {
        Source source = new Source();
        IngestionRun run = new IngestionRun();

        List<Relationship> relationships = Arrays.asList(
                new Relationship(),
                new Relationship(),
                null, // Should be skipped
                new Relationship()
        );

        when(relationshipRepository.saveAll(anyList())).thenAnswer(i -> i.getArgument(0));

        int result = service.persist(source, run, relationships);

        assertEquals(3, result); // 3 non-null relationships
    }
}
