package org.example.repository;

import org.example.models.entity.RawEvent;
import org.example.models.entity.Source;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;
import java.util.Set;

@Repository
public interface RawEventRepository extends JpaRepository<RawEvent, Long> {
    List<RawEvent> findByDataset_Id(Long datasetId);

    Page<RawEvent> findByDataset_Id(Long datasetId, Pageable pageable);

    long countByDataset_Id(Long datasetId);

    boolean existsBySourceAndPayloadHash(Source source, String payloadHash);

    @Query("select r.payloadHash from RawEvent r where r.source = :source and r.payloadHash in :hashes")
    Set<String> findExistingPayloadHashes(Source source, Collection<String> hashes);
}
