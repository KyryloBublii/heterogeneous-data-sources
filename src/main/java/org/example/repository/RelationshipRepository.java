package org.example.repository;

import org.example.models.entity.Relationship;
import org.example.models.entity.Source;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface RelationshipRepository extends JpaRepository<Relationship, Long> {

    List<Relationship> findBySource(Source source);

    @Query("select r from Relationship r where r.source.dataset.id = :datasetId")
    List<Relationship> findByDatasetId(Long datasetId);
}
