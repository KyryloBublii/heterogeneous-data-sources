package org.example.repository;

import org.example.models.entity.IntegrationConnection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface IntegrationConnectionRepository extends JpaRepository<IntegrationConnection, Long> {

    List<IntegrationConnection> findAllBySource_ApplicationUser_Email(String email);

    IntegrationConnection findByConnectionUidAndSource_ApplicationUser_Email(String connectionUid, String email);

    List<IntegrationConnection> findAllByDataset_IdAndSource_ApplicationUser_Email(Long datasetId, String email);

    List<IntegrationConnection> findAllByDataset_Id(Long datasetId);

    Optional<IntegrationConnection> findFirstBySource_IdAndDestination_IdOrderByCreatedAtDesc(Long sourceId, Long destinationId);

    Optional<IntegrationConnection> findFirstBySource_IdOrderByCreatedAtDesc(Long sourceId);
}
