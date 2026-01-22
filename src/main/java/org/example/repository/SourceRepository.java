package org.example.repository;

import org.example.models.entity.Source;
import org.example.models.enums.SourceRole;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SourceRepository extends JpaRepository<Source, Long> {
    List<Source> findAllByApplicationUser_Email(String email);

    List<Source> findAllByApplicationUser_EmailAndRole(String email, SourceRole role);

    List<Source> findAllByDataset_IdAndApplicationUser_Email(Long datasetId, String email);

    List<Source> findAllByDataset_IdAndApplicationUser_EmailAndRole(Long datasetId, String email, SourceRole role);

    List<Source> findAllByDataset_Id(Long datasetId);

    Optional<Source> findBySourceUidAndApplicationUser_Email(String sourceUid, String email);

    Optional<Source> findByIdAndApplicationUser_Email(Long id, String email);
}
