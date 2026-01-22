package org.example.repository;

import org.example.models.entity.DatasetField;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface DatasetFieldRepository extends JpaRepository<DatasetField, Long> {

    List<DatasetField> findAllByDataset_Id(Long datasetId);

    Optional<DatasetField> findByDatasetFieldUid(String datasetFieldUid);
}
