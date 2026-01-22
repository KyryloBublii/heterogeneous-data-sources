package org.example.repository;

import org.example.models.entity.Dataset;
import org.example.models.entity.DatasetMapping;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DatasetMappingRepository extends JpaRepository<DatasetMapping, Long> {
    List<DatasetMapping> findAllByDataset(Dataset dataset);
}
