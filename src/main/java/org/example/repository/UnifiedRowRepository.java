package org.example.repository;

import org.example.models.entity.Dataset;
import org.example.models.entity.UnifiedRow;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Repository
public interface UnifiedRowRepository extends JpaRepository<UnifiedRow, Long> {
    Page<UnifiedRow> findByDataset(Dataset dataset, Pageable pageable);
    List<UnifiedRow> findByDataset(Dataset dataset);
    Page<UnifiedRow> findByDatasetAndIsExcludedFalse(Dataset dataset, Pageable pageable);
    long countByDataset(Dataset dataset);
    long countByDatasetAndIsExcludedFalse(Dataset dataset);
    void deleteByDataset(Dataset dataset);

    @Query(value = "select * from integration.unified_row where dataset_id = :datasetId and (is_excluded = false or is_excluded is null) order by ingested_at asc, unified_row_id asc", nativeQuery = true)
    List<UnifiedRow> findOrderedNonExcluded(@Param("datasetId") Long datasetId);

    Page<UnifiedRow> findByDatasetIn(Collection<Dataset> datasets, Pageable pageable);

    long countByDatasetIn(Collection<Dataset> datasets);

    Optional<UnifiedRow> findFirstByDatasetOrderByIngestedAtDesc(Dataset dataset);
}