package org.example.repository;

import org.example.models.entity.Dataset;
import org.example.models.entity.TransformRun;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TransformRunRepository extends JpaRepository<TransformRun, Long> {
    List<TransformRun> findAllByDatasetOrderByStartedAtDesc(Dataset dataset);
}
