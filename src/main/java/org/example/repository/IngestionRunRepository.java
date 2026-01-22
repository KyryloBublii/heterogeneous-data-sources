package org.example.repository;

import org.example.models.entity.IngestionRun;
import org.example.models.entity.Source;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface IngestionRunRepository extends JpaRepository<IngestionRun, Long> {
    List<IngestionRun> findBySourceOrderByStartedAtDesc(Source source);

    List<IngestionRun> findAllBySource_ApplicationUser_Id(Long userId, Sort sort);

    Page<IngestionRun> findBySource_ApplicationUser_Id(Long userId, Pageable pageable);
}
