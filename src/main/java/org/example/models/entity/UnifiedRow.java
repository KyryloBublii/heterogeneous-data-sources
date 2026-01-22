package org.example.models.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;

@Getter
@Setter
@Entity
@Table(name = "unified_row", schema = "integration")
public class UnifiedRow {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "unified_row_id", nullable = false)
    private Long id;

    @Column(name = "unified_row_uid", nullable = false, length = 40)
    private String unifiedRowUid;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "dataset_id", nullable = false)
    private Dataset dataset;

    @ManyToOne(fetch = FetchType.LAZY)
    @OnDelete(action = OnDeleteAction.SET_NULL)
    @JoinColumn(name = "source_id")
    private Source source;

    @Column(name = "record_key", length = Integer.MAX_VALUE)
    private String recordKey;

    @Column(name = "data", nullable = false)
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> data;

    @ColumnDefault("false")
    @Column(name = "is_excluded", nullable = false)
    private Boolean isExcluded = false;

    @Column(name = "observed_at")
    private Instant observedAt;

    @ColumnDefault("now()")
    @Column(name = "ingested_at", nullable = false)
    private Instant ingestedAt;


}