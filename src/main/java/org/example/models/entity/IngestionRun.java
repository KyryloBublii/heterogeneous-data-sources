package org.example.models.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.example.models.entity.Dataset;
import org.example.models.enums.RunStatus;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Setter
@Getter
@Entity
@Table(name = "ingestion_run", schema = "integration")
public class IngestionRun {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ingestion_id", nullable = false)
    private Long id;

    @Column(name = "ingestion_uid", nullable = false, length = 40)
    private String ingestionUid;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "dataset_id")
    private Dataset dataset;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "source_id", nullable = false)
    private Source source;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "destination_id")
    private Source destination;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "run_status", nullable = false, columnDefinition = "integration.run_status")
    @ColumnDefault("'QUEUED'")
    private RunStatus runStatus;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "ended_at")
    private Instant endedAt;

    @ColumnDefault("0")
    @Column(name = "rows_read")
    private Integer rowsRead;

    @ColumnDefault("0")
    @Column(name = "rows_stored")
    private Integer rowsStored;

    @Column(name = "error_message", length = Integer.MAX_VALUE)
    private String errorMessage;
}
