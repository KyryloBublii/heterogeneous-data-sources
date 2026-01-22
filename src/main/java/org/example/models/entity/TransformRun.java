package org.example.models.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.example.models.enums.RunStatus;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

@Getter
@Setter
@Entity
@Table(name = "transform_run", schema = "integration")
public class TransformRun {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "transform_run_id", nullable = false)
    private Long id;

    @Column(name = "transform_run_uid", nullable = false, length = 40)
    private String transformRunUid;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "dataset_id", nullable = false)
    private Dataset dataset;

    @ColumnDefault("now()")
    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "ended_at")
    private Instant endedAt;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "run_status", nullable = false, columnDefinition = "integration.run_status")
    @ColumnDefault("'RUNNING'")
    private RunStatus runStatus;

    @ColumnDefault("0")
    @Column(name = "rows_in")
    private Integer rowsIn;

    @ColumnDefault("0")
    @Column(name = "rows_out")
    private Integer rowsOut;

    @Column(name = "error_message", length = Integer.MAX_VALUE)
    private String errorMessage;
}
