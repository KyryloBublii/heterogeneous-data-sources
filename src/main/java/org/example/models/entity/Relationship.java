package org.example.models.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;

@Getter
@Setter
@Entity
@Table(name = "relationship", schema = "integration")
public class Relationship {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "relationship_id", nullable = false)
    private Long id;

    @Column(name = "relationship_uid", nullable = false, length = 40)
    private String relationshipUid;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "source_id", nullable = false)
    private Source source;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "ingestion_run_id")
    private IngestionRun ingestionRun;

    @Column(name = "from_type", nullable = false)
    private String fromType;

    @Column(name = "from_id", nullable = false)
    private String fromId;

    @Column(name = "to_type", nullable = false)
    private String toType;

    @Column(name = "to_id", nullable = false)
    private String toId;

    @Column(name = "relation_type", nullable = false)
    private String relationType;

    @Column(name = "payload")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> payload;

    @Column(name = "ingested_at")
    private Instant ingestedAt;
}
