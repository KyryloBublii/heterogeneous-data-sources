package org.example.models.entity;

import jakarta.persistence.*;

import lombok.*;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.type.SqlTypes;
import org.example.models.enums.TransformType;

@Entity
@Table(name = "dataset_mapping", schema = "integration")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(exclude = {"dataset", "source", "datasetField"})
public class DatasetMapping {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "dataset_mapping_id", nullable = false)
    @EqualsAndHashCode.Include
    private Long id;

    @Column(name = "dataset_mapping_uid", nullable = false, length = 40)
    private String datasetMappingUid;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "dataset_id", nullable = false)
    private Dataset dataset;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "source_id", nullable = false)
    private Source source;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "dataset_field_id", nullable = false)
    private DatasetField datasetField;

    @Column(name = "src_json_path", nullable = false, length = Integer.MAX_VALUE)
    private String srcJsonPath;

    @Builder.Default
    @Column(name = "src_path", nullable = false, length = Integer.MAX_VALUE)
    private String srcPath = "";

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "transform_type", nullable = false, columnDefinition = "integration.transform_type")
    @Builder.Default
    private TransformType transformType = TransformType.NONE;

    @Column(name = "transform_sql", length = Integer.MAX_VALUE)
    private String transformSql;

    @ColumnDefault("false")
    @Column(name = "required", nullable = false)
    @Builder.Default
    private Boolean required = false;

    @ColumnDefault("0")
    @Column(name = "priority", nullable = false)
    private Integer priority;
}