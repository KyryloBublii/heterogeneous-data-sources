package org.example.models.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.example.models.enums.DataType;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.type.SqlTypes;

@Getter
@Setter
@Entity
@Table(name = "dataset_field", schema = "integration")
public class DatasetField {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "dataset_field_id", nullable = false)
    private Long id;

    @Column(name = "dataset_field_uid", nullable = false, length = 40)
    private String datasetFieldUid;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "dataset_id", nullable = false)
    private Dataset dataset;

    @Column(name = "name", nullable = false, length = 60)
    private String name;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "dtype", nullable = false, columnDefinition = "integration.data_type")
    private DataType dtype;

    @ColumnDefault("true")
    @Column(name = "is_nullable", nullable = false)
    private Boolean isNullable = false;

    @ColumnDefault("false")
    @Column(name = "is_unique", nullable = false)
    private Boolean isUnique = false;

    @Column(name = "default_expr", length = Integer.MAX_VALUE)
    private String defaultExpr;

    @ColumnDefault("0")
    @Column(name = "\"position\"", nullable = false)
    private Integer position;
}
