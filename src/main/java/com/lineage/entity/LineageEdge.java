package com.lineage.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "lineage_edges")
@EntityListeners(com.lineage.listener.LineageEdgeListener.class)
public class LineageEdge {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "edge_id")
    private UUID edgeId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "file_id", nullable = false)
    private File file;

    @Enumerated(EnumType.STRING)
    @Column(name = "edge_type", nullable = false, length = 50)
    private EdgeType edgeType;

    @Column(name = "from_table", nullable = false, length = 255)
    private String fromTable;

    @Column(name = "from_column", length = 255)
    private String fromColumn;

    @Column(name = "to_table", nullable = false, length = 255)
    private String toTable;

    @Column(name = "to_column", length = 255)
    private String toColumn;

    @Column(name = "transformation_type", nullable = false, length = 100)
    private String transformationType;

    @Column(name = "transformation_lines", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> transformationLines;

    @Column(name = "transformation_code", columnDefinition = "TEXT")
    private String transformationCode;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime createdAt;

    // Constructors
    public LineageEdge() {}

    public LineageEdge(File file, EdgeType edgeType, String fromTable, String toTable, String transformationType) {
        this.file = file;
        this.edgeType = edgeType;
        this.fromTable = fromTable;
        this.toTable = toTable;
        this.transformationType = transformationType;
    }

    // Getters and Setters
    public UUID getEdgeId() {
        return edgeId;
    }

    public void setEdgeId(UUID edgeId) {
        this.edgeId = edgeId;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public EdgeType getEdgeType() {
        return edgeType;
    }

    public void setEdgeType(EdgeType edgeType) {
        this.edgeType = edgeType;
    }

    public String getFromTable() {
        return fromTable;
    }

    public void setFromTable(String fromTable) {
        this.fromTable = fromTable;
    }

    public String getFromColumn() {
        return fromColumn;
    }

    public void setFromColumn(String fromColumn) {
        this.fromColumn = fromColumn;
    }

    public String getToTable() {
        return toTable;
    }

    public void setToTable(String toTable) {
        this.toTable = toTable;
    }

    public String getToColumn() {
        return toColumn;
    }

    public void setToColumn(String toColumn) {
        this.toColumn = toColumn;
    }

    public String getTransformationType() {
        return transformationType;
    }

    public void setTransformationType(String transformationType) {
        this.transformationType = transformationType;
    }

    public Map<String, Object> getTransformationLines() {
        return transformationLines;
    }

    public void setTransformationLines(Map<String, Object> transformationLines) {
        this.transformationLines = transformationLines;
    }

    public String getTransformationCode() {
        return transformationCode;
    }

    public void setTransformationCode(String transformationCode) {
        this.transformationCode = transformationCode;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    // Enums
    public enum EdgeType {
        TABLE_EDGE, COLUMN_EDGE
    }
} 