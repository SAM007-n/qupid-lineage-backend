package com.lineage.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "lineage", indexes = {
        @Index(name = "idx_lineage_run_to", columnList = "run_id, to_asset_id"),
        @Index(name = "idx_lineage_run_from", columnList = "run_id, from_asset_id")
})
public class Lineage {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "lineage_id")
    private UUID lineageId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "run_id", nullable = false)
    private ExtractionRun extractionRun;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "file_id", nullable = false)
    private File file;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "from_asset_id", nullable = false)
    private Asset fromAsset;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "to_asset_id", nullable = false)
    private Asset toAsset;

    @Column(name = "from_column", length = 255)
    private String fromColumn;

    @Column(name = "to_column", length = 255)
    private String toColumn;

    @Column(name = "edge_type", length = 32)
    private String edgeType; // table_edge | column_edge

    @Column(name = "transformation_type", length = 64)
    private String transformationType;

    @Column(name = "start_line")
    private Integer startLine;

    @Column(name = "end_line")
    private Integer endLine;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    public UUID getLineageId() { return lineageId; }
    public void setLineageId(UUID lineageId) { this.lineageId = lineageId; }
    public ExtractionRun getExtractionRun() { return extractionRun; }
    public void setExtractionRun(ExtractionRun extractionRun) { this.extractionRun = extractionRun; }
    public File getFile() { return file; }
    public void setFile(File file) { this.file = file; }
    public Asset getFromAsset() { return fromAsset; }
    public void setFromAsset(Asset fromAsset) { this.fromAsset = fromAsset; }
    public Asset getToAsset() { return toAsset; }
    public void setToAsset(Asset toAsset) { this.toAsset = toAsset; }
    public String getFromColumn() { return fromColumn; }
    public void setFromColumn(String fromColumn) { this.fromColumn = fromColumn; }
    public String getToColumn() { return toColumn; }
    public void setToColumn(String toColumn) { this.toColumn = toColumn; }
    public String getEdgeType() { return edgeType; }
    public void setEdgeType(String edgeType) { this.edgeType = edgeType; }
    public String getTransformationType() { return transformationType; }
    public void setTransformationType(String transformationType) { this.transformationType = transformationType; }
    public Integer getStartLine() { return startLine; }
    public void setStartLine(Integer startLine) { this.startLine = startLine; }
    public Integer getEndLine() { return endLine; }
    public void setEndLine(Integer endLine) { this.endLine = endLine; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}


