package com.lineage.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "processed_column_lineages")
public class ProcessedColumnLineage {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "column_lineage_id")
    private UUID columnLineageId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "run_id", nullable = false)
    @JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
    private ExtractionRun extractionRun;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "processed_table_id", nullable = false)
    @JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
    private ProcessedTable processedTable;

    @Column(name = "downstream_table", nullable = false, length = 255)
    private String downstreamTable;

    @Column(name = "downstream_column", nullable = false, length = 255)
    private String downstreamColumn;

    @Column(name = "upstream_table", nullable = false, length = 255)
    private String upstreamTable;

    @Column(name = "upstream_column", nullable = false, length = 255)
    private String upstreamColumn;

    @Column(name = "transformation_type", length = 100)
    private String transformationType;

    @Column(name = "transformation_code", columnDefinition = "TEXT")
    private String transformationCode;

    @Column(name = "file_id", length = 500)
    private String fileId;

    @Column(name = "transformation_lines", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private TransformationLines transformationLines;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime createdAt;

    // Constructors
    public ProcessedColumnLineage() {}

    public ProcessedColumnLineage(ExtractionRun extractionRun, ProcessedTable processedTable,
                                  String downstreamTable, String downstreamColumn,
                                  String upstreamTable, String upstreamColumn) {
        this.extractionRun = extractionRun;
        this.processedTable = processedTable;
        this.downstreamTable = downstreamTable;
        this.downstreamColumn = downstreamColumn;
        this.upstreamTable = upstreamTable;
        this.upstreamColumn = upstreamColumn;
    }

    // Getters and Setters
    public UUID getColumnLineageId() {
        return columnLineageId;
    }

    public void setColumnLineageId(UUID columnLineageId) {
        this.columnLineageId = columnLineageId;
    }

    public ExtractionRun getExtractionRun() {
        return extractionRun;
    }

    public void setExtractionRun(ExtractionRun extractionRun) {
        this.extractionRun = extractionRun;
    }

    public ProcessedTable getProcessedTable() {
        return processedTable;
    }

    public void setProcessedTable(ProcessedTable processedTable) {
        this.processedTable = processedTable;
    }

    public String getDownstreamTable() {
        return downstreamTable;
    }

    public void setDownstreamTable(String downstreamTable) {
        this.downstreamTable = downstreamTable;
    }

    public String getDownstreamColumn() {
        return downstreamColumn;
    }

    public void setDownstreamColumn(String downstreamColumn) {
        this.downstreamColumn = downstreamColumn;
    }

    public String getUpstreamTable() {
        return upstreamTable;
    }

    public void setUpstreamTable(String upstreamTable) {
        this.upstreamTable = upstreamTable;
    }

    public String getUpstreamColumn() {
        return upstreamColumn;
    }

    public void setUpstreamColumn(String upstreamColumn) {
        this.upstreamColumn = upstreamColumn;
    }

    public String getTransformationType() {
        return transformationType;
    }

    public void setTransformationType(String transformationType) {
        this.transformationType = transformationType;
    }

    public String getTransformationCode() {
        return transformationCode;
    }

    public void setTransformationCode(String transformationCode) {
        this.transformationCode = transformationCode;
    }

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public TransformationLines getTransformationLines() {
        return transformationLines;
    }

    public void setTransformationLines(TransformationLines transformationLines) {
        this.transformationLines = transformationLines;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    // Inner class for transformation lines structure
    public static class TransformationLines {
        private Integer startLine;
        private Integer endLine;
        private Integer linesBeforeStartLine;
        private Integer linesAfterEndLine;

        public TransformationLines() {}

        public TransformationLines(Integer startLine, Integer endLine, 
                                   Integer linesBeforeStartLine, Integer linesAfterEndLine) {
            this.startLine = startLine;
            this.endLine = endLine;
            this.linesBeforeStartLine = linesBeforeStartLine;
            this.linesAfterEndLine = linesAfterEndLine;
        }

        // Getters and Setters
        public Integer getStartLine() {
            return startLine;
        }

        public void setStartLine(Integer startLine) {
            this.startLine = startLine;
        }

        public Integer getEndLine() {
            return endLine;
        }

        public void setEndLine(Integer endLine) {
            this.endLine = endLine;
        }

        public Integer getLinesBeforeStartLine() {
            return linesBeforeStartLine;
        }

        public void setLinesBeforeStartLine(Integer linesBeforeStartLine) {
            this.linesBeforeStartLine = linesBeforeStartLine;
        }

        public Integer getLinesAfterEndLine() {
            return linesAfterEndLine;
        }

        public void setLinesAfterEndLine(Integer linesAfterEndLine) {
            this.linesAfterEndLine = linesAfterEndLine;
        }
    }
}
