package com.lineage.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "processed_table_lineages", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"run_id", "table_name"})
})
public class ProcessedTableLineage {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "table_lineage_id")
    private UUID tableLineageId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "run_id", nullable = false)
    @JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
    private ExtractionRun extractionRun;

    @Column(name = "table_name", nullable = false, length = 255)
    private String tableName;

    @Column(name = "upstream_tables", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<TableLineageInfo> upstreamTables;

    @Column(name = "downstream_tables", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<TableLineageInfo> downstreamTables;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime updatedAt;

    // Constructors
    public ProcessedTableLineage() {}

    public ProcessedTableLineage(ExtractionRun extractionRun, String tableName) {
        this.extractionRun = extractionRun;
        this.tableName = tableName;
    }

    // Getters and Setters
    public UUID getTableLineageId() {
        return tableLineageId;
    }

    public void setTableLineageId(UUID tableLineageId) {
        this.tableLineageId = tableLineageId;
    }

    public ExtractionRun getExtractionRun() {
        return extractionRun;
    }

    public void setExtractionRun(ExtractionRun extractionRun) {
        this.extractionRun = extractionRun;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<TableLineageInfo> getUpstreamTables() {
        return upstreamTables;
    }

    public void setUpstreamTables(List<TableLineageInfo> upstreamTables) {
        this.upstreamTables = upstreamTables;
    }

    public List<TableLineageInfo> getDownstreamTables() {
        return downstreamTables;
    }

    public void setDownstreamTables(List<TableLineageInfo> downstreamTables) {
        this.downstreamTables = downstreamTables;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    // Inner classes for JSON structure
    public static class TableLineageInfo {
        private String table;
        private List<TransformationEntry> transformations;

        public TableLineageInfo() {}

        public TableLineageInfo(String table, List<TransformationEntry> transformations) {
            this.table = table;
            this.transformations = transformations;
        }

        // Getters and Setters
        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public List<TransformationEntry> getTransformations() {
            return transformations;
        }

        public void setTransformations(List<TransformationEntry> transformations) {
            this.transformations = transformations;
        }
    }

    public static class TransformationEntry {
        private String fileId;
        private String transformationType;
        private LineRange lines;

        public TransformationEntry() {}

        public TransformationEntry(String fileId, String transformationType, LineRange lines) {
            this.fileId = fileId;
            this.transformationType = transformationType;
            this.lines = lines;
        }

        // Getters and Setters
        public String getFileId() {
            return fileId;
        }

        public void setFileId(String fileId) {
            this.fileId = fileId;
        }

        public String getTransformationType() {
            return transformationType;
        }

        public void setTransformationType(String transformationType) {
            this.transformationType = transformationType;
        }

        public LineRange getLines() {
            return lines;
        }

        public void setLines(LineRange lines) {
            this.lines = lines;
        }
    }

    public static class LineRange {
        private Integer startLine;
        private Integer endLine;

        public LineRange() {}

        public LineRange(Integer startLine, Integer endLine) {
            this.startLine = startLine;
            this.endLine = endLine;
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
    }
}
