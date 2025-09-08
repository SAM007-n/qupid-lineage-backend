package com.lineage.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "fine_grained_lineages")
public class FineGrainedLineage {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "lineage_id")
    private UUID lineageId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "aggregated_table_id", nullable = false)
    private AggregatedTable aggregatedTable;

    @Column(name = "downstream_table", nullable = false, length = 255)
    private String downstreamTable;

    @Column(name = "downstream_column", nullable = false, length = 255)
    private String downstreamColumn;

    @Column(name = "upstream_references", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<UpstreamReference> upstreamReferences;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime createdAt;

    // Constructors
    public FineGrainedLineage() {}

    public FineGrainedLineage(AggregatedTable aggregatedTable, String downstreamTable, String downstreamColumn) {
        this.aggregatedTable = aggregatedTable;
        this.downstreamTable = downstreamTable;
        this.downstreamColumn = downstreamColumn;
    }

    // Getters and Setters
    public UUID getLineageId() {
        return lineageId;
    }

    public void setLineageId(UUID lineageId) {
        this.lineageId = lineageId;
    }

    public AggregatedTable getAggregatedTable() {
        return aggregatedTable;
    }

    public void setAggregatedTable(AggregatedTable aggregatedTable) {
        this.aggregatedTable = aggregatedTable;
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

    public List<UpstreamReference> getUpstreamReferences() {
        return upstreamReferences;
    }

    public void setUpstreamReferences(List<UpstreamReference> upstreamReferences) {
        this.upstreamReferences = upstreamReferences;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    // Inner classes for JSON structure
    public static class UpstreamReference {
        private String urn;
        private String path;
        private TransformationInfo transformation;

        public UpstreamReference() {}

        public UpstreamReference(String urn, String path, TransformationInfo transformation) {
            this.urn = urn;
            this.path = path;
            this.transformation = transformation;
        }

        // Getters and Setters
        public String getUrn() {
            return urn;
        }

        public void setUrn(String urn) {
            this.urn = urn;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public TransformationInfo getTransformation() {
            return transformation;
        }

        public void setTransformation(TransformationInfo transformation) {
            this.transformation = transformation;
        }
    }

    public static class TransformationInfo {
        private String fileId;
        private String transformationCode;
        private String type;
        private LineInfo lines;

        public TransformationInfo() {}

        public TransformationInfo(String fileId, String transformationCode, String type, LineInfo lines) {
            this.fileId = fileId;
            this.transformationCode = transformationCode;
            this.type = type;
            this.lines = lines;
        }

        // Getters and Setters
        public String getFileId() {
            return fileId;
        }

        public void setFileId(String fileId) {
            this.fileId = fileId;
        }

        public String getTransformationCode() {
            return transformationCode;
        }

        public void setTransformationCode(String transformationCode) {
            this.transformationCode = transformationCode;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public LineInfo getLines() {
            return lines;
        }

        public void setLines(LineInfo lines) {
            this.lines = lines;
        }
    }

    public static class LineInfo {
        private Integer startLine;
        private Integer endLine;
        private Integer linesBeforeStartLine;
        private Integer linesAfterEndLine;

        public LineInfo() {}

        public LineInfo(Integer startLine, Integer endLine) {
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
