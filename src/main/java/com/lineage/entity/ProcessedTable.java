package com.lineage.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Entity
@Table(name = "processed_tables", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"run_id", "entity_id"})
})
public class ProcessedTable {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "processed_table_id")
    private UUID processedTableId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "run_id", nullable = false)
    private ExtractionRun extractionRun;

    @Column(name = "entity_id", nullable = false, length = 255)
    private String entityId;

    @Column(name = "entity_name", nullable = false, length = 255)
    private String entityName;

    @Column(name = "source", length = 255)
    private String source;

    @Column(name = "entity_type", nullable = false, length = 50)
    private String entityType = "table";

    @Column(name = "columns_count", nullable = false)
    private Integer columnsCount = 0;

    @Column(name = "tool_key", length = 100)
    private String toolKey;

    @Column(name = "partition_keys", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<String> partitionKeys;

    @Column(name = "schema_metadata", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private SchemaMetadata schemaMetadata;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime updatedAt;

    // Constructors
    public ProcessedTable() {}

    public ProcessedTable(ExtractionRun extractionRun, String entityId, String entityName) {
        this.extractionRun = extractionRun;
        this.entityId = entityId;
        this.entityName = entityName;
    }

    // Getters and Setters
    public UUID getProcessedTableId() {
        return processedTableId;
    }

    public void setProcessedTableId(UUID processedTableId) {
        this.processedTableId = processedTableId;
    }

    public ExtractionRun getExtractionRun() {
        return extractionRun;
    }

    public void setExtractionRun(ExtractionRun extractionRun) {
        this.extractionRun = extractionRun;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public Integer getColumnsCount() {
        return columnsCount;
    }

    public void setColumnsCount(Integer columnsCount) {
        this.columnsCount = columnsCount;
    }

    public String getToolKey() {
        return toolKey;
    }

    public void setToolKey(String toolKey) {
        this.toolKey = toolKey;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public SchemaMetadata getSchemaMetadata() {
        return schemaMetadata;
    }

    public void setSchemaMetadata(SchemaMetadata schemaMetadata) {
        this.schemaMetadata = schemaMetadata;
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

    // Inner class for schema metadata structure
    public static class SchemaMetadata {
        private List<FieldMetadata> fields;

        public SchemaMetadata() {}

        public SchemaMetadata(List<FieldMetadata> fields) {
            this.fields = fields;
        }

        public List<FieldMetadata> getFields() {
            return fields;
        }

        public void setFields(List<FieldMetadata> fields) {
            this.fields = fields;
        }
    }

    public static class FieldMetadata {
        private String path;
        private String nativeDataType;
        private String label;
        private String description;
        private Boolean nullable;

        public FieldMetadata() {}

        public FieldMetadata(String path, String nativeDataType) {
            this.path = path;
            this.nativeDataType = nativeDataType;
        }

        // Getters and Setters
        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getNativeDataType() {
            return nativeDataType;
        }

        public void setNativeDataType(String nativeDataType) {
            this.nativeDataType = nativeDataType;
        }

        public String getLabel() {
            return label;
        }

        public void setLabel(String label) {
            this.label = label;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public Boolean getNullable() {
            return nullable;
        }

        public void setNullable(Boolean nullable) {
            this.nullable = nullable;
        }
    }
}
