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
@Table(name = "tables")
public class TableEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "table_id")
    private UUID tableId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "file_id", nullable = false)
    private File file;

    @Column(name = "table_name", nullable = false, length = 255)
    private String tableName;

    @Enumerated(EnumType.STRING)
    @Column(name = "table_role", nullable = false, length = 50)
    private TableRole tableRole;

    @Column(name = "table_schema", length = 100)
    private String tableSchema;

    @Column(name = "columns", columnDefinition = "jsonb", nullable = false)
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> columns;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime createdAt;

    // Constructors
    public TableEntity() {}

    public TableEntity(File file, String tableName, TableRole tableRole, Map<String, Object> columns) {
        this.file = file;
        this.tableName = tableName;
        this.tableRole = tableRole;
        this.columns = columns;
    }

    // Getters and Setters
    public UUID getTableId() {
        return tableId;
    }

    public void setTableId(UUID tableId) {
        this.tableId = tableId;
    }

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public TableRole getTableRole() {
        return tableRole;
    }

    public void setTableRole(TableRole tableRole) {
        this.tableRole = tableRole;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public Map<String, Object> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, Object> columns) {
        this.columns = columns;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    // Enums
    public enum TableRole {
        SOURCE, TARGET, INTERMEDIATE
    }
} 