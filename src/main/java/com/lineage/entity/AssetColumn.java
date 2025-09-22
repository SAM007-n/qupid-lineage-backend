package com.lineage.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "asset_columns", indexes = {
        @Index(name = "idx_asset_columns_asset", columnList = "asset_id"),
        @Index(name = "idx_asset_columns_name", columnList = "column_name"),
        @Index(name = "idx_asset_columns_role", columnList = "role")
})
public class AssetColumn {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "asset_column_id")
    private UUID assetColumnId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "asset_id", nullable = false)
    private Asset asset;

    @Column(name = "column_name", nullable = false, length = 255)
    private String columnName;

    @Enumerated(EnumType.STRING)
    @Column(name = "role", length = 32)
    private Asset.Role role; // SOURCE, TARGET, or null for backward compatibility

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    public UUID getAssetColumnId() { return assetColumnId; }
    public void setAssetColumnId(UUID assetColumnId) { this.assetColumnId = assetColumnId; }
    public Asset getAsset() { return asset; }
    public void setAsset(Asset asset) { this.asset = asset; }
    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }
    public Asset.Role getRole() { return role; }
    public void setRole(Asset.Role role) { this.role = role; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}


