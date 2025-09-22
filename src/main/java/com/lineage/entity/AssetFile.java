package com.lineage.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "asset_files", indexes = {
        @Index(name = "idx_asset_files_run_file", columnList = "run_id, file_id"),
        @Index(name = "idx_asset_files_asset", columnList = "asset_id")
})
public class AssetFile {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "asset_file_id")
    private UUID assetFileId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "run_id", nullable = false)
    private ExtractionRun extractionRun;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "file_id", nullable = false)
    private File file;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "asset_id", nullable = false)
    private Asset asset;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    public UUID getAssetFileId() { return assetFileId; }
    public void setAssetFileId(UUID assetFileId) { this.assetFileId = assetFileId; }
    public ExtractionRun getExtractionRun() { return extractionRun; }
    public void setExtractionRun(ExtractionRun extractionRun) { this.extractionRun = extractionRun; }
    public File getFile() { return file; }
    public void setFile(File file) { this.file = file; }
    public Asset getAsset() { return asset; }
    public void setAsset(Asset asset) { this.asset = asset; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}


