package com.lineage.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "assets", indexes = {
        @Index(name = "idx_assets_run_short", columnList = "run_id, short_name")
})
public class Asset {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "asset_id")
    private UUID assetId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "run_id", nullable = false)
    private ExtractionRun extractionRun;

    @Column(name = "full_name", nullable = false, length = 512)
    private String fullName;

    @Column(name = "short_name", nullable = false, length = 255)
    private String shortName;

    @Column(name = "schema_name", length = 512)
    private String schemaName;

    @Enumerated(EnumType.STRING)
    @Column(name = "role", length = 32)
    private Role role;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    public enum Role { SOURCE, TARGET, INTERMEDIATE }

    public UUID getAssetId() { return assetId; }
    public void setAssetId(UUID assetId) { this.assetId = assetId; }
    public ExtractionRun getExtractionRun() { return extractionRun; }
    public void setExtractionRun(ExtractionRun extractionRun) { this.extractionRun = extractionRun; }
    public String getFullName() { return fullName; }
    public void setFullName(String fullName) { this.fullName = fullName; }
    public String getShortName() { return shortName; }
    public void setShortName(String shortName) { this.shortName = shortName; }
    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }
    public Role getRole() { return role; }
    public void setRole(Role role) { this.role = role; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
}


