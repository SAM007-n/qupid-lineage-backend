package com.lineage.dto;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public class AssetSummaryDto {
    private UUID assetId;
    private String shortName;
    private String fullName;
    private String schemaName;
    private String role; // SOURCE|TARGET
    private int upstreamCount;
    private int downstreamCount;
    private LocalDateTime firstSeenAt;
    private LocalDateTime lastSeenAt;
    private List<String> labels;

    public UUID getAssetId() { return assetId; }
    public void setAssetId(UUID assetId) { this.assetId = assetId; }

    public String getShortName() { return shortName; }
    public void setShortName(String shortName) { this.shortName = shortName; }

    public String getFullName() { return fullName; }
    public void setFullName(String fullName) { this.fullName = fullName; }

    public String getSchemaName() { return schemaName; }
    public void setSchemaName(String schemaName) { this.schemaName = schemaName; }

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }

    public int getUpstreamCount() { return upstreamCount; }
    public void setUpstreamCount(int upstreamCount) { this.upstreamCount = upstreamCount; }

    public int getDownstreamCount() { return downstreamCount; }
    public void setDownstreamCount(int downstreamCount) { this.downstreamCount = downstreamCount; }

    public LocalDateTime getFirstSeenAt() { return firstSeenAt; }
    public void setFirstSeenAt(LocalDateTime firstSeenAt) { this.firstSeenAt = firstSeenAt; }

    public LocalDateTime getLastSeenAt() { return lastSeenAt; }
    public void setLastSeenAt(LocalDateTime lastSeenAt) { this.lastSeenAt = lastSeenAt; }

    public List<String> getLabels() { return labels; }
    public void setLabels(List<String> labels) { this.labels = labels; }
}


