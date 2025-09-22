package com.lineage.dto;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public class AssetDetailDto {
    private UUID assetId;
    private String shortName;
    private String fullName;
    private String schemaName;
    private String role;
    private int columnCount;
    private int upstreamCount;
    private int downstreamCount;
    private boolean hasUpstream;
    private boolean hasDownstream;
    private SchemaMetadataDto schemaMetadata;
    private List<FileRefDto> files;

    public static class FileRefDto {
        private String filePath;
        private String fileUrl;
        private LocalDateTime firstSeenAt;
        private LocalDateTime lastSeenAt;

        public String getFilePath() { return filePath; }
        public void setFilePath(String filePath) { this.filePath = filePath; }

        public String getFileUrl() { return fileUrl; }
        public void setFileUrl(String fileUrl) { this.fileUrl = fileUrl; }

        public LocalDateTime getFirstSeenAt() { return firstSeenAt; }
        public void setFirstSeenAt(LocalDateTime firstSeenAt) { this.firstSeenAt = firstSeenAt; }

        public LocalDateTime getLastSeenAt() { return lastSeenAt; }
        public void setLastSeenAt(LocalDateTime lastSeenAt) { this.lastSeenAt = lastSeenAt; }
    }

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
    public int getColumnCount() { return columnCount; }
    public void setColumnCount(int columnCount) { this.columnCount = columnCount; }
    public int getUpstreamCount() { return upstreamCount; }
    public void setUpstreamCount(int upstreamCount) { this.upstreamCount = upstreamCount; }
    public int getDownstreamCount() { return downstreamCount; }
    public void setDownstreamCount(int downstreamCount) { this.downstreamCount = downstreamCount; }
    public boolean isHasUpstream() { return hasUpstream; }
    public void setHasUpstream(boolean hasUpstream) { this.hasUpstream = hasUpstream; }
    public boolean isHasDownstream() { return hasDownstream; }
    public void setHasDownstream(boolean hasDownstream) { this.hasDownstream = hasDownstream; }
    public SchemaMetadataDto getSchemaMetadata() { return schemaMetadata; }
    public void setSchemaMetadata(SchemaMetadataDto schemaMetadata) { this.schemaMetadata = schemaMetadata; }
    public List<FileRefDto> getFiles() { return files; }
    public void setFiles(List<FileRefDto> files) { this.files = files; }
}


