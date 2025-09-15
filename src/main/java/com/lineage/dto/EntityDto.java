package com.lineage.dto;

import java.util.List;

public class EntityDto {
    private String entityId;
    private String entityName;
    private String entityType;
    private Integer columnCount;
    private String source;
    private String toolKey;
    private Integer upstreamCount;
    private Integer downstreamCount;
    private Boolean hasUpstream;
    private Boolean hasDownstream;
    private List<FineGrainedLineageDto> fineGrainedLineages;
    private SchemaMetadataDto schemaMetadata;
    
    public EntityDto() {}
    
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
    
    public String getEntityType() {
        return entityType;
    }
    
    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }
    
    public Integer getColumnCount() {
        return columnCount;
    }
    
    public void setColumnCount(Integer columnCount) {
        this.columnCount = columnCount;
    }
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
    
    public String getToolKey() {
        return toolKey;
    }
    
    public void setToolKey(String toolKey) {
        this.toolKey = toolKey;
    }
    
    public Integer getUpstreamCount() {
        return upstreamCount;
    }
    
    public void setUpstreamCount(Integer upstreamCount) {
        this.upstreamCount = upstreamCount;
    }
    
    public Integer getDownstreamCount() {
        return downstreamCount;
    }
    
    public void setDownstreamCount(Integer downstreamCount) {
        this.downstreamCount = downstreamCount;
    }
    
    public Boolean getHasUpstream() {
        return hasUpstream;
    }
    
    public void setHasUpstream(Boolean hasUpstream) {
        this.hasUpstream = hasUpstream;
    }
    
    public Boolean getHasDownstream() {
        return hasDownstream;
    }
    
    public void setHasDownstream(Boolean hasDownstream) {
        this.hasDownstream = hasDownstream;
    }
    
    public List<FineGrainedLineageDto> getFineGrainedLineages() {
        return fineGrainedLineages;
    }
    
    public void setFineGrainedLineages(List<FineGrainedLineageDto> fineGrainedLineages) {
        this.fineGrainedLineages = fineGrainedLineages;
    }
    
    public SchemaMetadataDto getSchemaMetadata() {
        return schemaMetadata;
    }
    
    public void setSchemaMetadata(SchemaMetadataDto schemaMetadata) {
        this.schemaMetadata = schemaMetadata;
    }
}
