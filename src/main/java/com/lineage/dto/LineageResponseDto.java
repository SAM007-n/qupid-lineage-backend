package com.lineage.dto;

import java.util.List;

public class LineageResponseDto {
    private String entityId;
    private String entityName;
    private String entityType;
    private LineageDirection direction;
    private List<LineageNodeDto> lineage;
    private List<FineGrainedLineageDto> fineGrainedLineages;
    private SchemaMetadataDto schemaMetadata;
    
    public LineageResponseDto() {}
    
    public LineageResponseDto(String entityId, String entityName, String entityType, 
                             LineageDirection direction, List<LineageNodeDto> lineage) {
        this.entityId = entityId;
        this.entityName = entityName;
        this.entityType = entityType;
        this.direction = direction;
        this.lineage = lineage;
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
    
    public String getEntityType() {
        return entityType;
    }
    
    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }
    
    public LineageDirection getDirection() {
        return direction;
    }
    
    public void setDirection(LineageDirection direction) {
        this.direction = direction;
    }
    
    public List<LineageNodeDto> getLineage() {
        return lineage;
    }
    
    public void setLineage(List<LineageNodeDto> lineage) {
        this.lineage = lineage;
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
