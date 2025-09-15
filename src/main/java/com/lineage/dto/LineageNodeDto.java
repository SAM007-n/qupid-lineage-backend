package com.lineage.dto;

import java.util.List;

public class LineageNodeDto {
    private String entityId;
    private String entityName;
    private String entityType;
    private List<TransformationDto> transformations;
    
    public LineageNodeDto() {}
    
    public LineageNodeDto(String entityId, String entityName, String entityType) {
        this.entityId = entityId;
        this.entityName = entityName;
        this.entityType = entityType;
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
    
    public List<TransformationDto> getTransformations() {
        return transformations;
    }
    
    public void setTransformations(List<TransformationDto> transformations) {
        this.transformations = transformations;
    }
}
