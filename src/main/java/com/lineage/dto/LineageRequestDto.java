package com.lineage.dto;

public class LineageRequestDto {
    private String entityId;
    private LineageDirection direction;
    
    public LineageRequestDto() {}
    
    public LineageRequestDto(String entityId, LineageDirection direction) {
        this.entityId = entityId;
        this.direction = direction;
    }
    
    public String getEntityId() {
        return entityId;
    }
    
    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }
    
    public LineageDirection getDirection() {
        return direction;
    }
    
    public void setDirection(LineageDirection direction) {
        this.direction = direction;
    }
}
