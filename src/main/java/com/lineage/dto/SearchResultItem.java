package com.lineage.dto;

public class SearchResultItem {
    private String entityId;
    private String entityName;
    private String entityType;
    private Integer columnCount;

    public SearchResultItem() {}

    public SearchResultItem(String entityId, String entityName, String entityType, Integer columnCount) {
        this.entityId = entityId;
        this.entityName = entityName;
        this.entityType = entityType;
        this.columnCount = columnCount;
    }

    public String getEntityId() { return entityId; }
    public void setEntityId(String entityId) { this.entityId = entityId; }

    public String getEntityName() { return entityName; }
    public void setEntityName(String entityName) { this.entityName = entityName; }

    public String getEntityType() { return entityType; }
    public void setEntityType(String entityType) { this.entityType = entityType; }

    public Integer getColumnCount() { return columnCount; }
    public void setColumnCount(Integer columnCount) { this.columnCount = columnCount; }
}


