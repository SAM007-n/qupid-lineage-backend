package com.lineage.dto;

public class EdgeDetailsDto {
    private String edgeId;
    private String edgeType; // table_edge | column_edge
    private String fromEntity;
    private String toEntity;
    private String fromColumn;
    private String toColumn;
    private String fileId;
    private TransformationLinesDto lines;
    private String transformationType;

    public String getEdgeId() { return edgeId; }
    public void setEdgeId(String edgeId) { this.edgeId = edgeId; }
    public String getEdgeType() { return edgeType; }
    public void setEdgeType(String edgeType) { this.edgeType = edgeType; }
    public String getFromEntity() { return fromEntity; }
    public void setFromEntity(String fromEntity) { this.fromEntity = fromEntity; }
    public String getToEntity() { return toEntity; }
    public void setToEntity(String toEntity) { this.toEntity = toEntity; }
    public String getFromColumn() { return fromColumn; }
    public void setFromColumn(String fromColumn) { this.fromColumn = fromColumn; }
    public String getToColumn() { return toColumn; }
    public void setToColumn(String toColumn) { this.toColumn = toColumn; }
    public String getFileId() { return fileId; }
    public void setFileId(String fileId) { this.fileId = fileId; }
    public TransformationLinesDto getLines() { return lines; }
    public void setLines(TransformationLinesDto lines) { this.lines = lines; }
    public String getTransformationType() { return transformationType; }
    public void setTransformationType(String transformationType) { this.transformationType = transformationType; }
}


