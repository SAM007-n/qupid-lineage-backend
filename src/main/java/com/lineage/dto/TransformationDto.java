package com.lineage.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransformationDto {
    private String fileId;
    
    @JsonProperty("transformation_type")
    private String transformationType;
    
    private TransformationLinesDto lines;
    
    public TransformationDto() {}
    
    public TransformationDto(String fileId, String transformationType, TransformationLinesDto lines) {
        this.fileId = fileId;
        this.transformationType = transformationType;
        this.lines = lines;
    }
    
    public String getFileId() {
        return fileId;
    }
    
    public void setFileId(String fileId) {
        this.fileId = fileId;
    }
    
    public String getTransformationType() {
        return transformationType;
    }
    
    public void setTransformationType(String transformationType) {
        this.transformationType = transformationType;
    }
    
    public TransformationLinesDto getLines() {
        return lines;
    }
    
    public void setLines(TransformationLinesDto lines) {
        this.lines = lines;
    }
}
