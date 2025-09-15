package com.lineage.dto;

public class FieldRefDto {
    private String urn;
    private String path;
    private TransformationDto transformation;
    
    public FieldRefDto() {}
    
    public FieldRefDto(String urn, String path) {
        this.urn = urn;
        this.path = path;
    }
    
    public String getUrn() {
        return urn;
    }
    
    public void setUrn(String urn) {
        this.urn = urn;
    }
    
    public String getPath() {
        return path;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public TransformationDto getTransformation() {
        return transformation;
    }
    
    public void setTransformation(TransformationDto transformation) {
        this.transformation = transformation;
    }
}
