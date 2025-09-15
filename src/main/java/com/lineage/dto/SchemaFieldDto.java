package com.lineage.dto;

public class SchemaFieldDto {
    private String path;
    private String nativeDataType;
    private String label;
    private String description;
    private Boolean nullable;
    
    public SchemaFieldDto() {}
    
    public SchemaFieldDto(String path, String nativeDataType) {
        this.path = path;
        this.nativeDataType = nativeDataType;
    }
    
    public String getPath() {
        return path;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public String getNativeDataType() {
        return nativeDataType;
    }
    
    public void setNativeDataType(String nativeDataType) {
        this.nativeDataType = nativeDataType;
    }
    
    public String getLabel() {
        return label;
    }
    
    public void setLabel(String label) {
        this.label = label;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public Boolean getNullable() {
        return nullable;
    }
    
    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }
}
