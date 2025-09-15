package com.lineage.dto;

import java.util.List;

public class SchemaMetadataDto {
    private List<SchemaFieldDto> fields;
    
    public SchemaMetadataDto() {}
    
    public SchemaMetadataDto(List<SchemaFieldDto> fields) {
        this.fields = fields;
    }
    
    public List<SchemaFieldDto> getFields() {
        return fields;
    }
    
    public void setFields(List<SchemaFieldDto> fields) {
        this.fields = fields;
    }
}
