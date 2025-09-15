package com.lineage.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransformationLinesDto {
    @JsonProperty("start_line")
    private int startLine;
    
    @JsonProperty("end_line")
    private int endLine;
    
    public TransformationLinesDto() {}
    
    public TransformationLinesDto(int startLine, int endLine) {
        this.startLine = startLine;
        this.endLine = endLine;
    }
    
    public int getStartLine() {
        return startLine;
    }
    
    public void setStartLine(int startLine) {
        this.startLine = startLine;
    }
    
    public int getEndLine() {
        return endLine;
    }
    
    public void setEndLine(int endLine) {
        this.endLine = endLine;
    }
}
