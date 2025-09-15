package com.lineage.dto;

import java.util.List;

public class FineGrainedLineageDto {
    private List<FieldRefDto> upstreams;
    private List<FieldRefDto> downstreams;
    
    public FineGrainedLineageDto() {}
    
    public FineGrainedLineageDto(List<FieldRefDto> upstreams, List<FieldRefDto> downstreams) {
        this.upstreams = upstreams;
        this.downstreams = downstreams;
    }
    
    public List<FieldRefDto> getUpstreams() {
        return upstreams;
    }
    
    public void setUpstreams(List<FieldRefDto> upstreams) {
        this.upstreams = upstreams;
    }
    
    public List<FieldRefDto> getDownstreams() {
        return downstreams;
    }
    
    public void setDownstreams(List<FieldRefDto> downstreams) {
        this.downstreams = downstreams;
    }
}
