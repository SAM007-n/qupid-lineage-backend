package com.lineage.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.lineage.entity.FlexibleLocalDateTimeDeserializer;
import java.time.LocalDateTime;

public class WebhookEvent {

    private String eventType;
    private String runId;
    private String podId;
    private Object data;
    
    @JsonDeserialize(using = FlexibleLocalDateTimeDeserializer.class)
    private LocalDateTime timestamp;

    // Constructors
    public WebhookEvent() {}

    public WebhookEvent(String eventType, String runId, Object data) {
        this.eventType = eventType;
        this.runId = runId;
        this.data = data;
        this.timestamp = LocalDateTime.now();
    }

    // Getters and Setters
    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    // Event Types
    public static class EventTypes {
        public static final String EXTRACTION_RUN_STARTED = "extraction_run_started";
        public static final String EXTRACTION_RUN_COMPLETED = "extraction_run_completed";
        public static final String EXTRACTION_RUN_FAILED = "extraction_run_failed";
        public static final String FILE_EXTRACTION = "file_extraction";
        public static final String PROGRESS_UPDATE = "progress_update";
        public static final String JOB_STATUS_UPDATE = "job_status_update";
    }
} 