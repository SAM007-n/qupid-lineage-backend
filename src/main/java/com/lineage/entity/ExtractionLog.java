package com.lineage.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "extraction_logs")
public class ExtractionLog {
    
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID logId;
    
    @Column(name = "run_id", nullable = false)
    private UUID runId;
    
    @Column(name = "log_level", nullable = false)
    private String logLevel = "INFO";
    
    @Column(name = "message", nullable = false, columnDefinition = "TEXT")
    private String message;
    
    @Column(name = "timestamp", nullable = false)
    private LocalDateTime timestamp;
    
    @Column(name = "source", nullable = false)
    private String source = "docker";
    
    // Constructors
    public ExtractionLog() {}
    
    public ExtractionLog(UUID runId, String message, String logLevel, String source) {
        this.runId = runId;
        this.message = message;
        this.logLevel = logLevel;
        this.source = source;
        this.timestamp = LocalDateTime.now();
    }
    
    // Getters and Setters
    public UUID getLogId() {
        return logId;
    }
    
    public void setLogId(UUID logId) {
        this.logId = logId;
    }
    
    public UUID getRunId() {
        return runId;
    }
    
    public void setRunId(UUID runId) {
        this.runId = runId;
    }
    
    public String getLogLevel() {
        return logLevel;
    }
    
    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getSource() {
        return source;
    }
    
    public void setSource(String source) {
        this.source = source;
    }
}
