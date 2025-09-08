package com.lineage.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "job_status")
public class JobStatus {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "job_id")
    private UUID jobId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "run_id", nullable = false)
    @JsonIgnore
    private ExtractionRun extractionRun;

    @Column(name = "pod_id", length = 100)
    private String podId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private JobStatusEnum status = JobStatusEnum.RUNNING;

    @Column(name = "total_files", nullable = false)
    private Integer totalFiles = 0;

    @Column(name = "processed_files", nullable = false)
    private Integer processedFiles = 0;

    @Column(name = "succeeded_files", nullable = false)
    private Integer succeededFiles = 0;

    @Column(name = "failed_files", nullable = false)
    private Integer failedFiles = 0;

    @Column(name = "current_file", length = 500)
    private String currentFile;

    @Column(name = "processing_speed", precision = 10, scale = 2)
    private BigDecimal processingSpeed;

    @Column(name = "estimated_time_remaining", precision = 10, scale = 2)
    private BigDecimal estimatedTimeRemaining;

    @Column(name = "current_phase", length = 100)
    private String currentPhase;

    @Column(name = "error_count", nullable = false)
    private Integer errorCount = 0;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @Column(name = "last_updated", nullable = false)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime lastUpdated = LocalDateTime.now();

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime createdAt;

    // Constructors
    public JobStatus() {}

    public JobStatus(ExtractionRun extractionRun) {
        this.extractionRun = extractionRun;
    }

    // Getters and Setters
    public UUID getJobId() {
        return jobId;
    }

    public void setJobId(UUID jobId) {
        this.jobId = jobId;
    }

    public ExtractionRun getExtractionRun() {
        return extractionRun;
    }

    public void setExtractionRun(ExtractionRun extractionRun) {
        this.extractionRun = extractionRun;
    }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    public JobStatusEnum getStatus() {
        return status;
    }

    public void setStatus(JobStatusEnum status) {
        this.status = status;
    }

    public Integer getTotalFiles() {
        return totalFiles;
    }

    public void setTotalFiles(Integer totalFiles) {
        this.totalFiles = totalFiles;
    }

    public Integer getProcessedFiles() {
        return processedFiles;
    }

    public void setProcessedFiles(Integer processedFiles) {
        this.processedFiles = processedFiles;
    }

    public Integer getSucceededFiles() {
        return succeededFiles;
    }

    public void setSucceededFiles(Integer succeededFiles) {
        this.succeededFiles = succeededFiles;
    }

    public Integer getFailedFiles() {
        return failedFiles;
    }

    public void setFailedFiles(Integer failedFiles) {
        this.failedFiles = failedFiles;
    }

    public String getCurrentFile() {
        return currentFile;
    }

    public void setCurrentFile(String currentFile) {
        this.currentFile = currentFile;
    }

    public BigDecimal getProcessingSpeed() {
        return processingSpeed;
    }

    public void setProcessingSpeed(BigDecimal processingSpeed) {
        this.processingSpeed = processingSpeed;
    }

    public BigDecimal getEstimatedTimeRemaining() {
        return estimatedTimeRemaining;
    }

    public void setEstimatedTimeRemaining(BigDecimal estimatedTimeRemaining) {
        this.estimatedTimeRemaining = estimatedTimeRemaining;
    }

    public String getCurrentPhase() {
        return currentPhase;
    }

    public void setCurrentPhase(String currentPhase) {
        this.currentPhase = currentPhase;
    }

    public Integer getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(Integer errorCount) {
        this.errorCount = errorCount;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public LocalDateTime getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(LocalDateTime lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    // Enums
    public enum JobStatusEnum {
        RUNNING, COMPLETED, FAILED
    }
} 