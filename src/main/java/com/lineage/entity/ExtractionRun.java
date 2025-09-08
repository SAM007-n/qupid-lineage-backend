package com.lineage.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "extraction_runs")
public class ExtractionRun {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "run_id")
    private UUID runId;

    @Column(name = "repository_url", nullable = false, length = 500)
    private String repositoryUrl;

    @Column(name = "branch", nullable = false, length = 100)
    private String branch = "main";

    @Column(name = "commit_hash", length = 100)
    private String commitHash;

    @Column(name = "commit_timestamp")
    @JsonDeserialize(using = FlexibleLocalDateTimeDeserializer.class)
    private LocalDateTime commitTimestamp;

    @Enumerated(EnumType.STRING)
    @Column(name = "run_mode", nullable = false, length = 20)
    private RunMode runMode = RunMode.FULL;

    @Enumerated(EnumType.STRING)
    @Column(name = "phase", nullable = false, length = 20)
    private ExtractionPhase phase = ExtractionPhase.STARTED;

    @Column(name = "triggered_by", nullable = false, length = 100)
    private String triggeredBy;

    @Column(name = "extractor_version", nullable = false, length = 50)
    private String extractorVersion;

    @Column(name = "started_at", nullable = false)
    @JsonDeserialize(using = FlexibleLocalDateTimeDeserializer.class)
    private LocalDateTime startedAt = LocalDateTime.now();

    @Column(name = "finished_at")
    @JsonDeserialize(using = FlexibleLocalDateTimeDeserializer.class)
    private LocalDateTime finishedAt;

    @Column(name = "stats", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private Map<String, Object> stats;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    @JsonDeserialize(using = FlexibleLocalDateTimeDeserializer.class)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    @JsonDeserialize(using = FlexibleLocalDateTimeDeserializer.class)
    private LocalDateTime updatedAt;

    // Constructors
    public ExtractionRun() {}

    public ExtractionRun(String repositoryUrl, String branch, String triggeredBy, String extractorVersion) {
        this.repositoryUrl = repositoryUrl;
        this.branch = branch;
        this.triggeredBy = triggeredBy;
        this.extractorVersion = extractorVersion;
    }

    // Getters and Setters
    public UUID getRunId() {
        return runId;
    }

    public void setRunId(UUID runId) {
        this.runId = runId;
    }

    public String getRepositoryUrl() {
        return repositoryUrl;
    }

    public void setRepositoryUrl(String repositoryUrl) {
        this.repositoryUrl = repositoryUrl;
    }

    public String getBranch() {
        return branch;
    }

    public void setBranch(String branch) {
        this.branch = branch;
    }

    public String getCommitHash() {
        return commitHash;
    }

    public void setCommitHash(String commitHash) {
        this.commitHash = commitHash;
    }

    public LocalDateTime getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(LocalDateTime commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public RunMode getRunMode() {
        return runMode;
    }

    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    public ExtractionPhase getPhase() {
        return phase;
    }

    public void setPhase(ExtractionPhase phase) {
        this.phase = phase;
    }

    public String getTriggeredBy() {
        return triggeredBy;
    }

    public void setTriggeredBy(String triggeredBy) {
        this.triggeredBy = triggeredBy;
    }

    public String getExtractorVersion() {
        return extractorVersion;
    }

    public void setExtractorVersion(String extractorVersion) {
        this.extractorVersion = extractorVersion;
    }

    public LocalDateTime getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(LocalDateTime startedAt) {
        this.startedAt = startedAt;
    }

    public LocalDateTime getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(LocalDateTime finishedAt) {
        this.finishedAt = finishedAt;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public void setStats(Map<String, Object> stats) {
        this.stats = stats;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    // Enums
    public enum RunMode {
        FULL, INCREMENTAL
    }

    public enum ExtractionPhase {
        STARTED, COMPLETED, FAILED
    }
} 