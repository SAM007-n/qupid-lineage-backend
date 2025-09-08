package com.lineage.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class RepositoryScanRequest {

    @NotBlank(message = "Repository URL is required")
    private String repositoryUrl;

    @NotBlank(message = "Branch is required")
    private String branch = "main";

    private String commitHash;

    @NotNull(message = "Run mode is required")
    private RunMode runMode = RunMode.FULL;

    @NotBlank(message = "Triggered by is required")
    private String triggeredBy = "api";

    @NotBlank(message = "GitHub token is required")
    private String gitHubToken;

    // Constructors
    public RepositoryScanRequest() {}

    public RepositoryScanRequest(String repositoryUrl, String branch) {
        this.repositoryUrl = repositoryUrl;
        this.branch = branch;
    }

    // Getters and Setters
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

    public RunMode getRunMode() {
        return runMode;
    }

    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    public String getTriggeredBy() {
        return triggeredBy;
    }

    public void setTriggeredBy(String triggeredBy) {
        this.triggeredBy = triggeredBy;
    }

    public String getGitHubToken() {
        return gitHubToken;
    }

    public void setGitHubToken(String gitHubToken) {
        this.gitHubToken = gitHubToken;
    }

    // Enums
    public enum RunMode {
        FULL, INCREMENTAL
    }
} 