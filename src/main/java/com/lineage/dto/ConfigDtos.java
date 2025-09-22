package com.lineage.dto;

import java.time.LocalDateTime;
import java.util.UUID;

public class ConfigDtos {

    public static class ConfigDto {
        private UUID id;
        private String profile;
        private String repositoryUrl;
        private String branch;
        private String githubTokenMasked;
        private String groqKeyMasked;
        private LocalDateTime createdAt;
        private LocalDateTime updatedAt;
        public UUID getId() { return id; }
        public void setId(UUID id) { this.id = id; }
        public String getProfile() { return profile; }
        public void setProfile(String profile) { this.profile = profile; }
        public String getRepositoryUrl() { return repositoryUrl; }
        public void setRepositoryUrl(String repositoryUrl) { this.repositoryUrl = repositoryUrl; }
        public String getBranch() { return branch; }
        public void setBranch(String branch) { this.branch = branch; }
        public String getGithubTokenMasked() { return githubTokenMasked; }
        public void setGithubTokenMasked(String githubTokenMasked) { this.githubTokenMasked = githubTokenMasked; }
        public String getGroqKeyMasked() { return groqKeyMasked; }
        public void setGroqKeyMasked(String groqKeyMasked) { this.groqKeyMasked = groqKeyMasked; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
        public LocalDateTime getUpdatedAt() { return updatedAt; }
        public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
    }

    public static class ConfigCreateRequest {
        private String profile;
        private String repositoryUrl;
        private String branch;
        private String githubToken;
        private String groqKey;
        public String getProfile() { return profile; }
        public void setProfile(String profile) { this.profile = profile; }
        public String getRepositoryUrl() { return repositoryUrl; }
        public void setRepositoryUrl(String repositoryUrl) { this.repositoryUrl = repositoryUrl; }
        public String getBranch() { return branch; }
        public void setBranch(String branch) { this.branch = branch; }
        public String getGithubToken() { return githubToken; }
        public void setGithubToken(String githubToken) { this.githubToken = githubToken; }
        public String getGroqKey() { return groqKey; }
        public void setGroqKey(String groqKey) { this.groqKey = groqKey; }
    }

    public static class ConfigUpdateRequest {
        private String profile;
        private String repositoryUrl;
        private String branch;
        private String githubToken;
        private String groqKey;
        public String getProfile() { return profile; }
        public void setProfile(String profile) { this.profile = profile; }
        public String getRepositoryUrl() { return repositoryUrl; }
        public void setRepositoryUrl(String repositoryUrl) { this.repositoryUrl = repositoryUrl; }
        public String getBranch() { return branch; }
        public void setBranch(String branch) { this.branch = branch; }
        public String getGithubToken() { return githubToken; }
        public void setGithubToken(String githubToken) { this.githubToken = githubToken; }
        public String getGroqKey() { return groqKey; }
        public void setGroqKey(String groqKey) { this.groqKey = groqKey; }
    }
}


