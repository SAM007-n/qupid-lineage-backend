package com.lineage.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "llm_configs", indexes = {
        @Index(name = "idx_llm_provider", columnList = "provider"),
        @Index(name = "idx_llm_name", columnList = "name")
})
public class LlmConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "llm_config_id")
    private UUID id;

    @Column(name = "provider", length = 64, nullable = false)
    private String provider; // e.g., "groq", "openai", "anthropic"

    @Column(name = "name", length = 128, nullable = false)
    private String name; // e.g., "default", "production", "development"

    @Column(name = "api_key", length = 4096)
    private String apiKey;

    @Column(name = "base_url", length = 512)
    private String baseUrl;

    @Column(name = "model", length = 128)
    private String model; // default model to use

    @Column(name = "is_active", nullable = false)
    private Boolean isActive = true;

    @Column(name = "description", length = 512)
    private String description;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    // Constructors
    public LlmConfig() {}

    public LlmConfig(String provider, String name, String apiKey) {
        this.provider = provider;
        this.name = name;
        this.apiKey = apiKey;
    }

    // Getters and Setters
    public UUID getId() { 
        return id; 
    }
    
    public void setId(UUID id) { 
        this.id = id; 
    }
    
    public String getProvider() { 
        return provider; 
    }
    
    public void setProvider(String provider) { 
        this.provider = provider; 
    }
    
    public String getName() { 
        return name; 
    }
    
    public void setName(String name) { 
        this.name = name; 
    }
    
    public String getApiKey() { 
        return apiKey; 
    }
    
    public void setApiKey(String apiKey) { 
        this.apiKey = apiKey; 
    }
    
    public String getBaseUrl() { 
        return baseUrl; 
    }
    
    public void setBaseUrl(String baseUrl) { 
        this.baseUrl = baseUrl; 
    }
    
    public String getModel() { 
        return model; 
    }
    
    public void setModel(String model) { 
        this.model = model; 
    }
    
    public Boolean getIsActive() { 
        return isActive; 
    }
    
    public void setIsActive(Boolean isActive) { 
        this.isActive = isActive; 
    }
    
    public String getDescription() { 
        return description; 
    }
    
    public void setDescription(String description) { 
        this.description = description; 
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
}
