package com.lineage.service;

import com.lineage.dto.SettingsDtos.GroqModelsResponse;
import com.lineage.entity.LlmConfig;
import com.lineage.repository.LlmConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.List;
import java.util.Optional;

@Service
public class GroqService {

    private static final Logger logger = LoggerFactory.getLogger(GroqService.class);
    private static final String GROQ_API_BASE_URL = "https://api.groq.com/openai/v1";
    private static final String MODELS_ENDPOINT = "/models";

    @Autowired
    private LlmConfigRepository llmConfigRepository;

    private final RestTemplate restTemplate;

    public GroqService() {
        this.restTemplate = new RestTemplate();
    }

    /**
     * Get the current Groq API key from the default LLM config
     */
    public String getGroqApiKey() {
        Optional<LlmConfig> defaultConfig = llmConfigRepository.findDefaultByProvider("groq");
        return defaultConfig.map(LlmConfig::getApiKey).orElse(null);
    }

    /**
     * Set the Groq API key in the default LLM config
     */
    public boolean setGroqApiKey(String groqKey) {
        try {
            Optional<LlmConfig> existingConfig = llmConfigRepository.findByProviderAndName("groq", "default");
            LlmConfig config;
            
            if (existingConfig.isPresent()) {
                // Update existing config
                config = existingConfig.get();
            } else {
                // Create new config
                config = new LlmConfig("groq", "default", groqKey);
                config.setDescription("Default Groq configuration");
            }
            
            config.setApiKey(groqKey);
            config.setIsActive(true);
            llmConfigRepository.save(config);
            return true;
        } catch (Exception e) {
            logger.error("Error setting Groq API key: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Delete the Groq API key from the default LLM config
     */
    public boolean deleteGroqApiKey() {
        try {
            Optional<LlmConfig> config = llmConfigRepository.findByProviderAndName("groq", "default");
            if (config.isPresent()) {
                LlmConfig llmConfig = config.get();
                llmConfig.setApiKey(null);
                llmConfig.setIsActive(false);
                llmConfigRepository.save(llmConfig);
            }
            return true;
        } catch (Exception e) {
            logger.error("Error deleting Groq API key: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Check if Groq API key is set
     */
    public boolean isGroqApiKeySet() {
        String apiKey = getGroqApiKey();
        return apiKey != null && !apiKey.trim().isEmpty();
    }

    /**
     * Get masked Groq API key for display
     */
    public String getMaskedGroqApiKey() {
        String apiKey = getGroqApiKey();
        if (apiKey == null || apiKey.length() < 6) {
            return "***";
        }
        return apiKey.substring(0, 3) + "***" + apiKey.substring(apiKey.length() - 3);
    }

    /**
     * Fetch available models from Groq API
     */
    public GroqModelsResponse getModels() {
        String apiKey = getGroqApiKey();
        if (apiKey == null || apiKey.trim().isEmpty()) {
            throw new IllegalStateException("Groq API key is not set");
        }

        try {
            String url = GROQ_API_BASE_URL + MODELS_ENDPOINT;
            
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + apiKey);
            headers.set("Content-Type", "application/json");
            
            HttpEntity<String> entity = new HttpEntity<>(headers);
            
            logger.info("Fetching models from Groq API");
            ResponseEntity<GroqModelsResponse> response = restTemplate.exchange(
                url, 
                HttpMethod.GET, 
                entity, 
                GroqModelsResponse.class
            );
            
            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                logger.info("Successfully fetched {} models from Groq API", 
                    response.getBody().getData() != null ? response.getBody().getData().size() : 0);
                return response.getBody();
            } else {
                throw new RuntimeException("Failed to fetch models from Groq API");
            }
            
        } catch (HttpClientErrorException e) {
            logger.error("Client error when fetching models from Groq API: {} - {}", 
                e.getStatusCode(), e.getResponseBodyAsString());
            if (e.getStatusCode() == HttpStatus.UNAUTHORIZED) {
                throw new IllegalStateException("Invalid Groq API key");
            }
            throw new RuntimeException("Failed to fetch models from Groq API: " + e.getMessage());
        } catch (HttpServerErrorException e) {
            logger.error("Server error when fetching models from Groq API: {} - {}", 
                e.getStatusCode(), e.getResponseBodyAsString());
            throw new RuntimeException("Groq API server error: " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error when fetching models from Groq API: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to fetch models from Groq API: " + e.getMessage());
        }
    }
}
