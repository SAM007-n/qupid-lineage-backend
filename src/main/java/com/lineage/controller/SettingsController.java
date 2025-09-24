package com.lineage.controller;

import com.lineage.dto.SettingsDtos.*;
import com.lineage.service.GroqService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/settings")
@CrossOrigin(origins = "*")
public class SettingsController {

    private static final Logger logger = LoggerFactory.getLogger(SettingsController.class);

    @Autowired
    private GroqService groqService;

    /**
     * Set Groq API key
     * POST /settings/groq-key
     */
    @PostMapping("/groq-key")
    public ResponseEntity<GroqKeyResponse> setGroqKey(@Valid @RequestBody GroqKeyRequest request) {
        try {
            if (request.getGroqKey() == null || request.getGroqKey().trim().isEmpty()) {
                return ResponseEntity.badRequest().build();
            }

            boolean success = groqService.setGroqApiKey(request.getGroqKey().trim());
            if (success) {
                GroqKeyResponse response = new GroqKeyResponse();
                response.setGroqKeyMasked(groqService.getMaskedGroqApiKey());
                response.setSet(true);
                logger.info("Groq API key set successfully");
                return ResponseEntity.ok(response);
            } else {
                logger.error("Failed to set Groq API key");
                return ResponseEntity.internalServerError().build();
            }
        } catch (Exception e) {
            logger.error("Error setting Groq API key: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get Groq API key status (masked)
     * GET /settings/groq-key
     */
    @GetMapping("/groq-key")
    public ResponseEntity<GroqKeyResponse> getGroqKey() {
        try {
            GroqKeyResponse response = new GroqKeyResponse();
            response.setGroqKeyMasked(groqService.getMaskedGroqApiKey());
            response.setSet(groqService.isGroqApiKeySet());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error getting Groq API key status: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Delete Groq API key
     * DELETE /settings/groq-key
     */
    @DeleteMapping("/groq-key")
    public ResponseEntity<Void> deleteGroqKey() {
        try {
            boolean success = groqService.deleteGroqApiKey();
            if (success) {
                logger.info("Groq API key deleted successfully");
                return ResponseEntity.ok().build();
            } else {
                logger.error("Failed to delete Groq API key");
                return ResponseEntity.internalServerError().build();
            }
        } catch (Exception e) {
            logger.error("Error deleting Groq API key: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }

    /**
     * Get available Groq models
     * GET /settings/models
     */
    @GetMapping("/models")
    public ResponseEntity<?> getModels() {
        try {
            if (!groqService.isGroqApiKeySet()) {
                ApiErrorResponse errorResponse = new ApiErrorResponse();
                errorResponse.setError("API_KEY_NOT_SET");
                errorResponse.setMessage("Groq API key is not configured. Please set the API key first.");
                return ResponseEntity.badRequest().body(errorResponse);
            }

            GroqModelsResponse models = groqService.getModels();
            logger.info("Successfully retrieved {} models from Groq API", 
                models.getData() != null ? models.getData().size() : 0);
            return ResponseEntity.ok(models);
        } catch (IllegalStateException e) {
            logger.warn("Groq API key not set: {}", e.getMessage());
            ApiErrorResponse errorResponse = new ApiErrorResponse();
            errorResponse.setError("API_KEY_NOT_SET");
            errorResponse.setMessage("Groq API key is not configured. Please set the API key first.");
            return ResponseEntity.badRequest().body(errorResponse);
        } catch (Exception e) {
            logger.error("Error fetching models from Groq API: {}", e.getMessage(), e);
            ApiErrorResponse errorResponse = new ApiErrorResponse();
            errorResponse.setError("API_ERROR");
            errorResponse.setMessage("Failed to fetch models from Groq API: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
}
