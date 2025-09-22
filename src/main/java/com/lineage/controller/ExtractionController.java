package com.lineage.controller;

import com.lineage.dto.RepositoryScanRequest;
import com.lineage.dto.WebhookEvent;
import com.lineage.entity.ExtractionRun;
import com.lineage.entity.ExtractionLog;
import com.lineage.entity.JobStatus;
import com.lineage.repository.ExtractionLogRepository;
import com.lineage.service.ExtractionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

import java.util.Map;
import java.util.UUID;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/extraction")
@CrossOrigin(origins = "*")
public class ExtractionController {

    private static final Logger logger = LoggerFactory.getLogger(ExtractionController.class);

    @Autowired
    private ExtractionService extractionService;
    
    @Autowired
    private ExtractionService dockerService;

    
    @Autowired
    private ExtractionLogRepository logRepository;

    /**
     * Start a new extraction run
     */
    @PostMapping("/start")
    public ResponseEntity<ExtractionRun> startExtraction(@Valid @RequestBody RepositoryScanRequest request) {
        try {
            ExtractionRun extractionRun = extractionService.startExtraction(request);
            return ResponseEntity.ok(extractionRun);
        } catch (Exception e) {
            logger.error("Error starting extraction: {}", e.getMessage(), e);
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Get extraction run status by ID
     */
    @GetMapping("/status/{runId}")
    public ResponseEntity<Map<String, Object>> getRunStatus(@PathVariable String runId) {
        try {
            UUID uuid = UUID.fromString(runId);
            Map<String, Object> status = extractionService.getRunStatus(uuid);
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Get all extraction runs
     */
    @GetMapping("/runs")
    public ResponseEntity<Map<String, Object>> getAllRuns(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        try {
            Map<String, Object> runs = extractionService.getAllRuns(page, size);
            return ResponseEntity.ok(runs);
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Get job status for a specific run
     */
    @GetMapping("/job-status/{runId}")
    public ResponseEntity<JobStatus> getJobStatus(@PathVariable String runId) {
        try {
            UUID uuid = UUID.fromString(runId);
            JobStatus jobStatus = extractionService.getJobStatus(uuid);
            return ResponseEntity.ok(jobStatus);
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Cancel an extraction run
     */
    @PostMapping("/cancel/{runId}")
    public ResponseEntity<Void> cancelExtraction(@PathVariable String runId) {
        try {
            UUID uuid = UUID.fromString(runId);
            extractionService.cancelExtraction(uuid);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }



    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "healthy"));
    }

    /**
     * Handle webhook events from extraction pods
     */
    @PostMapping("/webhook")
    public ResponseEntity<Void> handleWebhook(@RequestBody WebhookEvent event) {
        try {
            extractionService.handleWebhookEvent(event);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Get logs for a specific extraction run
     */
    @GetMapping("/logs/{runId}")
    public ResponseEntity<Map<String, Object>> getExtractionLogs(
            @PathVariable String runId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "100") int size,
            @RequestParam(required = false) String since) {
        try {
            UUID uuid = UUID.fromString(runId);
            
            List<ExtractionLog> logs;
            if (since != null && !since.isEmpty()) {
                // Get logs since timestamp for real-time updates
                LocalDateTime sinceTime = LocalDateTime.parse(since);
                logs = logRepository.findByRunIdAndTimestampAfterOrderByTimestampAsc(uuid, sinceTime);
            } else {
                // Get recent logs with pagination
                Pageable pageable = PageRequest.of(page, size);
                logs = logRepository.findRecentLogsByRunId(uuid, pageable);
            }
            
            Map<String, Object> response = Map.of(
                "runId", runId,
                "logs", logs,
                "total", logRepository.countByRunId(uuid),
                "page", page,
                "size", size
            );
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error fetching logs for run {}: {}", runId, e.getMessage());
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Stop an extraction run
     */
    @PostMapping("/stop/{runId}")
    public ResponseEntity<Map<String, Object>> stopExtraction(@PathVariable String runId) {
        try {
            UUID uuid = UUID.fromString(runId);
            
            // Stop the Docker container
            boolean stopped = dockerService.stopContainer(uuid);
            
            if (stopped) {
                // Update the extraction run status
                extractionService.cancelExtraction(uuid);
                
                Map<String, Object> response = Map.of(
                    "success", true,
                    "message", "Extraction stopped successfully",
                    "runId", runId
                );
                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = Map.of(
                    "success", false,
                    "message", "Failed to stop extraction",
                    "runId", runId
                );
                return ResponseEntity.badRequest().body(response);
            }
        } catch (Exception e) {
            logger.error("Error stopping extraction {}: {}", runId, e.getMessage());
            Map<String, Object> response = Map.of(
                "success", false,
                "message", "Error stopping extraction: " + e.getMessage(),
                "runId", runId
            );
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Pause an extraction run
     */
    @PostMapping("/pause/{runId}")
    public ResponseEntity<Map<String, Object>> pauseExtraction(@PathVariable String runId) {
        try {
            UUID uuid = UUID.fromString(runId);
            
            boolean paused = dockerService.pauseContainer(uuid);
            
            Map<String, Object> response = Map.of(
                "success", paused,
                "message", paused ? "Extraction paused successfully" : "Failed to pause extraction",
                "runId", runId
            );
            
            return paused ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
        } catch (Exception e) {
            logger.error("Error pausing extraction {}: {}", runId, e.getMessage());
            Map<String, Object> response = Map.of(
                "success", false,
                "message", "Error pausing extraction: " + e.getMessage(),
                "runId", runId
            );
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Resume a paused extraction run
     */
    @PostMapping("/resume/{runId}")
    public ResponseEntity<Map<String, Object>> resumeExtraction(@PathVariable String runId) {
        try {
            UUID uuid = UUID.fromString(runId);
            
            boolean resumed = dockerService.resumeContainer(uuid);
            
            Map<String, Object> response = Map.of(
                "success", resumed,
                "message", resumed ? "Extraction resumed successfully" : "Failed to resume extraction",
                "runId", runId
            );
            
            return resumed ? ResponseEntity.ok(response) : ResponseEntity.badRequest().body(response);
        } catch (Exception e) {
            logger.error("Error resuming extraction {}: {}", runId, e.getMessage());
            Map<String, Object> response = Map.of(
                "success", false,
                "message", "Error resuming extraction: " + e.getMessage(),
                "runId", runId
            );
            return ResponseEntity.badRequest().body(response);
        }
    }

    /**
     * Get container status
     */
    @GetMapping("/container-status/{runId}")
    public ResponseEntity<Map<String, Object>> getContainerStatus(@PathVariable String runId) {
        try {
            UUID uuid = UUID.fromString(runId);
            
            boolean isRunning = dockerService.isContainerRunning(uuid);
            
            Map<String, Object> response = Map.of(
                "runId", runId,
                "isRunning", isRunning,
                "status", isRunning ? "running" : "stopped"
            );
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }
}
