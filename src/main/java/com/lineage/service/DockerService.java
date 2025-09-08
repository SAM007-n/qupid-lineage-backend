package com.lineage.service;

import com.lineage.entity.ExtractionLog;
import com.lineage.repository.ExtractionLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DockerService {
    
    private static final Logger logger = LoggerFactory.getLogger(DockerService.class);
    
    @Value("${app.docker.image-name:qupid-lineage}")
    private String dockerImageName;
    
    @Value("${app.docker.backend-url:http://host.docker.internal:8080/api}")
    private String backendUrl;
    
    @Autowired
    private ExtractionLogRepository logRepository;
    
    // Track running processes for control operations
    private final Map<UUID, Process> runningProcesses = new ConcurrentHashMap<>();
    
    /**
     * Launch a Docker container for extraction
     */
    public CompletableFuture<Void> launchExtractionContainer(
            UUID runId, 
            String repositoryUrl, 
            String branch, 
            String githubToken,
            String groqApiKey,
            String runMode) {
        
        return CompletableFuture.runAsync(() -> {
            try {
                String containerName = "extraction-" + runId.toString().substring(0, 8);
                
                // Build the docker run command
                String[] command = {
                    "docker", "run", "--rm",
                    "-e", "GROQ_API_KEY=" + groqApiKey,
                    "-e", "GITHUB_TOKEN=" + githubToken,
                    "-v", System.getProperty("user.dir") + "/lineage_output:/app/lineage_output",
                    "--name", containerName,
                    dockerImageName,
                    "--repo-url", repositoryUrl,
                    "--github-token", githubToken,
                    "--branch", branch,
                    "--backend-url", backendUrl,
                    "--run-id", runId.toString(),
                    "--run-mode", runMode
                };
                
                logger.info("Launching Docker container for run {}: {}", runId, String.join(" ", command));
                
                ProcessBuilder processBuilder = new ProcessBuilder(command);
                processBuilder.redirectErrorStream(true);
                
                Process process = processBuilder.start();
                
                // Track the process for control operations
                runningProcesses.put(runId, process);
                
                // Log container startup
                saveLog(runId, "Docker container started: " + containerName, "INFO");
                
                // Read the output and store logs
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        logger.info("Docker [{}]: {}", runId, line);
                        // Store log in database for UI display
                        saveLog(runId, line, "INFO");
                    }
                }
                
                int exitCode = process.waitFor();
                logger.info("Docker container for run {} exited with code: {}", runId, exitCode);
                
                // Log container completion and remove from tracking
                saveLog(runId, "Docker container exited with code: " + exitCode, 
                    exitCode == 0 ? "INFO" : "ERROR");
                runningProcesses.remove(runId);
                
            } catch (IOException | InterruptedException e) {
                logger.error("Error launching Docker container for run {}: {}", runId, e.getMessage(), e);
                saveLog(runId, "Error launching Docker container: " + e.getMessage(), "ERROR");
                runningProcesses.remove(runId);
                Thread.currentThread().interrupt();
            }
        });
    }
    
    /**
     * Stop a running Docker container
     */
    public boolean stopContainer(UUID runId) {
        try {
            // First try to stop the process gracefully
            Process process = runningProcesses.get(runId);
            if (process != null && process.isAlive()) {
                saveLog(runId, "Stopping Docker container gracefully...", "INFO");
                process.destroy();
                
                // Wait a bit for graceful shutdown
                if (!process.waitFor(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    // Force kill if graceful shutdown failed
                    saveLog(runId, "Force killing Docker container...", "WARN");
                    process.destroyForcibly();
                }
                runningProcesses.remove(runId);
                saveLog(runId, "Docker container stopped by admin", "INFO");
                return true;
            }
            
            // Fallback to docker stop command
            String containerName = "extraction-" + runId.toString().substring(0, 8);
            String[] command = {"docker", "stop", containerName};
            
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            Process stopProcess = processBuilder.start();
            
            int exitCode = stopProcess.waitFor();
            logger.info("Stopped Docker container {} with exit code: {}", containerName, exitCode);
            saveLog(runId, "Docker container stopped via docker command", "INFO");
            
            return exitCode == 0;
            
        } catch (IOException | InterruptedException e) {
            logger.error("Error stopping Docker container for run {}: {}", runId, e.getMessage(), e);
            saveLog(runId, "Error stopping Docker container: " + e.getMessage(), "ERROR");
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    /**
     * Pause a running Docker container (sends SIGSTOP to pause processing)
     */
    public boolean pauseContainer(UUID runId) {
        try {
            String containerName = "extraction-" + runId.toString().substring(0, 8);
            String[] command = {"docker", "pause", containerName};
            
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            Process process = processBuilder.start();
            
            int exitCode = process.waitFor();
            logger.info("Paused Docker container {} with exit code: {}", containerName, exitCode);
            saveLog(runId, "Docker container paused by admin", "INFO");
            
            return exitCode == 0;
            
        } catch (IOException | InterruptedException e) {
            logger.error("Error pausing Docker container for run {}: {}", runId, e.getMessage(), e);
            saveLog(runId, "Error pausing Docker container: " + e.getMessage(), "ERROR");
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    /**
     * Resume a paused Docker container
     */
    public boolean resumeContainer(UUID runId) {
        try {
            String containerName = "extraction-" + runId.toString().substring(0, 8);
            String[] command = {"docker", "unpause", containerName};
            
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            Process process = processBuilder.start();
            
            int exitCode = process.waitFor();
            logger.info("Resumed Docker container {} with exit code: {}", containerName, exitCode);
            saveLog(runId, "Docker container resumed by admin", "INFO");
            
            return exitCode == 0;
            
        } catch (IOException | InterruptedException e) {
            logger.error("Error resuming Docker container for run {}: {}", runId, e.getMessage(), e);
            saveLog(runId, "Error resuming Docker container: " + e.getMessage(), "ERROR");
            Thread.currentThread().interrupt();
            return false;
        }
    }
    
    /**
     * Check if a container is currently running
     */
    public boolean isContainerRunning(UUID runId) {
        Process process = runningProcesses.get(runId);
        return process != null && process.isAlive();
    }
    
    /**
     * Save a log entry to the database
     */
    private void saveLog(UUID runId, String message, String logLevel) {
        try {
            ExtractionLog log = new ExtractionLog(runId, message, logLevel, "docker");
            logRepository.save(log);
        } catch (Exception e) {
            logger.warn("Failed to save log for run {}: {}", runId, e.getMessage());
        }
    }
}
