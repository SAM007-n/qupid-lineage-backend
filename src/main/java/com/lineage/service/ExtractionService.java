package com.lineage.service;

import com.lineage.dto.RepositoryScanRequest;
import com.lineage.dto.WebhookEvent;
import com.lineage.entity.*;
import com.lineage.repository.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;

@Service
@Transactional
public class ExtractionService {

    private static final Logger logger = LoggerFactory.getLogger(ExtractionService.class);

    @Autowired
    private ExtractionRunRepository extractionRunRepository;

    @Autowired
    private FileRepository fileRepository;

    // Legacy repositories removed - using normalized schema only

    @Autowired
    private AssetRepository assetRepository;

    @Autowired
    private AssetColumnRepository assetColumnRepository;

    @Autowired
    private AssetFileRepository assetFileRepository;

    @Autowired
    private LineageRepository lineageRepository;

    @Autowired
    private JobStatusRepository jobStatusRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    // Docker/orchestration settings (moved from DockerService)
    @Value("${app.docker.image-name:sql-dependency-extractor}")
    private String dockerImageName;

    @Value("${app.docker.backend-url:http://host.docker.internal:8080/api}")
    private String backendUrl;

    // Removed legacy processing service
    @Autowired
    private ExtractionLogRepository logRepository;

    // Track running processes for control operations
    private final java.util.concurrent.ConcurrentHashMap<UUID, Process> runningProcesses = new java.util.concurrent.ConcurrentHashMap<>();

    private static final String EXTRACTOR_VERSION = "1.0.0";

    /**
     * Start a new extraction run
     */
    public ExtractionRun startExtraction(RepositoryScanRequest request) {
        logger.info("Starting extraction for repository: {}", request.getRepositoryUrl());

        // Create extraction run
        ExtractionRun extractionRun = new ExtractionRun(
            request.getRepositoryUrl(),
            request.getBranch(),
            request.getTriggeredBy(),
            EXTRACTOR_VERSION
        );

        extractionRun.setRunMode(ExtractionRun.RunMode.valueOf(request.getRunMode().name()));
        extractionRun.setPhase(ExtractionRun.ExtractionPhase.STARTED);

        if (request.getCommitHash() != null) {
            extractionRun.setCommitHash(request.getCommitHash());
        }

        extractionRun = extractionRunRepository.save(extractionRun);
        logger.info("Created extraction run with ID: {}", extractionRun.getRunId());

        // Create initial job status
        JobStatus jobStatus = new JobStatus(extractionRun);
        jobStatus.setStatus(JobStatus.JobStatusEnum.RUNNING);
        jobStatus.setCurrentPhase("initialized");
        jobStatusRepository.save(jobStatus);

        // Launch Docker container for extraction
        final String groqApiKey = System.getenv("GROQ_API_KEY");
        if (groqApiKey == null || groqApiKey.isEmpty()) {
            logger.error("GROQ_API_KEY environment variable is not set");
            throw new RuntimeException("GROQ_API_KEY environment variable is required");
        }

        final UUID runId = extractionRun.getRunId();
        
        // Launch the Docker container asynchronously
        launchExtractionContainer(
                runId,
                request.getRepositoryUrl(),
                request.getBranch(),
                request.getGitHubToken(),
                groqApiKey,
                request.getRunMode().toString()
        ).exceptionally(throwable -> {
            logger.error("Failed to launch Docker container for run {}: {}", 
                runId, throwable.getMessage(), throwable);
            return null;
        });

        logger.info("Docker container launched for run ID: {}", extractionRun.getRunId());

        return extractionRun;
    }

    // =============================
    // Orchestration (moved from DockerService)
    // =============================

    public java.util.concurrent.CompletableFuture<Void> launchExtractionContainer(
            UUID runId,
            String repositoryUrl,
            String branch,
            String githubToken,
            String groqApiKey,
            String runMode) {

        return java.util.concurrent.CompletableFuture.runAsync(() -> {
            try {
                String containerName = "extraction-" + runId.toString().substring(0, 8);
                String[] command = {
                        "docker", "run", "--rm",
                        // Increase container resources
                        "--cpus", "4",
                        "--memory", "8g",
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
                runningProcesses.put(runId, process);
                saveLog(runId, "Docker container started: " + containerName, "INFO");

                try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        logger.info("Docker [{}]: {}", runId, line);
                        saveLog(runId, line, "INFO");
                    }
                }

                int exitCode = process.waitFor();
                logger.info("Docker container for run {} exited with code: {}", runId, exitCode);
                saveLog(runId, "Docker container exited with code: " + exitCode, exitCode == 0 ? "INFO" : "ERROR");
                runningProcesses.remove(runId);
            } catch (Exception e) {
                logger.error("Error launching Docker container for run {}: {}", runId, e.getMessage(), e);
                saveLog(runId, "Error launching Docker container: " + e.getMessage(), "ERROR");
                runningProcesses.remove(runId);
                Thread.currentThread().interrupt();
            }
        });
    }

    public boolean stopContainer(UUID runId) {
        try {
            Process process = runningProcesses.get(runId);
            if (process != null && process.isAlive()) {
                saveLog(runId, "Force killing Docker container process...", "INFO");
                process.destroyForcibly();
                runningProcesses.remove(runId);
                saveLog(runId, "Docker container killed by admin", "INFO");
                return true;
            }
            String containerName = "extraction-" + runId.toString().substring(0, 8);
            String[] command = {"docker", "kill", containerName};
            ProcessBuilder pb = new ProcessBuilder(command);
            Process killProcess = pb.start();
            int exitCode = killProcess.waitFor();
            logger.info("Killed Docker container {} with exit code: {}", containerName, exitCode);
            saveLog(runId, "Docker container killed via docker command", "INFO");
            return exitCode == 0;
        } catch (Exception e) {
            logger.error("Error killing Docker container for run {}: {}", runId, e.getMessage(), e);
            saveLog(runId, "Error killing Docker container: " + e.getMessage(), "ERROR");
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public boolean pauseContainer(UUID runId) {
        try {
            String containerName = "extraction-" + runId.toString().substring(0, 8);
            String[] command = {"docker", "pause", containerName};
            Process process = new ProcessBuilder(command).start();
            int exitCode = process.waitFor();
            logger.info("Paused Docker container {} with exit code: {}", containerName, exitCode);
            saveLog(runId, "Docker container paused by admin", "INFO");
            return exitCode == 0;
        } catch (Exception e) {
            logger.error("Error pausing Docker container for run {}: {}", runId, e.getMessage(), e);
            saveLog(runId, "Error pausing Docker container: " + e.getMessage(), "ERROR");
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public boolean resumeContainer(UUID runId) {
        try {
            String containerName = "extraction-" + runId.toString().substring(0, 8);
            String[] command = {"docker", "unpause", containerName};
            Process process = new ProcessBuilder(command).start();
            int exitCode = process.waitFor();
            logger.info("Resumed Docker container {} with exit code: {}", containerName, exitCode);
            saveLog(runId, "Docker container resumed by admin", "INFO");
            return exitCode == 0;
        } catch (Exception e) {
            logger.error("Error resuming Docker container for run {}: {}", runId, e.getMessage(), e);
            saveLog(runId, "Error resuming Docker container: " + e.getMessage(), "ERROR");
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public boolean isContainerRunning(UUID runId) {
        Process process = runningProcesses.get(runId);
        return process != null && process.isAlive();
    }

    private void saveLog(UUID runId, String message, String logLevel) {
        try {
            ExtractionLog log = new ExtractionLog(runId, message, logLevel, "docker");
            logRepository.save(log);
        } catch (Exception e) {
            logger.warn("Failed to save log for run {}: {}", runId, e.getMessage());
        }
    }

    /**
     * Get extraction run status
     */
    public Map<String, Object> getRunStatus(UUID runId) {
        logger.info("Getting status for run: {}", runId);

        ExtractionRun extractionRun = extractionRunRepository.findById(runId)
            .orElseThrow(() -> new RuntimeException("Extraction run not found: " + runId));

        // Get latest job status
        Optional<JobStatus> latestJobStatus = jobStatusRepository.findFirstByExtractionRunRunIdOrderByLastUpdatedDesc(runId);

        // Get file statistics
        long totalFiles = fileRepository.countByRunId(runId);
        long succeededFiles = fileRepository.countByRunIdAndStatus(runId, File.FileStatus.SUCCESS);
        long failedFiles = fileRepository.countByRunIdAndStatus(runId, File.FileStatus.FAILED);

        // Lineage statistics from normalized schema
        long totalEdges = lineageRepository.findByExtractionRunRunId(runId).size();
        long tableEdges = lineageRepository.findByExtractionRunRunId(runId).stream().filter(l -> "table_edge".equalsIgnoreCase(l.getEdgeType())).count();
        long columnEdges = lineageRepository.findByExtractionRunRunId(runId).stream().filter(l -> "column_edge".equalsIgnoreCase(l.getEdgeType())).count();

        Map<String, Object> status = new HashMap<>();
        status.put("runId", extractionRun.getRunId());
        status.put("repositoryUrl", extractionRun.getRepositoryUrl());
        status.put("branch", extractionRun.getBranch());
        status.put("phase", extractionRun.getPhase());
        status.put("startedAt", extractionRun.getStartedAt());
        status.put("finishedAt", extractionRun.getFinishedAt());
        status.put("triggeredBy", extractionRun.getTriggeredBy());
        status.put("extractorVersion", extractionRun.getExtractorVersion());

        // Job status
        if (latestJobStatus.isPresent()) {
            JobStatus jobStatus = latestJobStatus.get();
            status.put("currentPhase", jobStatus.getCurrentPhase());
            status.put("currentFile", jobStatus.getCurrentFile());
            status.put("processingSpeed", jobStatus.getProcessingSpeed());
            status.put("estimatedTimeRemaining", jobStatus.getEstimatedTimeRemaining());
            status.put("errorCount", jobStatus.getErrorCount());
            status.put("lastError", jobStatus.getLastError());
            status.put("lastUpdated", jobStatus.getLastUpdated());
        }

        // Statistics
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalFiles", totalFiles);
        stats.put("succeededFiles", succeededFiles);
        stats.put("failedFiles", failedFiles);
        stats.put("totalEdges", totalEdges);
        stats.put("tableEdges", tableEdges);
        stats.put("columnEdges", columnEdges);
        status.put("statistics", stats);

        return status;
    }

    /**
     * Get all extraction runs with pagination
     */
    public Map<String, Object> getAllRuns(int page, int size) {
        logger.info("Getting all extraction runs, page: {}, size: {}", page, size);

        Pageable pageable = PageRequest.of(page, size);
        Page<ExtractionRun> runsPage = extractionRunRepository.findAll(pageable);

        Map<String, Object> response = new HashMap<>();
        response.put("runs", runsPage.getContent());
        response.put("totalElements", runsPage.getTotalElements());
        response.put("totalPages", runsPage.getTotalPages());
        response.put("currentPage", page);
        response.put("pageSize", size);

        return response;
    }

    /**
     * Handle webhook events from extraction pods
     */
    public void handleWebhookEvent(WebhookEvent event) {
        logger.info("Processing webhook event: {} for run: {}", event.getEventType(), event.getRunId());

        switch (event.getEventType()) {
            case WebhookEvent.EventTypes.EXTRACTION_RUN_STARTED:
                handleExtractionRunStarted(event);
                break;
            case WebhookEvent.EventTypes.EXTRACTION_RUN_COMPLETED:
                handleExtractionRunCompleted(event);
                break;
            case WebhookEvent.EventTypes.EXTRACTION_RUN_FAILED:
                handleExtractionRunFailed(event);
                break;
            case WebhookEvent.EventTypes.FILE_EXTRACTION:
                handleFileExtraction(event);
                break;
            case WebhookEvent.EventTypes.PROGRESS_UPDATE:
                handleProgressUpdate(event);
                break;
            case WebhookEvent.EventTypes.JOB_STATUS_UPDATE:
                handleJobStatusUpdate(event);
                break;
            default:
                logger.warn("Unknown webhook event type: {}", event.getEventType());
        }
    }

    /**
     * Cancel an extraction run
     */
    public void cancelRun(UUID runId) {
        logger.info("Cancelling run: {}", runId);

        ExtractionRun extractionRun = extractionRunRepository.findById(runId)
            .orElseThrow(() -> new RuntimeException("Extraction run not found: " + runId));

        if (extractionRun.getPhase() == ExtractionRun.ExtractionPhase.COMPLETED ||
            extractionRun.getPhase() == ExtractionRun.ExtractionPhase.FAILED) {
            throw new RuntimeException("Cannot cancel completed or failed run");
        }

        extractionRun.setPhase(ExtractionRun.ExtractionPhase.FAILED);
        extractionRun.setFinishedAt(LocalDateTime.now());
        extractionRunRepository.save(extractionRun);

        // Update job status
        JobStatus jobStatus = new JobStatus(extractionRun);
        jobStatus.setStatus(JobStatus.JobStatusEnum.FAILED);
        jobStatus.setCurrentPhase("cancelled");
        jobStatus.setLastError("Run cancelled by user");
        jobStatusRepository.save(jobStatus);

        logger.info("Run cancelled successfully: {}", runId);
    }

    // Private helper methods for webhook event handling

    private void handleExtractionRunStarted(WebhookEvent event) {
        // Create or update extraction run
        ExtractionRun extractionRun = extractionRunRepository.findById(UUID.fromString(event.getRunId()))
            .orElseGet(() -> {
                // Create new extraction run if it doesn't exist
                ExtractionRun newRun = new ExtractionRun();
                newRun.setRunId(UUID.fromString(event.getRunId()));
                
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) event.getData();
                newRun.setRepositoryUrl((String) data.get("repositoryUrl"));
                newRun.setBranch((String) data.get("branch"));
                newRun.setRunMode(ExtractionRun.RunMode.FULL);
                newRun.setPhase(ExtractionRun.ExtractionPhase.STARTED);
                newRun.setTriggeredBy((String) data.get("triggeredBy"));
                newRun.setExtractorVersion("1.0.0");
                newRun.setStartedAt(LocalDateTime.now());
                return extractionRunRepository.save(newRun);
            });

        // Create initial job status
        JobStatus jobStatus = new JobStatus(extractionRun);
        jobStatus.setStatus(JobStatus.JobStatusEnum.RUNNING);
        jobStatus.setCurrentPhase("started");
        jobStatus.setPodId(event.getPodId());
        jobStatusRepository.save(jobStatus);

        logger.info("Extraction run started: {}", event.getRunId());
    }

    private void handleExtractionRunCompleted(WebhookEvent event) {
        try {
            ExtractionRun extractionRun = extractionRunRepository.findById(UUID.fromString(event.getRunId()))
                .orElseGet(() -> {
                    logger.warn("Extraction run not found for completion event: {}, creating new one", event.getRunId());
                    ExtractionRun newRun = new ExtractionRun();
                    newRun.setRunId(UUID.fromString(event.getRunId()));
                    newRun.setRepositoryUrl("unknown"); // Set default value for required field
                    newRun.setBranch("main");
                    newRun.setPhase(ExtractionRun.ExtractionPhase.STARTED);
                    newRun.setTriggeredBy("docker_container");
                    newRun.setExtractorVersion("1.0.0");
                    newRun.setStartedAt(LocalDateTime.now());
                    return extractionRunRepository.save(newRun);
                });

            extractionRun.setPhase(ExtractionRun.ExtractionPhase.COMPLETED);
            extractionRun.setFinishedAt(LocalDateTime.now());
            
            // Update stats if provided
            if (event.getData() != null && event.getData() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) event.getData();
            if (data.get("stats") instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> stats = (Map<String, Object>) data.get("stats");
                extractionRun.setStats(stats);
            }
            }
            
            extractionRunRepository.save(extractionRun);

            // Update job status to processing lineage
            JobStatus processingJobStatus = new JobStatus(extractionRun);
            processingJobStatus.setStatus(JobStatus.JobStatusEnum.RUNNING);
            processingJobStatus.setCurrentPhase("processing_lineage");
            processingJobStatus.setPodId(event.getPodId());
            jobStatusRepository.save(processingJobStatus);

            // Note: Processed lineage generation is now handled by event listeners
            // when raw data is inserted into tables and lineage_edges

            // Final job status update
            JobStatus jobStatus = new JobStatus(extractionRun);
            jobStatus.setStatus(JobStatus.JobStatusEnum.COMPLETED);
            jobStatus.setCurrentPhase("completed");
            jobStatus.setPodId(event.getPodId());
            jobStatusRepository.save(jobStatus);

            logger.info("Extraction run completed: {}", event.getRunId());
        } catch (Exception e) {
            logger.error("Error handling extraction run completed event: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void handleExtractionRunFailed(WebhookEvent event) {
        try {
            ExtractionRun extractionRun = extractionRunRepository.findById(UUID.fromString(event.getRunId()))
                .orElseGet(() -> {
                    logger.warn("Extraction run not found for failed event: {}, creating new one", event.getRunId());
                    ExtractionRun newRun = new ExtractionRun();
                    newRun.setRunId(UUID.fromString(event.getRunId()));
                    newRun.setRepositoryUrl("unknown"); // Set default value for required field
                    newRun.setBranch("main");
                    newRun.setPhase(ExtractionRun.ExtractionPhase.STARTED);
                    newRun.setTriggeredBy("docker_container");
                    newRun.setExtractorVersion("1.0.0");
                    newRun.setStartedAt(LocalDateTime.now());
                    return extractionRunRepository.save(newRun);
                });

            extractionRun.setPhase(ExtractionRun.ExtractionPhase.FAILED);
            extractionRun.setFinishedAt(LocalDateTime.now());
            extractionRunRepository.save(extractionRun);

            // Update job status
            JobStatus jobStatus = new JobStatus(extractionRun);
            jobStatus.setStatus(JobStatus.JobStatusEnum.FAILED);
            jobStatus.setCurrentPhase("failed");
            jobStatus.setPodId(event.getPodId());
            
            if (event.getData() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) event.getData();
                jobStatus.setLastError((String) data.get("error"));
            }
            
            jobStatusRepository.save(jobStatus);

            logger.info("Extraction run failed: {}", event.getRunId());
        } catch (Exception e) {
            logger.error("Error handling extraction run failed event: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void handleFileExtraction(WebhookEvent event) {
        try {
            ExtractionRun extractionRun = extractionRunRepository.findById(UUID.fromString(event.getRunId()))
                .orElseGet(() -> {
                    logger.warn("Extraction run not found for file extraction event: {}, creating new one", event.getRunId());
                    ExtractionRun newRun = new ExtractionRun();
                    newRun.setRunId(UUID.fromString(event.getRunId()));
                    newRun.setRepositoryUrl("unknown"); // Set default value for required field
                    newRun.setBranch("main");
                    newRun.setPhase(ExtractionRun.ExtractionPhase.STARTED);
                    newRun.setTriggeredBy("docker_container");
                    newRun.setExtractorVersion("1.0.0");
                    newRun.setStartedAt(LocalDateTime.now());
                    return extractionRunRepository.save(newRun);
                });

            if (event.getData() == null || !(event.getData() instanceof Map)) {
                logger.warn("Invalid file extraction data for event: {}", event.getRunId());
                return;
            }

            // Create file record
            File file = new File();
            file.setExtractionRun(extractionRun);
            
            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) event.getData();
            file.setFilePath((String) data.get("filePath"));
            file.setFileUrl((String) data.get("fileUrl"));
            
            // Set required file_hash field - use a default if not provided
            String fileHash = (String) data.get("fileHash");
            if (fileHash == null || fileHash.trim().isEmpty()) {
                fileHash = "unknown"; // Default value for required field
            }
            file.setFileHash(fileHash);
            
            // Handle file type
            String fileTypeStr = (String) data.get("fileType");
            if ("JINJA2".equals(fileTypeStr)) {
                file.setFileType(File.FileType.JINJA2);
            } else {
                file.setFileType(File.FileType.SQL);
            }
            
            file.setExtractedAt(LocalDateTime.now());
            
            // Set status based on event data
            String status = (String) data.get("status");
            if ("success".equals(status)) {
                file.setStatus(File.FileStatus.SUCCESS);
            } else if ("failed".equals(status)) {
                file.setStatus(File.FileStatus.FAILED);
            } else {
                file.setStatus(File.FileStatus.SKIPPED);
            }
            
            fileRepository.save(file);

            // Process tables from webhook data into normalized schema
            logger.info("🔍 WEBHOOK DATA KEYS: {}", data.keySet());
            if (data.get("tables") instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tables = (List<Map<String, Object>>) data.get("tables");
                logger.info("✅ Processing {} tables for file: {}", tables.size(), file.getFilePath());
                processTables(file, tables);
            }
            
            // Process lineage edges from webhook data (supports multiple shapes)
            List<Map<String, Object>> unifiedEdges = new ArrayList<>();

            // 1) Flat array under key "lineageEdges"
            if (data.get("lineageEdges") instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> lineageEdges = (List<Map<String, Object>>) data.get("lineageEdges");
                unifiedEdges.addAll(lineageEdges);
            }

            // 2) Separate arrays under keys "table_edges" / "column_edges" at root
            if (data.get("table_edges") instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tableEdges = (List<Map<String, Object>>) data.get("table_edges");
                for (Map<String, Object> e : tableEdges) {
                    Map<String, Object> m = new HashMap<>(e);
                    m.putIfAbsent("edge_type", "TABLE_EDGE");
                    unifiedEdges.add(m);
                }
            }
            if (data.get("column_edges") instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> columnEdges = (List<Map<String, Object>>) data.get("column_edges");
                for (Map<String, Object> e : columnEdges) {
                    Map<String, Object> m = new HashMap<>(e);
                    m.putIfAbsent("edge_type", "COLUMN_EDGE");
                    unifiedEdges.add(m);
                }
            }

            // 3) Nested under a "lineage" object
            if (data.get("lineage") instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> lineage = (Map<String, Object>) data.get("lineage");
                if (lineage.get("table_edges") instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> tableEdges = (List<Map<String, Object>>) lineage.get("table_edges");
                    for (Map<String, Object> e : tableEdges) {
                        Map<String, Object> m = new HashMap<>(e);
                        m.putIfAbsent("edge_type", "TABLE_EDGE");
                        unifiedEdges.add(m);
                    }
                }
                if (lineage.get("column_edges") instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> columnEdges = (List<Map<String, Object>>) lineage.get("column_edges");
                    for (Map<String, Object> e : columnEdges) {
                        Map<String, Object> m = new HashMap<>(e);
                        m.putIfAbsent("edge_type", "COLUMN_EDGE");
                        unifiedEdges.add(m);
                    }
                }
            }

            if (!unifiedEdges.isEmpty()) {
                logger.info("Processing {} lineage edges for file: {}", unifiedEdges.size(), file.getFilePath());
                processLineageEdges(file, unifiedEdges);
            }

            logger.info("File extraction processed: {} for run: {}", file.getFilePath(), event.getRunId());
        } catch (Exception e) {
            logger.error("Error handling file extraction event: {}", e.getMessage(), e);
            // Don't rethrow, just log the error to avoid breaking the webhook flow
        }
    }

    private void handleProgressUpdate(WebhookEvent event) {
        try {
            ExtractionRun extractionRun = extractionRunRepository.findById(UUID.fromString(event.getRunId()))
                .orElseGet(() -> {
                    logger.warn("Extraction run not found for progress update: {}, creating new one", event.getRunId());
                    ExtractionRun newRun = new ExtractionRun();
                    newRun.setRunId(UUID.fromString(event.getRunId()));
                    newRun.setRepositoryUrl("unknown"); // Set default value for required field
                    newRun.setBranch("main");
                    newRun.setPhase(ExtractionRun.ExtractionPhase.STARTED);
                    newRun.setTriggeredBy("docker_container");
                    newRun.setExtractorVersion("1.0.0");
                    newRun.setStartedAt(LocalDateTime.now());
                    return extractionRunRepository.save(newRun);
                });

            // Update job status with progress
            JobStatus jobStatus = new JobStatus(extractionRun);
            jobStatus.setStatus(JobStatus.JobStatusEnum.RUNNING);
            jobStatus.setPodId(event.getPodId());
            
            if (event.getData() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) event.getData();
                
                // Safely handle numeric fields
                if (data.get("totalFiles") instanceof Number) {
                    jobStatus.setTotalFiles(((Number) data.get("totalFiles")).intValue());
                }
                if (data.get("processedFiles") instanceof Number) {
                    jobStatus.setProcessedFiles(((Number) data.get("processedFiles")).intValue());
                }
                if (data.get("errorCount") instanceof Number) {
                    jobStatus.setErrorCount(((Number) data.get("errorCount")).intValue());
                }
                
                jobStatus.setCurrentFile((String) data.get("currentFile"));
                jobStatus.setCurrentPhase((String) data.get("currentPhase"));
            }
            
            jobStatusRepository.save(jobStatus);

            logger.info("Progress update processed for run: {}", event.getRunId());
        } catch (Exception e) {
            logger.error("Error handling progress update event: {}", e.getMessage(), e);
            // Don't rethrow, just log the error
        }
    }

    private void handleJobStatusUpdate(WebhookEvent event) {
        try {
            ExtractionRun extractionRun = extractionRunRepository.findById(UUID.fromString(event.getRunId()))
                .orElseGet(() -> {
                    logger.warn("Extraction run not found for job status update: {}, creating new one", event.getRunId());
                    ExtractionRun newRun = new ExtractionRun();
                    newRun.setRunId(UUID.fromString(event.getRunId()));
                    newRun.setRepositoryUrl("unknown"); // Set default value for required field
                    newRun.setBranch("main");
                    newRun.setPhase(ExtractionRun.ExtractionPhase.STARTED);
                    newRun.setTriggeredBy("docker_container");
                    newRun.setExtractorVersion("1.0.0");
                    newRun.setStartedAt(LocalDateTime.now());
                    return extractionRunRepository.save(newRun);
                });

            // Update job status
            JobStatus jobStatus = new JobStatus(extractionRun);
            jobStatus.setStatus(JobStatus.JobStatusEnum.RUNNING);
            jobStatus.setPodId(event.getPodId());
            
            if (event.getData() instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) event.getData();
                
                // Safely handle numeric fields
                if (data.get("totalFiles") instanceof Number) {
                    jobStatus.setTotalFiles(((Number) data.get("totalFiles")).intValue());
                }
                if (data.get("processedFiles") instanceof Number) {
                    jobStatus.setProcessedFiles(((Number) data.get("processedFiles")).intValue());
                }
                
                jobStatus.setCurrentFile((String) data.get("currentFile"));
                jobStatus.setCurrentPhase((String) data.get("currentPhase"));
            }
            
            jobStatusRepository.save(jobStatus);

            logger.info("Job status update processed for run: {}", event.getRunId());
        } catch (Exception e) {
            logger.error("Error handling job status update event: {}", e.getMessage(), e);
            // Don't rethrow, just log the error
        }
    }

    /**
     * Process tables from webhook data and save to database
     */
    private void processTables(File file, List<Map<String, Object>> tables) {
        try {
            logger.info("🚀 STARTING TO PROCESS {} TABLES for file: {}", tables.size(), file.getFilePath());
            for (Map<String, Object> tableData : tables) {
                logger.info("🔍 Processing table data: {}", tableData);
                // Extract table data
                String tableName = (String) tableData.get("name");
                if (tableName == null || tableName.trim().isEmpty()) {
                    logger.warn("Skipping table with missing name for file: {}", file.getFilePath());
                    continue;
                }
                
                // Determine role
                String roleStr = (String) tableData.get("role");
                Asset.Role assetRole = Asset.Role.INTERMEDIATE;
                if ("SOURCE".equalsIgnoreCase(roleStr)) {
                    assetRole = Asset.Role.SOURCE;
                } else if ("TARGET".equalsIgnoreCase(roleStr)) {
                    assetRole = Asset.Role.TARGET;
                }
                
                // Set schema: prefer provided schema, else derive from table name
                String schema = (String) tableData.get("schema");
                if (schema == null || schema.trim().isEmpty()) {
                    schema = deriveSchemaFromTableName(tableName);
                }
                
                Object columnsObj = tableData.get("columns");
                logger.info("✅ Processing table: {} (role: {}, schema: {}) for file: {}", tableName, assetRole, schema, file.getFilePath());

                String fullName = (schema != null && !schema.isBlank()) ? (schema + "." + tableName) : tableName;
                String shortName = tableName;
                // Only persist SOURCE/TARGET assets; skip intermediates
                if (assetRole != Asset.Role.INTERMEDIATE) {
                    Asset asset = upsertAsset(file.getExtractionRun().getRunId(), fullName, shortName, schema, assetRole, file.getExtractionRun());
                    linkAssetToFile(asset, file);
                    
                    // Store columns for both SOURCE and TARGET tables, but with role information
                    // This allows us to keep complete data while controlling what's displayed
                    upsertColumns(asset, columnsObj, assetRole);
                    logger.info("✅ Added {} columns for {} table: {}", 
                        assetRole == Asset.Role.TARGET ? "TARGET" : "SOURCE", 
                        assetRole, shortName);
                } else {
                    logger.info("Skipping intermediate asset persistence for table: {}", shortName);
                }
            }
        } catch (Exception e) {
            logger.error("Error processing tables for file {}: {}", file.getFilePath(), e.getMessage(), e);
        }
    }

    private Asset upsertAsset(UUID runId, String fullName, String shortName, String schemaName, Asset.Role role, ExtractionRun extractionRun) {
        Asset existing = assetRepository.findFirstByExtractionRunRunIdAndShortName(runId, shortName);
        if (existing != null) {
            // Update role/schema if missing
            if (existing.getSchemaName() == null && schemaName != null) existing.setSchemaName(schemaName);
            if (existing.getRole() == null && role != null) existing.setRole(role);
            if (existing.getFullName() == null) existing.setFullName(fullName);
            
            // Note: We no longer clear columns when role changes since we now store
            // SOURCE and TARGET columns separately with role information
            return assetRepository.save(existing);
        }
        Asset a = new Asset();
        a.setExtractionRun(extractionRun);
        a.setFullName(fullName);
        a.setShortName(shortName);
        a.setSchemaName(schemaName);
        a.setRole(role);
        return assetRepository.save(a);
    }

    private void linkAssetToFile(Asset asset, File file) {
        try {
            AssetFile af = new AssetFile();
            af.setExtractionRun(file.getExtractionRun());
            af.setFile(file);
            af.setAsset(asset);
            assetFileRepository.save(af);
        } catch (Exception ignore) {
            // best-effort linking; duplicates are acceptable if db constraints allow
        }
    }

    @SuppressWarnings("unchecked")
    private void upsertColumns(Asset asset, Object columnsObj, Asset.Role role) {
        if (!(columnsObj instanceof List)) return;
        try {
            List<String> cols = (List<String>) columnsObj;
            if (cols == null) return;
            
            // Fetch existing columns for this asset and role to avoid duplicates
            List<AssetColumn> existing = assetColumnRepository.findByAssetAssetIdAndRole(asset.getAssetId(), role);
            java.util.Set<String> existingNames = new java.util.HashSet<>();
            for (AssetColumn c : existing) existingNames.add(c.getColumnName());
            
            for (String c : cols) {
                if (c == null || c.isBlank()) continue;
                String name = c.trim().toLowerCase();
                if (existingNames.contains(name)) continue;
                
                AssetColumn ac = new AssetColumn();
                ac.setAsset(asset);
                ac.setColumnName(name);
                ac.setRole(role); // Store the role with the column
                assetColumnRepository.save(ac);
            }
        } catch (Exception e) {
            logger.warn("Failed to upsert columns for asset {} with role {}: {}", asset.getShortName(), role, e.getMessage());
        }
    }

    private String deriveSchemaFromTableName(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            return "unknown_schema";
        }
        String name = tableName.trim();
        String[] parts = name.split("\\.");
        if (parts.length > 1) {
            return String.join(".", java.util.Arrays.copyOf(parts, parts.length - 1));
        }
        return "unknown_schema";
    }

    // Sanitization helpers removed; handled in extractor

    /**
     * Process lineage edges from webhook data and save to database
     */
    private void processLineageEdges(File file, List<Map<String, Object>> lineageEdges) {
        try {
            logger.info("Starting to process {} lineage edges for file: {}", lineageEdges.size(), file.getFilePath());
            for (Map<String, Object> edgeData : lineageEdges) {
                logger.debug("Processing lineage edge data: {}", edgeData);
                
                // Extract required fields
                String fromTable = (String) edgeData.get("from_table");
                String toTable = (String) edgeData.get("to_table");
                
                if (fromTable == null || fromTable.trim().isEmpty() || 
                    toTable == null || toTable.trim().isEmpty()) {
                    logger.warn("Skipping lineage edge with missing table names for file: {}", file.getFilePath());
                    continue;
                }
                
                // Extract optional column fields
                String fromColumn = (String) edgeData.get("from_column");
                String toColumn = (String) edgeData.get("to_column");
                
                // Determine edge type
                String edgeTypeStr = (String) edgeData.get("edge_type");
                String edgeType = "table_edge";
                if ("COLUMN_LINEAGE".equalsIgnoreCase(edgeTypeStr) || "COLUMN_EDGE".equalsIgnoreCase(edgeTypeStr)) {
                    edgeType = "column_edge";
                }
                
                // Set transformation type
                String transformationType = (String) edgeData.get("transformation_type");
                if (transformationType == null || transformationType.trim().isEmpty()) {
                    transformationType = "UNKNOWN";
                }
                
                // Extract transformation details
                @SuppressWarnings("unchecked")
                Map<String, Object> transformationLines = (Map<String, Object>) edgeData.get("transformation_lines");
                
                logger.debug("Processing lineage edge: {} -> {} (type: {}) for file: {}", fromTable, toTable, edgeType, file.getFilePath());
                // Only persist lineage if both endpoints are stored (i.e., not intermediate)
                String fromShort = extractShort(fromTable);
                String toShort = extractShort(toTable);
                Asset fromAsset = assetRepository.findFirstByExtractionRunRunIdAndShortName(file.getExtractionRun().getRunId(), fromShort);
                Asset toAsset = assetRepository.findFirstByExtractionRunRunIdAndShortName(file.getExtractionRun().getRunId(), toShort);
                if (fromAsset == null || toAsset == null) {
                    logger.info("Skipping lineage edge {} -> {} because one or both endpoints are not persisted (likely intermediate)", fromTable, toTable);
                    continue;
                }
                linkAssetToFile(fromAsset, file);
                linkAssetToFile(toAsset, file);

                Lineage ln = new Lineage();
                ln.setExtractionRun(file.getExtractionRun());
                ln.setFile(file);
                ln.setFromAsset(fromAsset);
                ln.setToAsset(toAsset);
                ln.setFromColumn(fromColumn);
                ln.setToColumn(toColumn);
                ln.setEdgeType(edgeType);
                ln.setTransformationType(transformationType);
                if (transformationLines != null) {
                    Object s = transformationLines.get("start_line");
                    Object e = transformationLines.get("end_line");
                    ln.setStartLine(parseLineToInt(s));
                    ln.setEndLine(parseLineToInt(e));
                }
                lineageRepository.save(ln);
            }
        } catch (Exception e) {
            logger.error("Error processing lineage edges for file {}: {}", file.getFilePath(), e.getMessage(), e);
        }
    }

    private String extractShort(String full) {
        if (full == null) return null;
        String[] parts = full.split("\\.");
        return parts[parts.length - 1];
    }

    private Integer parseLineToInt(Object value) {
        if (value instanceof Number) return ((Number) value).intValue();
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.toLowerCase().startsWith("l")) s = s.substring(1);
            try { return Integer.parseInt(s); } catch (NumberFormatException ex) { return null; }
        }
        return null;
    }

    /**
     * Get job status for a specific run
     */
    public JobStatus getJobStatus(UUID runId) {
        logger.info("Getting job status for run: {}", runId);
        
        // Find the latest job status for this run
        Optional<JobStatus> latestJobStatus = jobStatusRepository.findFirstByExtractionRunRunIdOrderByLastUpdatedDesc(runId);
        
        if (latestJobStatus.isPresent()) {
            return latestJobStatus.get();
        } else {
            // Create a default job status if none exists
            ExtractionRun extractionRun = extractionRunRepository.findById(runId)
                .orElseThrow(() -> new RuntimeException("Extraction run not found: " + runId));
            
            JobStatus defaultStatus = new JobStatus(extractionRun);
            defaultStatus.setStatus(JobStatus.JobStatusEnum.FAILED);
            defaultStatus.setCurrentPhase("unknown");
            defaultStatus.setTotalFiles(0);
            defaultStatus.setProcessedFiles(0);
            defaultStatus.setErrorCount(0);
            
            return jobStatusRepository.save(defaultStatus);
        }
    }

    /**
     * Cancel an extraction run
     */
    public void cancelExtraction(UUID runId) {
        logger.info("Cancelling extraction run: {}", runId);
        
        ExtractionRun extractionRun = extractionRunRepository.findById(runId)
            .orElseThrow(() -> new RuntimeException("Extraction run not found: " + runId));
        
        // Update extraction run status
        extractionRun.setPhase(ExtractionRun.ExtractionPhase.FAILED);
        extractionRun.setFinishedAt(LocalDateTime.now());
        extractionRunRepository.save(extractionRun);
        
        // Update job status
        JobStatus jobStatus = new JobStatus(extractionRun);
        jobStatus.setStatus(JobStatus.JobStatusEnum.FAILED);
        jobStatus.setCurrentPhase("cancelled");
        jobStatus.setLastUpdated(LocalDateTime.now());
        jobStatusRepository.save(jobStatus);
        
        logger.info("Successfully cancelled extraction run: {}", runId);
    }
} 