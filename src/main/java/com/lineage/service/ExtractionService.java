package com.lineage.service;

import com.lineage.dto.RepositoryScanRequest;
import com.lineage.dto.WebhookEvent;
import com.lineage.entity.*;
import com.lineage.repository.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private LineageEdgeRepository lineageEdgeRepository;

    @Autowired
    private JobStatusRepository jobStatusRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DockerService dockerService;

    @Autowired
    private LineageProcessingService lineageProcessingService;

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
        dockerService.launchExtractionContainer(
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

        // Get lineage statistics
        long totalEdges = lineageEdgeRepository.countByRunId(runId);
        long tableEdges = lineageEdgeRepository.countByEdgeTypeAndRunId(LineageEdge.EdgeType.TABLE_EDGE, runId);
        long columnEdges = lineageEdgeRepository.countByEdgeTypeAndRunId(LineageEdge.EdgeType.COLUMN_EDGE, runId);

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

            // Process lineage data - this replicates the process_lineage.json functionality
            try {
                logger.info("Starting lineage processing for run: {}", extractionRun.getRunId());
                lineageProcessingService.processLineageForRun(extractionRun.getRunId());
                logger.info("Completed lineage processing for run: {}", extractionRun.getRunId());
            } catch (Exception e) {
                logger.error("Failed to process lineage for run {}: {}", extractionRun.getRunId(), e.getMessage(), e);
                // Don't fail the entire extraction, just log the error
            }

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

            // Process tables from webhook data
            logger.info("🔍 WEBHOOK DATA KEYS: {}", data.keySet());
            logger.info("🔍 TABLES DATA: type={}, value={}", 
                data.get("tables") != null ? data.get("tables").getClass().getSimpleName() : "null", 
                data.get("tables"));
                
            if (data.get("tables") instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> tables = (List<Map<String, Object>>) data.get("tables");
                logger.info("✅ Processing {} tables for file: {}", tables.size(), file.getFilePath());
                processTables(file, tables);
            } else {
                logger.warn("❌ No tables data found or invalid format for file: {}", file.getFilePath());
                logger.warn("❌ Tables data type: {}, value: {}", 
                    data.get("tables") != null ? data.get("tables").getClass().getSimpleName() : "null", 
                    data.get("tables"));
            }
            
            // Process lineage edges from webhook data
            if (data.get("lineageEdges") instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> lineageEdges = (List<Map<String, Object>>) data.get("lineageEdges");
                logger.info("Processing {} lineage edges for file: {}", lineageEdges.size(), file.getFilePath());
                processLineageEdges(file, lineageEdges);
            } else {
                logger.warn("No lineageEdges data found or invalid format for file: {}", file.getFilePath());
                logger.debug("LineageEdges data type: {}, value: {}", 
                    data.get("lineageEdges") != null ? data.get("lineageEdges").getClass().getSimpleName() : "null", 
                    data.get("lineageEdges"));
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
                TableEntity table = new TableEntity();
                table.setFile(file);
                
                // Set table name (required field)
                String tableName = (String) tableData.get("name");
                if (tableName == null || tableName.trim().isEmpty()) {
                    logger.warn("Skipping table with missing name for file: {}", file.getFilePath());
                    continue;
                }
                table.setTableName(tableName);
                
                // Set table role
                String roleStr = (String) tableData.get("role");
                if ("SOURCE".equalsIgnoreCase(roleStr)) {
                    table.setTableRole(TableEntity.TableRole.SOURCE);
                } else if ("TARGET".equalsIgnoreCase(roleStr)) {
                    table.setTableRole(TableEntity.TableRole.TARGET);
                } else {
                    table.setTableRole(TableEntity.TableRole.INTERMEDIATE);
                }
                
                // Set schema if provided
                String schema = (String) tableData.get("schema");
                if (schema != null && !schema.trim().isEmpty()) {
                    table.setTableSchema(schema);
                }
                
                // Set columns (required field) - columns come as List<String> from pod
                Object columnsObj = tableData.get("columns");
                Map<String, Object> columns;
                if (columnsObj instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<String> columnsList = (List<String>) columnsObj;
                    // Convert List<String> to Map<String, Object> format
                    columns = new HashMap<>();
                    for (int i = 0; i < columnsList.size(); i++) {
                        columns.put("column_" + i, columnsList.get(i));
                    }
                    logger.info("📝 Converted {} columns from list to map for table: {}", columnsList.size(), tableName);
                } else if (columnsObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> mapColumns = (Map<String, Object>) columnsObj;
                    columns = mapColumns;
                } else {
                    columns = Map.of(); // Empty map if no columns provided
                    logger.warn("⚠️ No columns data or invalid format for table: {}", tableName);
                }
                table.setColumns(columns);
                
                tableRepository.save(table);
                logger.info("✅ SAVED TABLE: {} (role: {}) for file: {}", tableName, table.getTableRole(), file.getFilePath());
            }
        } catch (Exception e) {
            logger.error("Error processing tables for file {}: {}", file.getFilePath(), e.getMessage(), e);
        }
    }

    /**
     * Process lineage edges from webhook data and save to database
     */
    private void processLineageEdges(File file, List<Map<String, Object>> lineageEdges) {
        try {
            logger.info("Starting to process {} lineage edges for file: {}", lineageEdges.size(), file.getFilePath());
            for (Map<String, Object> edgeData : lineageEdges) {
                logger.debug("Processing lineage edge data: {}", edgeData);
                LineageEdge edge = new LineageEdge();
                edge.setFile(file);
                
                // Set required fields
                String fromTable = (String) edgeData.get("from_table");
                String toTable = (String) edgeData.get("to_table");
                
                if (fromTable == null || fromTable.trim().isEmpty() || 
                    toTable == null || toTable.trim().isEmpty()) {
                    logger.warn("Skipping lineage edge with missing table names for file: {}", file.getFilePath());
                    continue;
                }
                
                edge.setFromTable(fromTable);
                edge.setToTable(toTable);
                
                // Set optional column fields
                String fromColumn = (String) edgeData.get("from_column");
                String toColumn = (String) edgeData.get("to_column");
                if (fromColumn != null && !fromColumn.trim().isEmpty()) {
                    edge.setFromColumn(fromColumn);
                }
                if (toColumn != null && !toColumn.trim().isEmpty()) {
                    edge.setToColumn(toColumn);
                }
                
                // Set edge type
                String edgeTypeStr = (String) edgeData.get("edge_type");
                if ("COLUMN_LINEAGE".equalsIgnoreCase(edgeTypeStr) || "COLUMN_EDGE".equalsIgnoreCase(edgeTypeStr)) {
                    edge.setEdgeType(LineageEdge.EdgeType.COLUMN_EDGE);
                } else {
                    edge.setEdgeType(LineageEdge.EdgeType.TABLE_EDGE);
                }
                
                // Set transformation type (required field)
                String transformationType = (String) edgeData.get("transformation_type");
                if (transformationType == null || transformationType.trim().isEmpty()) {
                    transformationType = "UNKNOWN";
                }
                edge.setTransformationType(transformationType);
                
                // Set transformation details
                @SuppressWarnings("unchecked")
                Map<String, Object> transformationLines = (Map<String, Object>) edgeData.get("transformation_lines");
                if (transformationLines != null) {
                    edge.setTransformationLines(transformationLines);
                }
                
                String transformationCode = (String) edgeData.get("transformation_code");
                if (transformationCode != null && !transformationCode.trim().isEmpty()) {
                    edge.setTransformationCode(transformationCode);
                }
                
                lineageEdgeRepository.save(edge);
                logger.debug("Saved lineage edge: {} -> {} for file: {}", fromTable, toTable, file.getFilePath());
            }
        } catch (Exception e) {
            logger.error("Error processing lineage edges for file {}: {}", file.getFilePath(), e.getMessage(), e);
        }
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