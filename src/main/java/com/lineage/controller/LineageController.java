package com.lineage.controller;

import com.lineage.entity.TableEntity;
import com.lineage.entity.LineageEdge;
import com.lineage.repository.TableRepository;
import com.lineage.repository.LineageEdgeRepository;
import com.lineage.service.LineageProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/lineage")
@CrossOrigin(origins = "*")
public class LineageController {

    private static final Logger logger = LoggerFactory.getLogger(LineageController.class);

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private LineageEdgeRepository lineageEdgeRepository;

    @Autowired
    private LineageProcessingService lineageProcessingService;

    /**
     * Get all tables for a run
     */
    @GetMapping("/runs/{runId}/tables")
    public ResponseEntity<List<TableEntity>> getTables(@PathVariable UUID runId) {
        logger.info("Getting all tables for run: {}", runId);

        List<TableEntity> tables = tableRepository.findByRunId(runId);
        return ResponseEntity.ok(tables);
    }

    /**
     * Get all lineage edges for a run
     */
    @GetMapping("/runs/{runId}/edges")
    public ResponseEntity<List<LineageEdge>> getLineageEdges(@PathVariable UUID runId) {
        logger.info("Getting all lineage edges for run: {}", runId);

        List<LineageEdge> edges = lineageEdgeRepository.findByRunId(runId);
        return ResponseEntity.ok(edges);
    }

    /**
     * Get lineage statistics for a run
     */
    @GetMapping("/runs/{runId}/stats")
    public ResponseEntity<Map<String, Object>> getLineageStats(@PathVariable UUID runId) {
        logger.info("Getting lineage statistics for run: {}", runId);

        Map<String, Object> stats = new HashMap<>();
        
        long tableCount = tableRepository.countByRunId(runId);
        long edgeCount = lineageEdgeRepository.countByRunId(runId);
        
        stats.put("totalTables", tableCount);
        stats.put("totalEdges", edgeCount);

        return ResponseEntity.ok(stats);
    }

    /**
     * Manually trigger lineage processing for a run (for debugging/reprocessing)
     */
    @PostMapping("/runs/{runId}/process")
    public ResponseEntity<Map<String, String>> processLineage(@PathVariable UUID runId) {
        logger.info("Manually triggering lineage processing for run: {}", runId);

        try {
            lineageProcessingService.processLineageForRun(runId);
            
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Basic lineage processing completed successfully");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to process lineage for run {}: {}", runId, e.getMessage(), e);
            
            Map<String, String> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "Failed to process lineage: " + e.getMessage());
            
            return ResponseEntity.status(500).body(response);
        }
    }
}
