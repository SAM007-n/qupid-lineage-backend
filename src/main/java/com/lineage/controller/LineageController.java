package com.lineage.controller;

import com.lineage.dto.*;
import com.lineage.entity.TableEntity;
import com.lineage.entity.LineageEdge;
import com.lineage.entity.ProcessedTable;
import com.lineage.entity.ProcessedColumnLineage;
import com.lineage.entity.ProcessedTableLineage;
import com.lineage.repository.TableRepository;
import com.lineage.repository.LineageEdgeRepository;
import com.lineage.repository.ProcessedTableRepository;
import com.lineage.repository.ProcessedColumnLineageRepository;
import com.lineage.repository.ProcessedTableLineageRepository;
import com.lineage.service.LineageApiService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * LineageController exposes REST endpoints for:
 * - Frontend-facing lineage APIs (entity lineage GET/POST, entity details, bulk details)
 * - Admin/debug APIs to browse raw and processed data for a specific extraction run
 *
 * All routes are served under /api (configured context-path) and /lineage base path here.
 */
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
    private ProcessedTableRepository processedTableRepository;

    @Autowired
    private ProcessedColumnLineageRepository processedColumnLineageRepository;

    @Autowired
    private ProcessedTableLineageRepository processedTableLineageRepository;


    @Autowired
    private LineageApiService lineageApiService;

    // ===============================
    // FRONTEND-COMPATIBLE ENDPOINTS
    // ===============================

    /**
     * Frontend endpoint: GET /lineage?entityId=X&direction=Y
     * Returns upstream or downstream lineage for a given entity, including optional
     * table-level transformations and fine-grained (column) lineage.
     */
    @GetMapping("")
    public ResponseEntity<LineageResponseDto> getLineage(
            @RequestParam String entityId,
            @RequestParam LineageDirection direction) {
        logger.info("Getting {} lineage for entity: {}", direction, entityId);
        try {
            LineageResponseDto response = lineageApiService.getLineage(entityId, direction);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get lineage for entity {}: {}", entityId, e.getMessage(), e);
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Frontend endpoint: GET /search/entities?q=...
     * Simple search over processed tables by entity name/id.
     */
    @GetMapping("/search/entities")
    public ResponseEntity<List<SearchResultItem>> searchEntities(@RequestParam("q") String query) {
        logger.info("Searching entities for query: {}", query);
        try {
            List<SearchResultItem> results = lineageApiService.searchEntities(query);
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            logger.error("Search failed for query {}: {}", query, e.getMessage(), e);
            return ResponseEntity.badRequest().build();
        }
    }

    /**
     * Frontend endpoint: POST /lineage { entityId, direction }
     * Same as GET /lineage, parameters accepted via JSON body.
     */
    @PostMapping("")
    public ResponseEntity<LineageResponseDto> getLineagePost(@RequestBody LineageRequestDto request) {
        logger.info("Getting {} lineage for entity: {}", request.getDirection(), request.getEntityId());
        try {
            LineageResponseDto response = lineageApiService.getLineage(request.getEntityId(), request.getDirection());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to get lineage for entity {}: {}", request.getEntityId(), e.getMessage(), e);
            return ResponseEntity.notFound().build();
        }
    }

    // ===============================
    // ENTITY ENDPOINTS
    // ===============================

    /**
     * Frontend endpoint: GET /entity/{entityId}
     * Returns entity summary (counts, presence flags, schema metadata, fine-grained lineage).
     */
    @GetMapping("/entity/{entityId}")
    public ResponseEntity<EntityDto> getEntity(@PathVariable String entityId) {
        logger.info("Getting entity details for: {}", entityId);
        try {
            EntityDto entity = lineageApiService.getEntity(entityId);
            return ResponseEntity.ok(entity);
        } catch (Exception e) {
            logger.error("Failed to get entity {}: {}", entityId, e.getMessage(), e);
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Frontend endpoint: POST /entity/bulk [entityId1, entityId2, ...]
     * Returns multiple entity summaries in a single call.
     */
    @PostMapping("/entity/bulk")
    public ResponseEntity<List<EntityDto>> getBulkEntities(@RequestBody List<String> entityIds) {
        logger.info("Getting bulk entity details for {} entities", entityIds.size());
        try {
            List<EntityDto> entities = lineageApiService.getBulkEntities(entityIds);
            return ResponseEntity.ok(entities);
        } catch (Exception e) {
            logger.error("Failed to get bulk entities: {}", e.getMessage(), e);
            return ResponseEntity.badRequest().build();
        }
    }

    // ===============================
    // ADMIN/DEBUG ENDPOINTS (Run-specific)
    // ===============================

    /**
     * Admin/debug: Return raw extracted tables for a run (pre-aggregation).
     */
    @GetMapping("/runs/{runId}/tables")
    public ResponseEntity<List<TableEntity>> getTables(@PathVariable UUID runId) {
        logger.info("Getting raw tables for run: {}", runId);
        List<TableEntity> tables = tableRepository.findByRunId(runId);
        return ResponseEntity.ok(tables);
    }

    /**
     * Admin/debug: Return raw lineage edges (table/column edges) for a run (pre-aggregation).
     */
    @GetMapping("/runs/{runId}/edges")
    public ResponseEntity<List<LineageEdge>> getLineageEdges(@PathVariable UUID runId) {
        logger.info("Getting lineage edges for run: {}", runId);
        List<LineageEdge> edges = lineageEdgeRepository.findByRunId(runId);
        return ResponseEntity.ok(edges);
    }

    /**
     * Admin/debug: Return processed tables for a run (post-aggregation).
     */
    @GetMapping("/runs/{runId}/processed-tables")
    public ResponseEntity<List<ProcessedTable>> getProcessedTables(@PathVariable UUID runId) {
        logger.info("Getting processed tables for run: {}", runId);
        List<ProcessedTable> tables = processedTableRepository.findByExtractionRunRunIdOrderByEntityName(runId);
        return ResponseEntity.ok(tables);
    }

    /**
     * Admin/debug: Return processed column-level lineage entries for a run.
     */
    @GetMapping("/runs/{runId}/processed-column-lineages")
    public ResponseEntity<List<ProcessedColumnLineage>> getProcessedColumnLineages(@PathVariable UUID runId) {
        logger.info("Getting processed column lineages for run: {}", runId);
        List<ProcessedColumnLineage> lineages = processedColumnLineageRepository.findByRunIdOrderByDownstreamTable(runId);
        return ResponseEntity.ok(lineages);
    }

    /**
     * Admin/debug: Return processed table-level lineage relationships for a run.
     */
    @GetMapping("/runs/{runId}/processed-table-lineages")
    public ResponseEntity<List<ProcessedTableLineage>> getProcessedTableLineages(@PathVariable UUID runId) {
        logger.info("Getting processed table lineages for run: {}", runId);
        List<ProcessedTableLineage> lineages = processedTableLineageRepository.findByExtractionRunRunIdOrderByTableName(runId);
        return ResponseEntity.ok(lineages);
    }

    /**
     * Admin/debug: Convenience endpoint returning only tables that have upstream dependencies.
     */
    @GetMapping("/runs/{runId}/table-relationships")
    public ResponseEntity<List<ProcessedTableLineage>> getTableRelationships(@PathVariable UUID runId) {
        logger.info("Getting table relationships for run: {}", runId);
        List<ProcessedTableLineage> relationships = processedTableLineageRepository.findTablesWithUpstreamDependencies(runId);
        return ResponseEntity.ok(relationships);
    }

    /**
     * Admin/debug: Return a map keyed by entity_id with processed table details (legacy shape).
     */
    @GetMapping("/runs/{runId}/table-details")
    public ResponseEntity<Map<String, Object>> getTableDetails(@PathVariable UUID runId) {
        logger.info("Getting table details for run: {}", runId);

        List<ProcessedTable> tables = processedTableRepository.findByExtractionRunRunIdOrderByEntityName(runId);
        Map<String, Object> tableDetails = new HashMap<>();
        
        for (ProcessedTable table : tables) {
            Map<String, Object> tableInfo = new HashMap<>();
            tableInfo.put("entity_id", table.getEntityId());
            tableInfo.put("entity_name", table.getEntityName());
            tableInfo.put("source", table.getSource());
            tableInfo.put("entity_type", table.getEntityType());
            tableInfo.put("columns", table.getColumnsCount());
            tableInfo.put("tool_key", table.getToolKey());
            tableInfo.put("partition_keys", table.getPartitionKeys());
            tableInfo.put("schemaMetadata", table.getSchemaMetadata());
            
            // Get fine-grained lineages for this table
            List<ProcessedColumnLineage> columnLineages = processedColumnLineageRepository
                .findByRunIdAndDownstreamTable(runId, table.getEntityId());
            tableInfo.put("fineGrainedLineages", columnLineages);
            
            tableDetails.put(table.getEntityId(), tableInfo);
        }

        return ResponseEntity.ok(tableDetails);
    }

    /**
     * Admin/debug: Return aggregated counters for raw and processed entities for a run.
     */
    @GetMapping("/runs/{runId}/stats")
    public ResponseEntity<Map<String, Object>> getLineageStats(@PathVariable UUID runId) {
        logger.info("Getting lineage statistics for run: {}", runId);

        Map<String, Object> stats = new HashMap<>();
        
        long rawTableCount = tableRepository.countByRunId(runId);
        long rawEdgeCount = lineageEdgeRepository.countByRunId(runId);
        long processedTableCount = processedTableRepository.countByRunId(runId);
        long processedColumnLineageCount = processedColumnLineageRepository.countByRunId(runId);
        long processedTableLineageCount = processedTableLineageRepository.countByRunId(runId);
        
        stats.put("rawTables", rawTableCount);
        stats.put("rawEdges", rawEdgeCount);
        stats.put("processedTables", processedTableCount);
        stats.put("processedColumnLineages", processedColumnLineageCount);
        stats.put("processedTableLineages", processedTableLineageCount);

        return ResponseEntity.ok(stats);
    }

    // Removed manual batch reprocess endpoint — real-time processing keeps data in sync.
}