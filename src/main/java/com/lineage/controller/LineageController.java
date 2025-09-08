package com.lineage.controller;

import com.lineage.entity.AggregatedTable;
import com.lineage.entity.FineGrainedLineage;
import com.lineage.entity.TableRelationship;
import com.lineage.repository.AggregatedTableRepository;
import com.lineage.repository.FineGrainedLineageRepository;
import com.lineage.repository.TableRelationshipRepository;
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
import java.util.stream.Collectors;

@RestController
@RequestMapping("/lineage")
@CrossOrigin(origins = "*")
public class LineageController {

    private static final Logger logger = LoggerFactory.getLogger(LineageController.class);

    @Autowired
    private AggregatedTableRepository aggregatedTableRepository;

    @Autowired
    private FineGrainedLineageRepository fineGrainedLineageRepository;

    @Autowired
    private TableRelationshipRepository tableRelationshipRepository;

    @Autowired
    private LineageProcessingService lineageProcessingService;

    /**
     * Get aggregated table details for a run (equivalent to table-detail-output.generated.json)
     */
    @GetMapping("/runs/{runId}/table-details")
    public ResponseEntity<Map<String, Object>> getTableDetails(@PathVariable UUID runId) {
        logger.info("Getting table details for run: {}", runId);

        List<AggregatedTable> tables = aggregatedTableRepository.findByExtractionRunRunIdOrderByEntityName(runId);
        
        Map<String, Object> tableDetails = new HashMap<>();
        for (AggregatedTable table : tables) {
            Map<String, Object> tableInfo = new HashMap<>();
            tableInfo.put("entity_id", table.getEntityId());
            tableInfo.put("entity_name", table.getEntityName());
            tableInfo.put("source", table.getSource());
            tableInfo.put("entity_type", table.getEntityType());
            tableInfo.put("columns", table.getColumnsCount());
            tableInfo.put("tool_key", table.getToolKey());
            tableInfo.put("partition_keys", table.getPartitionKeys());
            tableInfo.put("schemaMetadata", table.getSchemaMetadata());
            tableInfo.put("fineGrainedLineages", getFineGrainedLineagesForTable(table.getAggregatedTableId()));
            
            tableDetails.put(table.getEntityId(), tableInfo);
        }

        return ResponseEntity.ok(tableDetails);
    }

    /**
     * Get table relationships for a run (equivalent to table-lineage-output.generated.json)
     */
    @GetMapping("/runs/{runId}/table-relationships")
    public ResponseEntity<Map<String, Object>> getTableRelationships(@PathVariable UUID runId) {
        logger.info("Getting table relationships for run: {}", runId);

        List<TableRelationship> relationships = tableRelationshipRepository.findByExtractionRunRunIdOrderByTableName(runId);
        
        Map<String, Object> tableRelationships = new HashMap<>();
        for (TableRelationship relationship : relationships) {
            Map<String, Object> relationshipInfo = new HashMap<>();
            relationshipInfo.put("upstream", relationship.getUpstreamTables());
            relationshipInfo.put("downstream", relationship.getDownstreamTables());
            
            tableRelationships.put(relationship.getTableName(), relationshipInfo);
        }

        return ResponseEntity.ok(tableRelationships);
    }

    /**
     * Get fine-grained lineages for a specific table
     */
    @GetMapping("/runs/{runId}/tables/{tableName}/fine-grained-lineages")
    public ResponseEntity<List<FineGrainedLineage>> getFineGrainedLineages(
            @PathVariable UUID runId, 
            @PathVariable String tableName) {
        logger.info("Getting fine-grained lineages for table: {} in run: {}", tableName, runId);

        List<FineGrainedLineage> lineages = fineGrainedLineageRepository.findByRunIdAndDownstreamTable(runId, tableName);
        return ResponseEntity.ok(lineages);
    }

    /**
     * Get all tables for a run
     */
    @GetMapping("/runs/{runId}/tables")
    public ResponseEntity<List<AggregatedTable>> getTables(@PathVariable UUID runId) {
        logger.info("Getting all tables for run: {}", runId);

        List<AggregatedTable> tables = aggregatedTableRepository.findByExtractionRunRunIdOrderByEntityName(runId);
        return ResponseEntity.ok(tables);
    }

    /**
     * Get table by entity ID for a run
     */
    @GetMapping("/runs/{runId}/tables/{entityId}")
    public ResponseEntity<AggregatedTable> getTable(@PathVariable UUID runId, @PathVariable String entityId) {
        logger.info("Getting table {} for run: {}", entityId, runId);

        return aggregatedTableRepository.findByExtractionRunRunIdAndEntityId(runId, entityId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get lineage statistics for a run
     */
    @GetMapping("/runs/{runId}/stats")
    public ResponseEntity<Map<String, Object>> getLineageStats(@PathVariable UUID runId) {
        logger.info("Getting lineage statistics for run: {}", runId);

        Map<String, Object> stats = new HashMap<>();
        
        long tableCount = aggregatedTableRepository.countByRunId(runId);
        long lineageCount = fineGrainedLineageRepository.countByRunId(runId);
        long relationshipCount = tableRelationshipRepository.countByRunId(runId);
        
        List<String> sources = aggregatedTableRepository.findDistinctSourcesByRunId(runId);
        
        stats.put("totalTables", tableCount);
        stats.put("totalFineGrainedLineages", lineageCount);
        stats.put("totalTableRelationships", relationshipCount);
        stats.put("distinctSources", sources);
        stats.put("sourceCount", sources.size());

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
            response.put("message", "Lineage processing completed successfully");
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to process lineage for run {}: {}", runId, e.getMessage(), e);
            
            Map<String, String> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "Failed to process lineage: " + e.getMessage());
            
            return ResponseEntity.status(500).body(response);
        }
    }

    /**
     * Get tables by source for a run
     */
    @GetMapping("/runs/{runId}/sources/{source}/tables")
    public ResponseEntity<List<AggregatedTable>> getTablesBySource(
            @PathVariable UUID runId, 
            @PathVariable String source) {
        logger.info("Getting tables for source: {} in run: {}", source, runId);

        List<AggregatedTable> tables = aggregatedTableRepository.findByRunIdAndSource(runId, source);
        return ResponseEntity.ok(tables);
    }

    // Helper method
    private List<Object> getFineGrainedLineagesForTable(UUID aggregatedTableId) {
        List<FineGrainedLineage> lineages = fineGrainedLineageRepository.findByAggregatedTableAggregatedTableIdOrderByDownstreamColumn(aggregatedTableId);
        
        return lineages.stream().map(lineage -> {
            Map<String, Object> lineageMap = new HashMap<>();
            lineageMap.put("upstreams", lineage.getUpstreamReferences());
            lineageMap.put("downstreams", List.of(Map.of(
                "urn", lineage.getDownstreamTable(),
                "path", lineage.getDownstreamColumn()
            )));
            return lineageMap;
        }).collect(Collectors.toList());
    }
}
