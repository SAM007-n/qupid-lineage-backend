package com.lineage.controller;

import com.lineage.dto.*;
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
 * - Search APIs for asset discovery
 *
 * All routes are served under /api (configured context-path) and /lineage base path here.
 */
@RestController
@RequestMapping("/lineage")
@CrossOrigin(origins = "*")
public class LineageController {

    private static final Logger logger = LoggerFactory.getLogger(LineageController.class);

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

    /**
     * Frontend endpoint: GET /edge/{edgeId}
     * Returns details about a specific lineage edge from normalized schema.
     */
    @GetMapping("/edge/{edgeId}")
    public ResponseEntity<EdgeDetailsDto> getEdge(@PathVariable String edgeId) {
        try {
            EdgeDetailsDto dto = lineageApiService.getEdgeDetails(edgeId);
            return ResponseEntity.ok(dto);
        } catch (Exception e) {
            logger.error("Failed to get edge details for {}: {}", edgeId, e.getMessage(), e);
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

    // Legacy admin endpoints removed - using simplified normalized schema
}