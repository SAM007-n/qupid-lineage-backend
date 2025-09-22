package com.lineage.controller;

import com.lineage.entity.ExtractionRun;
import com.lineage.repository.ExtractionRunRepository;
import com.lineage.service.ExtractionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.*;

@RestController
@RequestMapping("/runs")
@CrossOrigin(origins = "*")
public class RunsController {
    private static final Logger logger = LoggerFactory.getLogger(RunsController.class);

    @Autowired private ExtractionRunRepository extractionRunRepository;
    @Autowired private ExtractionService extractionService;

    @GetMapping("/recent")
    public ResponseEntity<List<Map<String, Object>>> recent() {
        try {
            List<ExtractionRun> runs = extractionRunRepository.findRecentRuns(LocalDateTime.now().minusDays(7));
            List<Map<String, Object>> resp = new ArrayList<>();
            for (ExtractionRun r : runs) {
                Map<String, Object> row = new HashMap<>();
                row.put("runId", r.getRunId());
                row.put("sourceRepo", r.getRepositoryUrl());
                row.put("branch", r.getBranch());
                row.put("status", r.getPhase() != null ? r.getPhase().name() : null);
                row.put("trigger", r.getTriggeredBy());
                row.put("startedAt", r.getStartedAt());
                row.put("finishedAt", r.getFinishedAt());
                // deltas and errors can be filled later in service impl
                row.put("assetsDelta", 0);
                row.put("edgesDelta", 0);
                row.put("errors", 0);
                resp.add(row);
            }
            return ResponseEntity.ok(resp);
        } catch (Exception e) {
            logger.error("Failed to load recent runs: {}", e.getMessage(), e);
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("")
    public ResponseEntity<Map<String, Object>> list(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        try {
            List<ExtractionRun> all = extractionRunRepository.findAll();
            int from = Math.min(page * size, all.size());
            int to = Math.min(from + size, all.size());
            List<ExtractionRun> pageItems = all.subList(from, to);
            List<Map<String, Object>> items = new ArrayList<>();
            for (ExtractionRun r : pageItems) {
                Map<String, Object> row = new HashMap<>();
                row.put("runId", r.getRunId());
                row.put("sourceRepo", r.getRepositoryUrl());
                row.put("branch", r.getBranch());
                row.put("status", r.getPhase() != null ? r.getPhase().name() : null);
                row.put("trigger", r.getTriggeredBy());
                row.put("startedAt", r.getStartedAt());
                row.put("finishedAt", r.getFinishedAt());
                row.put("assetsDelta", 0);
                row.put("edgesDelta", 0);
                row.put("errors", 0);
                items.add(row);
            }
            Map<String, Object> resp = new HashMap<>();
            resp.put("items", items);
            resp.put("totalElements", all.size());
            resp.put("totalPages", (int) Math.ceil((double) all.size() / size));
            resp.put("currentPage", page);
            resp.put("pageSize", size);
            return ResponseEntity.ok(resp);
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/{runId}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable UUID runId) {
        try {
            ExtractionRun r = extractionRunRepository.findById(runId).orElse(null);
            if (r == null) return ResponseEntity.notFound().build();
            Map<String, Object> status = extractionService.getRunStatus(runId);
            return ResponseEntity.ok(status);
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }
}


