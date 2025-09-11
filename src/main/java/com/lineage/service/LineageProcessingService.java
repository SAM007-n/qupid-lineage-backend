package com.lineage.service;

import com.lineage.entity.*;
import com.lineage.repository.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
public class LineageProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(LineageProcessingService.class);

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private LineageEdgeRepository lineageEdgeRepository;

    @Autowired
    private FileRepository fileRepository;

    /**
     * Main processing method for basic lineage processing
     */
    @Transactional
    public void processLineageForRun(UUID runId) {
        logger.info("Starting basic lineage processing for run: {}", runId);

        try {
            // Get basic statistics
            List<TableEntity> allTables = tableRepository.findByRunId(runId);
            List<LineageEdge> allEdges = lineageEdgeRepository.findByRunId(runId);

            logger.info("Completed basic lineage processing for run: {}. Found {} tables and {} edges", 
                       runId, allTables.size(), allEdges.size());

        } catch (Exception e) {
            logger.error("Error processing lineage for run {}: {}", runId, e.getMessage(), e);
            throw new RuntimeException("Failed to process lineage for run: " + runId, e);
        }
    }

}
