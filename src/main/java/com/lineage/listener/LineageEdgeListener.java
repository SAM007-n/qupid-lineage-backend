package com.lineage.listener;

import com.lineage.config.ApplicationContextProvider;
import com.lineage.entity.LineageEdge;
import com.lineage.service.LineageProcessingService;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PostUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LineageEdgeListener: JPA callback listener used to trigger real-time processing
 * whenever a raw `lineage_edges` row is inserted or updated.
 */
public class LineageEdgeListener {

    private static final Logger logger = LoggerFactory.getLogger(LineageEdgeListener.class);

    @PostPersist
    public void onLineageEdgeInsert(LineageEdge edge) {
        logger.info("Lineage edge inserted: {} -> {}", edge.getFromTable(), edge.getToTable());
        try {
            LineageProcessingService service = ApplicationContextProvider.getBean(LineageProcessingService.class);
            service.processNewLineageEdge(edge);
        } catch (Exception e) {
            logger.error("Error processing new lineage edge {} -> {}: {}", 
                edge.getFromTable(), edge.getToTable(), e.getMessage(), e);
        }
    }

    @PostUpdate
    public void onLineageEdgeUpdate(LineageEdge edge) {
        logger.info("Lineage edge updated: {} -> {}", edge.getFromTable(), edge.getToTable());
        try {
            LineageProcessingService service = ApplicationContextProvider.getBean(LineageProcessingService.class);
            service.processUpdatedLineageEdge(edge);
        } catch (Exception e) {
            logger.error("Error processing updated lineage edge {} -> {}: {}", 
                edge.getFromTable(), edge.getToTable(), e.getMessage(), e);
        }
    }
}
