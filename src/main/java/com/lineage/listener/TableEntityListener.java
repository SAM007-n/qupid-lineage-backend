package com.lineage.listener;

import com.lineage.config.ApplicationContextProvider;
import com.lineage.entity.TableEntity;
import com.lineage.service.LineageProcessingService;
import jakarta.persistence.PostPersist;
import jakarta.persistence.PostUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TableEntityListener: JPA callback listener used to trigger real-time processing
 * whenever a raw `tables` row is inserted or updated.
 */
public class TableEntityListener {

    private static final Logger logger = LoggerFactory.getLogger(TableEntityListener.class);

    @PostPersist
    public void onTableInsert(TableEntity table) {
        logger.info("Table inserted: {}", table.getTableName());
        try {
            LineageProcessingService service = ApplicationContextProvider.getBean(LineageProcessingService.class);
            service.processNewTable(table);
        } catch (Exception e) {
            logger.error("Error processing new table {}: {}", table.getTableName(), e.getMessage(), e);
        }
    }

    @PostUpdate
    public void onTableUpdate(TableEntity table) {
        logger.info("Table updated: {}", table.getTableName());
        try {
            LineageProcessingService service = ApplicationContextProvider.getBean(LineageProcessingService.class);
            service.processUpdatedTable(table);
        } catch (Exception e) {
            logger.error("Error processing updated table {}: {}", table.getTableName(), e.getMessage(), e);
        }
    }
}
