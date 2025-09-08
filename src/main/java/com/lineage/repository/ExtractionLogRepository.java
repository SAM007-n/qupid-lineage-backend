package com.lineage.repository;

import com.lineage.entity.ExtractionLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface ExtractionLogRepository extends JpaRepository<ExtractionLog, UUID> {
    
    /**
     * Find logs for a specific run ID, ordered by timestamp
     */
    List<ExtractionLog> findByRunIdOrderByTimestampAsc(UUID runId);
    
    /**
     * Find logs for a specific run ID with pagination
     */
    Page<ExtractionLog> findByRunIdOrderByTimestampDesc(UUID runId, Pageable pageable);
    
    /**
     * Find recent logs for a run (last N entries)
     */
    @Query("SELECT l FROM ExtractionLog l WHERE l.runId = :runId ORDER BY l.timestamp DESC")
    List<ExtractionLog> findRecentLogsByRunId(@Param("runId") UUID runId, Pageable pageable);
    
    /**
     * Find logs since a specific timestamp
     */
    List<ExtractionLog> findByRunIdAndTimestampAfterOrderByTimestampAsc(UUID runId, LocalDateTime since);
    
    /**
     * Count logs for a run
     */
    long countByRunId(UUID runId);
    
    /**
     * Delete logs older than specified date
     */
    void deleteByTimestampBefore(LocalDateTime cutoff);
}
