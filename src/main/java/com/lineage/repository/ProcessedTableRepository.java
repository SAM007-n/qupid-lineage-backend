package com.lineage.repository;

import com.lineage.entity.ProcessedTable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ProcessedTableRepository extends JpaRepository<ProcessedTable, UUID> {

    // Find all processed tables for a run, ordered by entity name
    @Query("SELECT pt FROM ProcessedTable pt WHERE pt.extractionRun.runId = :runId ORDER BY pt.entityName")
    List<ProcessedTable> findByExtractionRunRunIdOrderByEntityName(@Param("runId") UUID runId);

    // Find processed table by run ID and entity ID
    @Query("SELECT pt FROM ProcessedTable pt WHERE pt.extractionRun.runId = :runId AND pt.entityId = :entityId")
    Optional<ProcessedTable> findByExtractionRunRunIdAndEntityId(@Param("runId") UUID runId, @Param("entityId") String entityId);

    // Find processed tables by source for a run
    @Query("SELECT pt FROM ProcessedTable pt WHERE pt.extractionRun.runId = :runId AND pt.source = :source ORDER BY pt.entityName")
    List<ProcessedTable> findByRunIdAndSource(@Param("runId") UUID runId, @Param("source") String source);

    // Count processed tables for a run
    @Query("SELECT COUNT(pt) FROM ProcessedTable pt WHERE pt.extractionRun.runId = :runId")
    long countByRunId(@Param("runId") UUID runId);

    // Get distinct sources for a run
    @Query("SELECT DISTINCT pt.source FROM ProcessedTable pt WHERE pt.extractionRun.runId = :runId AND pt.source IS NOT NULL ORDER BY pt.source")
    List<String> findDistinctSourcesByRunId(@Param("runId") UUID runId);

    // Delete all processed tables for a run (for reprocessing)
    void deleteByExtractionRunRunId(UUID runId);
}
