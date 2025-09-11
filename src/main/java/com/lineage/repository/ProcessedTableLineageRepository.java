package com.lineage.repository;

import com.lineage.entity.ProcessedTableLineage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ProcessedTableLineageRepository extends JpaRepository<ProcessedTableLineage, UUID> {

    // Find table lineages for a run, ordered by table name
    @Query("SELECT ptl FROM ProcessedTableLineage ptl WHERE ptl.extractionRun.runId = :runId ORDER BY ptl.tableName")
    List<ProcessedTableLineage> findByExtractionRunRunIdOrderByTableName(@Param("runId") UUID runId);

    // Find specific table lineage by run ID and table name
    @Query("SELECT ptl FROM ProcessedTableLineage ptl WHERE ptl.extractionRun.runId = :runId AND ptl.tableName = :tableName")
    Optional<ProcessedTableLineage> findByExtractionRunRunIdAndTableName(@Param("runId") UUID runId, @Param("tableName") String tableName);

    // Count table lineages for a run
    @Query("SELECT COUNT(ptl) FROM ProcessedTableLineage ptl WHERE ptl.extractionRun.runId = :runId")
    long countByRunId(@Param("runId") UUID runId);

    // Find tables that have upstream dependencies (are targets)
    @Query("SELECT ptl FROM ProcessedTableLineage ptl WHERE ptl.extractionRun.runId = :runId " +
           "AND JSON_LENGTH(ptl.upstreamTables) > 0 ORDER BY ptl.tableName")
    List<ProcessedTableLineage> findTablesWithUpstreamDependencies(@Param("runId") UUID runId);

    // Find tables that have downstream dependencies (are sources)
    @Query("SELECT ptl FROM ProcessedTableLineage ptl WHERE ptl.extractionRun.runId = :runId " +
           "AND JSON_LENGTH(ptl.downstreamTables) > 0 ORDER BY ptl.tableName")
    List<ProcessedTableLineage> findTablesWithDownstreamDependencies(@Param("runId") UUID runId);

    // Delete all table lineages for a run (for reprocessing)
    void deleteByExtractionRunRunId(UUID runId);
}
