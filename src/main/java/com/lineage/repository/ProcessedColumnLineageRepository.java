package com.lineage.repository;

import com.lineage.entity.ProcessedColumnLineage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ProcessedColumnLineageRepository extends JpaRepository<ProcessedColumnLineage, UUID> {

    // Find column lineages for a specific processed table
    @Query("SELECT pcl FROM ProcessedColumnLineage pcl WHERE pcl.processedTable.processedTableId = :processedTableId ORDER BY pcl.downstreamColumn")
    List<ProcessedColumnLineage> findByProcessedTableIdOrderByDownstreamColumn(@Param("processedTableId") UUID processedTableId);

    // Find column lineages by run ID and downstream table
    @Query("SELECT pcl FROM ProcessedColumnLineage pcl WHERE pcl.extractionRun.runId = :runId AND pcl.downstreamTable = :tableName ORDER BY pcl.downstreamColumn")
    List<ProcessedColumnLineage> findByRunIdAndDownstreamTable(@Param("runId") UUID runId, @Param("tableName") String tableName);

    // Find column lineages by run ID and upstream table
    @Query("SELECT pcl FROM ProcessedColumnLineage pcl WHERE pcl.extractionRun.runId = :runId AND pcl.upstreamTable = :tableName ORDER BY pcl.upstreamColumn")
    List<ProcessedColumnLineage> findByRunIdAndUpstreamTable(@Param("runId") UUID runId, @Param("tableName") String tableName);

    // Find all column lineages for a run
    @Query("SELECT pcl FROM ProcessedColumnLineage pcl WHERE pcl.extractionRun.runId = :runId ORDER BY pcl.downstreamTable, pcl.downstreamColumn")
    List<ProcessedColumnLineage> findByRunIdOrderByDownstreamTable(@Param("runId") UUID runId);

    // Count column lineages for a run
    @Query("SELECT COUNT(pcl) FROM ProcessedColumnLineage pcl WHERE pcl.extractionRun.runId = :runId")
    long countByRunId(@Param("runId") UUID runId);

    // Find specific column lineage
    @Query("SELECT pcl FROM ProcessedColumnLineage pcl WHERE pcl.extractionRun.runId = :runId " +
           "AND pcl.downstreamTable = :downstreamTable AND pcl.downstreamColumn = :downstreamColumn " +
           "AND pcl.upstreamTable = :upstreamTable AND pcl.upstreamColumn = :upstreamColumn")
    List<ProcessedColumnLineage> findByRunIdAndColumns(@Param("runId") UUID runId,
                                                       @Param("downstreamTable") String downstreamTable,
                                                       @Param("downstreamColumn") String downstreamColumn,
                                                       @Param("upstreamTable") String upstreamTable,
                                                       @Param("upstreamColumn") String upstreamColumn);

    // Delete all column lineages for a run (for reprocessing)
    void deleteByExtractionRunRunId(UUID runId);
}
