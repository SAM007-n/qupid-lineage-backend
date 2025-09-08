package com.lineage.repository;

import com.lineage.entity.FineGrainedLineage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface FineGrainedLineageRepository extends JpaRepository<FineGrainedLineage, UUID> {

    List<FineGrainedLineage> findByAggregatedTableAggregatedTableIdOrderByDownstreamColumn(UUID aggregatedTableId);

    List<FineGrainedLineage> findByDownstreamTable(String downstreamTable);

    List<FineGrainedLineage> findByDownstreamTableAndDownstreamColumn(String downstreamTable, String downstreamColumn);

    @Query("SELECT fgl FROM FineGrainedLineage fgl WHERE fgl.aggregatedTable.extractionRun.runId = :runId")
    List<FineGrainedLineage> findByRunId(@Param("runId") UUID runId);

    @Query("SELECT fgl FROM FineGrainedLineage fgl WHERE fgl.aggregatedTable.extractionRun.runId = :runId AND fgl.downstreamTable = :tableName")
    List<FineGrainedLineage> findByRunIdAndDownstreamTable(@Param("runId") UUID runId, @Param("tableName") String tableName);

    @Query("SELECT COUNT(fgl) FROM FineGrainedLineage fgl WHERE fgl.aggregatedTable.extractionRun.runId = :runId")
    long countByRunId(@Param("runId") UUID runId);

    @Query("SELECT COUNT(fgl) FROM FineGrainedLineage fgl WHERE fgl.downstreamTable = :tableName")
    long countByDownstreamTable(@Param("tableName") String tableName);

    @Query("SELECT DISTINCT fgl.downstreamTable FROM FineGrainedLineage fgl WHERE fgl.aggregatedTable.extractionRun.runId = :runId")
    List<String> findDistinctDownstreamTablesByRunId(@Param("runId") UUID runId);
}
