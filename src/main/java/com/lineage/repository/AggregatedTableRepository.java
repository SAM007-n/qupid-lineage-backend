package com.lineage.repository;

import com.lineage.entity.AggregatedTable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface AggregatedTableRepository extends JpaRepository<AggregatedTable, UUID> {

    List<AggregatedTable> findByExtractionRunRunIdOrderByEntityName(UUID runId);

    Optional<AggregatedTable> findByExtractionRunRunIdAndEntityId(UUID runId, String entityId);

    List<AggregatedTable> findByEntityName(String entityName);

    List<AggregatedTable> findBySource(String source);

    @Query("SELECT at FROM AggregatedTable at WHERE at.extractionRun.runId = :runId AND at.entityType = :entityType")
    List<AggregatedTable> findByRunIdAndEntityType(@Param("runId") UUID runId, @Param("entityType") String entityType);

    @Query("SELECT at FROM AggregatedTable at WHERE at.extractionRun.runId = :runId AND at.source = :source")
    List<AggregatedTable> findByRunIdAndSource(@Param("runId") UUID runId, @Param("source") String source);

    @Query("SELECT COUNT(at) FROM AggregatedTable at WHERE at.extractionRun.runId = :runId")
    long countByRunId(@Param("runId") UUID runId);

    @Query("SELECT COUNT(at) FROM AggregatedTable at WHERE at.extractionRun.runId = :runId AND at.source = :source")
    long countByRunIdAndSource(@Param("runId") UUID runId, @Param("source") String source);

    @Query("SELECT DISTINCT at.source FROM AggregatedTable at WHERE at.extractionRun.runId = :runId AND at.source IS NOT NULL")
    List<String> findDistinctSourcesByRunId(@Param("runId") UUID runId);

    @Query("SELECT at FROM AggregatedTable at WHERE at.extractionRun.runId = :runId AND at.columnsCount > :minColumns")
    List<AggregatedTable> findByRunIdAndMinColumns(@Param("runId") UUID runId, @Param("minColumns") Integer minColumns);
}
