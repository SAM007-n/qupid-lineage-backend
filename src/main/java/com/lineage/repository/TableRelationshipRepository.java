package com.lineage.repository;

import com.lineage.entity.TableRelationship;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface TableRelationshipRepository extends JpaRepository<TableRelationship, UUID> {

    List<TableRelationship> findByExtractionRunRunIdOrderByTableName(UUID runId);

    Optional<TableRelationship> findByExtractionRunRunIdAndTableName(UUID runId, String tableName);

    List<TableRelationship> findByTableName(String tableName);

    @Query("SELECT tr FROM TableRelationship tr WHERE tr.extractionRun.runId = :runId AND SIZE(tr.upstreamTables) > 0")
    List<TableRelationship> findByRunIdWithUpstreams(@Param("runId") UUID runId);

    @Query("SELECT tr FROM TableRelationship tr WHERE tr.extractionRun.runId = :runId AND SIZE(tr.downstreamTables) > 0")
    List<TableRelationship> findByRunIdWithDownstreams(@Param("runId") UUID runId);

    @Query("SELECT COUNT(tr) FROM TableRelationship tr WHERE tr.extractionRun.runId = :runId")
    long countByRunId(@Param("runId") UUID runId);

    @Query("SELECT COUNT(tr) FROM TableRelationship tr WHERE tr.extractionRun.runId = :runId AND SIZE(tr.upstreamTables) > 0")
    long countByRunIdWithUpstreams(@Param("runId") UUID runId);

    @Query("SELECT COUNT(tr) FROM TableRelationship tr WHERE tr.extractionRun.runId = :runId AND SIZE(tr.downstreamTables) > 0")
    long countByRunIdWithDownstreams(@Param("runId") UUID runId);

    @Query("SELECT DISTINCT tr.tableName FROM TableRelationship tr WHERE tr.extractionRun.runId = :runId")
    List<String> findDistinctTableNamesByRunId(@Param("runId") UUID runId);
}
