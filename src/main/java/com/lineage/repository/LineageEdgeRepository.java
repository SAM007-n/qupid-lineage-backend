package com.lineage.repository;

import com.lineage.entity.LineageEdge;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface LineageEdgeRepository extends JpaRepository<LineageEdge, UUID> {

    List<LineageEdge> findByFileFileIdOrderByCreatedAt(UUID fileId);

    List<LineageEdge> findByFromTable(String fromTable);

    List<LineageEdge> findByToTable(String toTable);

    @Query("SELECT le FROM LineageEdge le WHERE le.file.extractionRun.runId = :runId")
    List<LineageEdge> findByRunId(@Param("runId") UUID runId);

    @Query("SELECT le FROM LineageEdge le WHERE le.edgeType = :edgeType AND le.file.extractionRun.runId = :runId")
    List<LineageEdge> findByEdgeTypeAndRunId(@Param("edgeType") LineageEdge.EdgeType edgeType, @Param("runId") UUID runId);

    @Query("SELECT le FROM LineageEdge le WHERE le.fromTable = :tableName OR le.toTable = :tableName")
    List<LineageEdge> findByTableName(@Param("tableName") String tableName);

    @Query("SELECT le FROM LineageEdge le WHERE le.transformationType = :transformationType")
    List<LineageEdge> findByTransformationType(@Param("transformationType") String transformationType);

    @Query("SELECT COUNT(le) FROM LineageEdge le WHERE le.file.extractionRun.runId = :runId")
    long countByRunId(@Param("runId") UUID runId);

    @Query("SELECT COUNT(le) FROM LineageEdge le WHERE le.edgeType = :edgeType AND le.file.extractionRun.runId = :runId")
    long countByEdgeTypeAndRunId(@Param("edgeType") LineageEdge.EdgeType edgeType, @Param("runId") UUID runId);
} 