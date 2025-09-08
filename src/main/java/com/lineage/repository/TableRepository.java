package com.lineage.repository;

import com.lineage.entity.TableEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface TableRepository extends JpaRepository<TableEntity, UUID> {

    List<TableEntity> findByFileFileIdOrderByTableName(UUID fileId);

    List<TableEntity> findByTableName(String tableName);

    List<TableEntity> findByTableRole(TableEntity.TableRole tableRole);

    @Query("SELECT t FROM TableEntity t WHERE t.file.extractionRun.runId = :runId")
    List<TableEntity> findByRunId(@Param("runId") UUID runId);

    @Query("SELECT t FROM TableEntity t WHERE t.tableRole = :role AND t.file.extractionRun.runId = :runId")
    List<TableEntity> findByRoleAndRunId(@Param("role") TableEntity.TableRole role, @Param("runId") UUID runId);

    @Query("SELECT t FROM TableEntity t WHERE t.tableSchema = :schema")
    List<TableEntity> findByTableSchema(@Param("schema") String schema);

    @Query("SELECT COUNT(t) FROM TableEntity t WHERE t.file.extractionRun.runId = :runId")
    long countByRunId(@Param("runId") UUID runId);

    @Query("SELECT COUNT(t) FROM TableEntity t WHERE t.tableRole = :role AND t.file.extractionRun.runId = :runId")
    long countByRoleAndRunId(@Param("role") TableEntity.TableRole role, @Param("runId") UUID runId);
} 