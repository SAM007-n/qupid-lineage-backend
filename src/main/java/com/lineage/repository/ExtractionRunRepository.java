package com.lineage.repository;

import com.lineage.entity.ExtractionRun;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface ExtractionRunRepository extends JpaRepository<ExtractionRun, UUID> {

    List<ExtractionRun> findByRepositoryUrlOrderByCreatedAtDesc(String repositoryUrl);

    List<ExtractionRun> findByPhaseOrderByCreatedAtDesc(ExtractionRun.ExtractionPhase phase);

    @Query("SELECT er FROM ExtractionRun er WHERE er.createdAt >= :since ORDER BY er.createdAt DESC")
    List<ExtractionRun> findRecentRuns(@Param("since") LocalDateTime since);

    @Query("SELECT er FROM ExtractionRun er WHERE er.repositoryUrl = :repositoryUrl AND er.branch = :branch ORDER BY er.createdAt DESC")
    List<ExtractionRun> findByRepositoryAndBranch(@Param("repositoryUrl") String repositoryUrl, @Param("branch") String branch);

    @Query("SELECT COUNT(er) FROM ExtractionRun er WHERE er.phase = :phase")
    long countByPhase(@Param("phase") ExtractionRun.ExtractionPhase phase);

    @Query("SELECT er FROM ExtractionRun er WHERE er.triggeredBy = :triggeredBy ORDER BY er.createdAt DESC")
    List<ExtractionRun> findByTriggeredBy(@Param("triggeredBy") String triggeredBy);
} 