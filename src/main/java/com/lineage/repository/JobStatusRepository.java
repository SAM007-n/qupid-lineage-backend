package com.lineage.repository;

import com.lineage.entity.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface JobStatusRepository extends JpaRepository<JobStatus, UUID> {

    List<JobStatus> findByExtractionRunRunIdOrderByLastUpdatedDesc(UUID runId);

    Optional<JobStatus> findFirstByExtractionRunRunIdOrderByLastUpdatedDesc(UUID runId);

    List<JobStatus> findByStatus(JobStatus.JobStatusEnum status);

    @Query("SELECT js FROM JobStatus js WHERE js.extractionRun.runId = :runId AND js.status = :status")
    List<JobStatus> findByRunIdAndStatus(@Param("runId") UUID runId, @Param("status") JobStatus.JobStatusEnum status);

    @Query("SELECT js FROM JobStatus js WHERE js.podId = :podId ORDER BY js.lastUpdated DESC")
    List<JobStatus> findByPodId(@Param("podId") String podId);

    @Query("SELECT js FROM JobStatus js WHERE js.status = :status AND js.lastUpdated < :since")
    List<JobStatus> findStaleJobs(@Param("status") JobStatus.JobStatusEnum status, @Param("since") java.time.LocalDateTime since);

    @Query("SELECT COUNT(js) FROM JobStatus js WHERE js.status = :status")
    long countByStatus(@Param("status") JobStatus.JobStatusEnum status);
} 