package com.lineage.repository;

import com.lineage.entity.File;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface FileRepository extends JpaRepository<File, UUID> {

    List<File> findByExtractionRunRunIdOrderByFilePath(UUID runId);

    List<File> findByFileHash(String fileHash);

    Optional<File> findByExtractionRunRunIdAndFilePath(UUID runId, String filePath);

    @Query("SELECT f FROM File f WHERE f.extractionRun.runId = :runId AND f.status = :status")
    List<File> findByRunIdAndStatus(@Param("runId") UUID runId, @Param("status") File.FileStatus status);

    @Query("SELECT COUNT(f) FROM File f WHERE f.extractionRun.runId = :runId")
    long countByRunId(@Param("runId") UUID runId);

    @Query("SELECT COUNT(f) FROM File f WHERE f.extractionRun.runId = :runId AND f.status = :status")
    long countByRunIdAndStatus(@Param("runId") UUID runId, @Param("status") File.FileStatus status);

    @Query("SELECT f FROM File f WHERE f.fileType = :fileType ORDER BY f.createdAt DESC")
    List<File> findByFileType(@Param("fileType") File.FileType fileType);
} 