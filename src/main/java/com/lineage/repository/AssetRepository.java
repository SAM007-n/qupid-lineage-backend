package com.lineage.repository;

import com.lineage.entity.Asset;
import com.lineage.entity.ExtractionRun;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface AssetRepository extends JpaRepository<Asset, UUID> {
    List<Asset> findByExtractionRunRunId(UUID runId);
    Asset findFirstByExtractionRunRunIdAndFullName(UUID runId, String fullName);
    Asset findFirstByExtractionRunRunIdAndShortName(UUID runId, String shortName);

    // For selecting the most recent asset snapshot when run id is not provided
    List<Asset> findByShortNameIgnoreCase(String shortName);
    Asset findTopByShortNameIgnoreCaseOrderByCreatedAtDesc(String shortName);
}


