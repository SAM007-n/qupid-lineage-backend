package com.lineage.repository;

import com.lineage.entity.Lineage;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface LineageRepository extends JpaRepository<Lineage, UUID> {
    List<Lineage> findByExtractionRunRunId(UUID runId);
    List<Lineage> findByToAssetAssetId(UUID assetId);
    List<Lineage> findByFromAssetAssetId(UUID assetId);
    
    // New methods to find lineage by table name across all runs
    List<Lineage> findByToAssetShortNameIgnoreCase(String shortName);
    List<Lineage> findByFromAssetShortNameIgnoreCase(String shortName);
}


