package com.lineage.repository;

import com.lineage.entity.AssetFile;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface AssetFileRepository extends JpaRepository<AssetFile, UUID> {
    List<AssetFile> findByExtractionRunRunId(UUID runId);
    List<AssetFile> findByAssetAssetId(UUID assetId);
}


