package com.lineage.repository;

import com.lineage.entity.Asset;
import com.lineage.entity.AssetColumn;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface AssetColumnRepository extends JpaRepository<AssetColumn, UUID> {
    List<AssetColumn> findByAssetAssetId(UUID assetId);
    List<AssetColumn> findByAssetAssetIdAndRole(UUID assetId, Asset.Role role);
    void deleteByAssetAssetId(UUID assetId);
}


