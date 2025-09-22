package com.lineage.controller;

import com.lineage.dto.AssetDetailDto;
import com.lineage.dto.AssetSummaryDto;
import com.lineage.dto.LineageDirection;
import com.lineage.dto.LineageResponseDto;
import com.lineage.service.LineageApiService;
import com.lineage.repository.AssetRepository;
import com.lineage.repository.AssetFileRepository;
import com.lineage.repository.LineageRepository;
import com.lineage.entity.Asset;
import com.lineage.entity.AssetFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping("/assets")
@CrossOrigin(origins = "*")
public class AssetsController {

    private static final Logger logger = LoggerFactory.getLogger(AssetsController.class);

    @Autowired private AssetRepository assetRepository;
    @Autowired private AssetFileRepository assetFileRepository;
    @Autowired private LineageRepository lineageRepository;
    @Autowired private LineageApiService lineageApiService;

    @GetMapping("")
    public ResponseEntity<Map<String, Object>> listAssets(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String q) {
        try {
            List<Asset> all = assetRepository.findAll();
            if (q != null && !q.isBlank()) {
                String query = q.toLowerCase();
                all.removeIf(a -> (a.getShortName() == null || !a.getShortName().toLowerCase().contains(query)) &&
                        (a.getFullName() == null || !a.getFullName().toLowerCase().contains(query)));
            }
            int from = Math.min(page * size, all.size());
            int to = Math.min(from + size, all.size());
            List<Asset> pageItems = all.subList(from, to);

            List<AssetSummaryDto> items = new ArrayList<>();
            for (Asset a : pageItems) {
                AssetSummaryDto dto = new AssetSummaryDto();
                dto.setAssetId(a.getAssetId());
                dto.setShortName(a.getShortName());
                dto.setFullName(a.getFullName());
                dto.setSchemaName(a.getSchemaName());
                dto.setRole(a.getRole() != null ? a.getRole().name() : null);
                dto.setUpstreamCount(lineageRepository.findByToAssetAssetId(a.getAssetId()).size());
                dto.setDownstreamCount(lineageRepository.findByFromAssetAssetId(a.getAssetId()).size());
                // best-effort first/last seen via AssetFile
                List<AssetFile> links = assetFileRepository.findByAssetAssetId(a.getAssetId());
                if (!links.isEmpty()) {
                    links.sort(Comparator.comparing(af -> af.getCreatedAt()));
                    dto.setFirstSeenAt(links.get(0).getCreatedAt());
                    dto.setLastSeenAt(links.get(links.size()-1).getCreatedAt());
                }
                items.add(dto);
            }

            Map<String, Object> resp = new HashMap<>();
            resp.put("items", items);
            resp.put("totalElements", all.size());
            resp.put("totalPages", (int) Math.ceil((double) all.size() / size));
            resp.put("currentPage", page);
            resp.put("pageSize", size);
            return ResponseEntity.ok(resp);
        } catch (Exception e) {
            logger.error("Failed to list assets: {}", e.getMessage(), e);
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/{assetId}")
    public ResponseEntity<AssetDetailDto> getAsset(@PathVariable UUID assetId) {
        try {
            Asset a = assetRepository.findById(assetId).orElse(null);
            if (a == null) return ResponseEntity.notFound().build();
            // reuse existing service to build details similar to entity endpoint
            var entity = lineageApiService.getEntity(a.getShortName());
            AssetDetailDto dto = new AssetDetailDto();
            dto.setAssetId(a.getAssetId());
            dto.setShortName(entity.getEntityId());
            dto.setFullName(entity.getEntityName());
            dto.setSchemaName(a.getSchemaName());
            dto.setRole(a.getRole() != null ? a.getRole().name() : null);
            dto.setColumnCount(entity.getColumnCount());
            dto.setUpstreamCount(entity.getUpstreamCount());
            dto.setDownstreamCount(entity.getDownstreamCount());
            dto.setHasUpstream(entity.getHasUpstream() != null ? entity.getHasUpstream() : false);
            dto.setHasDownstream(entity.getHasDownstream() != null ? entity.getHasDownstream() : false);
            dto.setSchemaMetadata(entity.getSchemaMetadata());
            return ResponseEntity.ok(dto);
        } catch (Exception e) {
            logger.error("Failed to get asset {}: {}", assetId, e.getMessage(), e);
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/{assetId}/lineage")
    public ResponseEntity<LineageResponseDto> getAssetLineage(
            @PathVariable UUID assetId,
            @RequestParam LineageDirection direction) {
        try {
            Asset a = assetRepository.findById(assetId).orElse(null);
            if (a == null) return ResponseEntity.notFound().build();
            LineageResponseDto resp = lineageApiService.getLineage(a.getShortName(), direction);
            return ResponseEntity.ok(resp);
        } catch (Exception e) {
            logger.error("Failed to get asset lineage {}: {}", assetId, e.getMessage(), e);
            return ResponseEntity.badRequest().build();
        }
    }
}