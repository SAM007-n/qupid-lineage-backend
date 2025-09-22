package com.lineage.service;

import com.lineage.dto.*;
import com.lineage.entity.Asset;
import com.lineage.entity.Lineage;
import com.lineage.repository.AssetRepository;
import com.lineage.repository.AssetColumnRepository;
import com.lineage.repository.LineageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * LineageApiService provides search functionality using the normalized asset schema.
 * Legacy lineage endpoints are temporarily disabled while migrating to the new schema.
 */
@Service
public class LineageApiService {

    private static final Logger logger = LoggerFactory.getLogger(LineageApiService.class);

    @Autowired
    private AssetRepository assetRepository;

    @Autowired
    private LineageRepository lineageRepository;

    @Autowired
    private AssetColumnRepository assetColumnRepository;

    public EdgeDetailsDto getEdgeDetails(String edgeId) {
        try {
            java.util.UUID id = java.util.UUID.fromString(edgeId);
            var opt = lineageRepository.findById(id);
            if (opt.isEmpty()) throw new RuntimeException("Edge not found: " + edgeId);
            var e = opt.get();
            EdgeDetailsDto dto = new EdgeDetailsDto();
            dto.setEdgeId(edgeId);
            dto.setEdgeType(e.getEdgeType());
            dto.setFromEntity(e.getFromAsset() != null ? e.getFromAsset().getShortName() : null);
            dto.setToEntity(e.getToAsset() != null ? e.getToAsset().getShortName() : null);
            dto.setFromColumn(e.getFromColumn());
            dto.setToColumn(e.getToColumn());
            dto.setTransformationType(e.getTransformationType());
            dto.setFileId(e.getFile() != null ? e.getFile().getFilePath() : null);
            if (e.getStartLine() != null || e.getEndLine() != null) {
                dto.setLines(new TransformationLinesDto(e.getStartLine(), e.getEndLine()));
            }
            return dto;
        } catch (IllegalArgumentException ex) {
            throw new RuntimeException("Invalid edgeId: " + edgeId);
        }
    }

    /**
     * Get lineage (upstream/downstream) for a specific entity
     * Uses table name-based queries to find all relationships across extraction runs
     */
    public LineageResponseDto getLineage(String entityId, LineageDirection direction) {
        logger.info("Getting {} lineage for entity: {}", direction, entityId);
        Asset center = assetRepository.findTopByShortNameIgnoreCaseOrderByCreatedAtDesc(entityId);
        if (center == null) throw new RuntimeException("Entity not found: " + entityId);

        // Fetch edges for direction by table name across all runs to ensure complete lineage
        List<Lineage> edges = (direction == LineageDirection.upstream)
                ? lineageRepository.findByToAssetShortNameIgnoreCase(entityId)
                : lineageRepository.findByFromAssetShortNameIgnoreCase(entityId);

        Map<String, List<Lineage>> grouped = new LinkedHashMap<>();
        for (Lineage e : edges) {
            String neighborShort = (direction == LineageDirection.upstream)
                    ? e.getFromAsset().getShortName()
                    : e.getToAsset().getShortName();
            grouped.computeIfAbsent(neighborShort, k -> new ArrayList<>()).add(e);
        }

        List<LineageNodeDto> nodes = new ArrayList<>();
        for (Map.Entry<String, List<Lineage>> entry : grouped.entrySet()) {
            String shortName = entry.getKey();
            List<Lineage> groupEdges = entry.getValue();
            // Use the first edge to get full name
            String fullName = (direction == LineageDirection.upstream)
                    ? groupEdges.get(0).getFromAsset().getFullName()
                    : groupEdges.get(0).getToAsset().getFullName();

            LineageNodeDto node = new LineageNodeDto(shortName, fullName, "table");

            // Aggregate transformations per neighbor, dedup by file + line range + type
            List<TransformationDto> transformations = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            for (Lineage e : groupEdges) {
                String fileId = e.getFile() != null ? e.getFile().getFilePath() : null;
                Integer s = e.getStartLine();
                Integer en = e.getEndLine();
                String t = e.getTransformationType();
                String key = (fileId == null ? "" : fileId) + "|" + (s == null ? "" : s) + "|" + (en == null ? "" : en) + "|" + (t == null ? "" : t);
                if (seen.contains(key)) continue;
                seen.add(key);
                TransformationLinesDto lines = (s != null || en != null) ? new TransformationLinesDto(s, en) : null;
                transformations.add(new TransformationDto(fileId, t, lines));
            }
            node.setTransformations(transformations.isEmpty() ? null : transformations);
            nodes.add(node);
        }

        // Build fine-grained (column) lineage
        List<FineGrainedLineageDto> fine = new ArrayList<>();
        Set<String> fgSeen = new HashSet<>();
        for (Lineage e : edges) {
            if (e.getEdgeType() == null || !"column_edge".equalsIgnoreCase(e.getEdgeType())) continue;
            String fromCol = e.getFromColumn();
            String toCol = e.getToColumn();
            if ((fromCol == null || fromCol.isBlank()) && (toCol == null || toCol.isBlank())) continue;

            List<FieldRefDto> upstreams = new ArrayList<>();
            List<FieldRefDto> downstreams = new ArrayList<>();

            String fileId = e.getFile() != null ? e.getFile().getFilePath() : null;
            Integer s = e.getStartLine();
            Integer en = e.getEndLine();
            TransformationLinesDto linesDto = (s != null || en != null) ? new TransformationLinesDto(s, en) : null;
            TransformationDto tDto = new TransformationDto(fileId, e.getTransformationType(), linesDto);

            if (direction == LineageDirection.upstream) {
                // center is target; show upstream columns feeding center
                FieldRefDto up = new FieldRefDto(e.getFromAsset().getShortName(), fromCol);
                up.setTransformation(tDto);
                upstreams.add(up);
                downstreams.add(new FieldRefDto(center.getShortName(), toCol));
            } else {
                // center is source; show downstream columns produced from center
                FieldRefDto up = new FieldRefDto(center.getShortName(), fromCol);
                up.setTransformation(tDto);
                upstreams.add(up);
                downstreams.add(new FieldRefDto(e.getToAsset().getShortName(), toCol));
            }

            String key = (upstreams.isEmpty()?"":upstreams.get(0).getUrn()+"|"+upstreams.get(0).getPath())+
                    "->"+
                    (downstreams.isEmpty()?"":downstreams.get(0).getUrn()+"|"+downstreams.get(0).getPath());
            if (fgSeen.add(key)) {
                fine.add(new FineGrainedLineageDto(upstreams, downstreams));
            }
        }

        LineageResponseDto resp = new LineageResponseDto(center.getShortName(), center.getFullName(), "table", direction, nodes);
        if (!fine.isEmpty()) resp.setFineGrainedLineages(fine);
        return resp;
    }

    /**
     * Get entity details from normalized schema
     */
    public EntityDto getEntity(String entityId) {
        logger.info("Getting entity details for: {}", entityId);
        Asset asset = assetRepository.findTopByShortNameIgnoreCaseOrderByCreatedAtDesc(entityId);
        if (asset == null) throw new RuntimeException("Entity not found: " + entityId);

        int upstream = lineageRepository.findByToAssetAssetId(asset.getAssetId()).size();
        int downstream = lineageRepository.findByFromAssetAssetId(asset.getAssetId()).size();

        int colCount = 0;
        List<SchemaFieldDto> fields = new ArrayList<>();
        try {
            // Only retrieve TARGET columns for display, but keep SOURCE columns in database
            var cols = assetColumnRepository.findByAssetAssetIdAndRole(asset.getAssetId(), Asset.Role.TARGET);
            colCount = cols.size();
            for (var c : cols) fields.add(new SchemaFieldDto(c.getColumnName(), "varchar"));
        } catch (Exception ignore) {}

        // Build fine-grained (column) lineage for this entity (both directions)
        List<FineGrainedLineageDto> fine = new ArrayList<>();
        Set<String> fgSeen = new HashSet<>();
        List<Lineage> allEdges = new ArrayList<>();
        try {
            allEdges.addAll(lineageRepository.findByToAssetAssetId(asset.getAssetId()));
            allEdges.addAll(lineageRepository.findByFromAssetAssetId(asset.getAssetId()));
        } catch (Exception ignore) {}

        for (Lineage e : allEdges) {
            if (e.getEdgeType() == null || !"column_edge".equalsIgnoreCase(e.getEdgeType())) continue;
            String fromCol = e.getFromColumn();
            String toCol = e.getToColumn();
            if ((fromCol == null || fromCol.isBlank()) && (toCol == null || toCol.isBlank())) continue;

            List<FieldRefDto> upstreams = new ArrayList<>();
            List<FieldRefDto> downstreams = new ArrayList<>();

            String fileId = e.getFile() != null ? e.getFile().getFilePath() : null;
            Integer s = e.getStartLine();
            Integer en = e.getEndLine();
            TransformationLinesDto linesDto = (s != null || en != null) ? new TransformationLinesDto(s, en) : null;
            TransformationDto tDto = new TransformationDto(fileId, e.getTransformationType(), linesDto);

            if (e.getToAsset() != null && e.getToAsset().getAssetId().equals(asset.getAssetId())) {
                // upstream into this asset
                FieldRefDto up = new FieldRefDto(e.getFromAsset().getShortName(), fromCol);
                up.setTransformation(tDto);
                upstreams.add(up);
                downstreams.add(new FieldRefDto(asset.getShortName(), toCol));
            } else {
                // downstream from this asset
                FieldRefDto up = new FieldRefDto(asset.getShortName(), fromCol);
                up.setTransformation(tDto);
                upstreams.add(up);
                downstreams.add(new FieldRefDto(e.getToAsset().getShortName(), toCol));
            }

            String key = (upstreams.isEmpty()?"":upstreams.get(0).getUrn()+"|"+upstreams.get(0).getPath())+
                    "->"+
                    (downstreams.isEmpty()?"":downstreams.get(0).getUrn()+"|"+downstreams.get(0).getPath());
            if (fgSeen.add(key)) {
                fine.add(new FineGrainedLineageDto(upstreams, downstreams));
            }
        }

        EntityDto dto = new EntityDto();
        dto.setEntityId(asset.getShortName());
        dto.setEntityName(asset.getFullName());
        dto.setEntityType("table");
        dto.setColumnCount(colCount);
        dto.setSource(asset.getSchemaName());
        dto.setUpstreamCount(upstream);
        dto.setDownstreamCount(downstream);
        dto.setHasUpstream(upstream > 0);
        dto.setHasDownstream(downstream > 0);
        dto.setSchemaMetadata(new SchemaMetadataDto(fields));
        if (!fine.isEmpty()) dto.setFineGrainedLineages(fine);
        return dto;
    }

    /**
     * Get multiple entity details
     */
    public List<EntityDto> getBulkEntities(List<String> entityIds) {
        logger.info("Getting bulk entity details for {} entities", entityIds == null ? 0 : entityIds.size());
        if (entityIds == null || entityIds.isEmpty()) return List.of();
        List<EntityDto> out = new ArrayList<>();
        for (String id : entityIds) {
            try { out.add(getEntity(id)); } catch (Exception ignore) {}
        }
        return out;
    }

    /**
     * Search entities by name or id (case-insensitive, contains)
     */
    public List<SearchResultItem> searchEntities(String query) {
        String q = query == null ? "" : query.trim().toLowerCase();
        if (q.isEmpty()) {
            return Collections.emptyList();
        }
        // Use new normalized schema
        return assetRepository.findAll().stream()
            .filter(a -> {
                String id = Optional.ofNullable(a.getShortName()).orElse("").toLowerCase();
                String name = Optional.ofNullable(a.getFullName()).orElse("").toLowerCase();
                return id.contains(q) || name.contains(q);
            })
            .limit(50)
            .map(a -> new SearchResultItem(a.getShortName(), a.getFullName(), "table", null))
            .collect(Collectors.toList());
    }

    // Helper methods removed - will be reimplemented using normalized schema when needed
}
