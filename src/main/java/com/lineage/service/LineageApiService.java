package com.lineage.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lineage.dto.*;
import com.lineage.entity.ProcessedTable;
import com.lineage.entity.ProcessedTableLineage;
import com.lineage.entity.ProcessedColumnLineage;
import com.lineage.repository.ProcessedTableRepository;
import com.lineage.repository.ProcessedTableLineageRepository;
import com.lineage.repository.ProcessedColumnLineageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * LineageApiService adapts processed data from the database into the
 * frontend-facing DTO contract (LineageResponseDto, EntityDto, etc.).
 *
 * This service performs format conversion only, without mutating state.
 */
@Service
public class LineageApiService {

    private static final Logger logger = LoggerFactory.getLogger(LineageApiService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private ProcessedTableRepository processedTableRepository;

    @Autowired
    private ProcessedTableLineageRepository processedTableLineageRepository;

    @Autowired
    private ProcessedColumnLineageRepository processedColumnLineageRepository;

    /**
     * Get lineage (upstream/downstream) for a specific entity
     */
    public LineageResponseDto getLineage(String entityId, LineageDirection direction) {
        logger.info("Getting {} lineage for entity: {}", direction, entityId);

        // Find the main entity
        ProcessedTable mainEntity = processedTableRepository.findByEntityId(entityId)
            .orElseThrow(() -> new RuntimeException("Entity not found: " + entityId));

        // Get table lineage
        ProcessedTableLineage tableLineage = processedTableLineageRepository
            .findByExtractionRunAndTableName(mainEntity.getExtractionRun(), entityId)
            .orElse(null);

        List<LineageNodeDto> lineageNodes = new ArrayList<>();
        
        if (tableLineage != null) {
            List<ProcessedTableLineage.TableLineageInfo> relatedTables = direction == LineageDirection.upstream 
                ? tableLineage.getUpstreamTables() 
                : tableLineage.getDownstreamTables();

            if (relatedTables != null) {
                lineageNodes = relatedTables.stream()
                    .map(this::convertToLineageNode)
                    .collect(Collectors.toList());
            }
        }

        // Get fine-grained lineages for this entity
        List<FineGrainedLineageDto> fineGrainedLineages = getFineGrainedLineagesForEntity(
            mainEntity.getExtractionRun().getRunId(), entityId);

        // Get schema metadata
        SchemaMetadataDto schemaMetadata = convertSchemaMetadata(mainEntity);

        return new LineageResponseDto(
            mainEntity.getEntityId(),
            mainEntity.getEntityName(),
            mainEntity.getEntityType(),
            direction,
            lineageNodes
        ) {{
            setFineGrainedLineages(fineGrainedLineages);
            setSchemaMetadata(schemaMetadata);
        }};
    }

    /**
     * Get entity details
     */
    public EntityDto getEntity(String entityId) {
        logger.info("Getting entity details for: {}", entityId);

        ProcessedTable entity = processedTableRepository.findByEntityId(entityId)
            .orElseThrow(() -> new RuntimeException("Entity not found: " + entityId));

        return convertToEntityDto(entity);
    }

    /**
     * Get multiple entity details
     */
    public List<EntityDto> getBulkEntities(List<String> entityIds) {
        logger.info("Getting bulk entity details for {} entities", entityIds.size());

        return entityIds.stream()
            .map(this::getEntity)
            .collect(Collectors.toList());
    }

    /**
     * Search entities by name or id (case-insensitive, contains)
     */
    public List<SearchResultItem> searchEntities(String query) {
        String q = query == null ? "" : query.trim().toLowerCase();
        if (q.isEmpty()) {
            return Collections.emptyList();
        }

        List<ProcessedTable> all = processedTableRepository.findAll();

        return all.stream()
            .filter(t -> {
                String id = Optional.ofNullable(t.getEntityId()).orElse("").toLowerCase();
                String name = Optional.ofNullable(t.getEntityName()).orElse("").toLowerCase();
                return id.contains(q) || name.contains(q);
            })
            .limit(50)
            .map(t -> new SearchResultItem(t.getEntityId(), t.getEntityName(), t.getEntityType(), t.getColumnsCount()))
            .collect(Collectors.toList());
    }

    /**
     * Convert ProcessedTable to EntityDto
     */
    private EntityDto convertToEntityDto(ProcessedTable entity) {
        EntityDto dto = new EntityDto();
        dto.setEntityId(entity.getEntityId());
        dto.setEntityName(entity.getEntityName());
        dto.setEntityType(entity.getEntityType());
        dto.setColumnCount(entity.getColumnsCount());
        dto.setSource(entity.getSource());
        dto.setToolKey(entity.getToolKey());

        // Calculate upstream/downstream counts
        ProcessedTableLineage tableLineage = processedTableLineageRepository
            .findByExtractionRunAndTableName(entity.getExtractionRun(), entity.getEntityId())
            .orElse(null);

        int upstreamCount = 0;
        int downstreamCount = 0;
        
        if (tableLineage != null) {
            upstreamCount = tableLineage.getUpstreamTables() != null ? tableLineage.getUpstreamTables().size() : 0;
            downstreamCount = tableLineage.getDownstreamTables() != null ? tableLineage.getDownstreamTables().size() : 0;
        }

        dto.setUpstreamCount(upstreamCount);
        dto.setDownstreamCount(downstreamCount);
        dto.setHasUpstream(upstreamCount > 0);
        dto.setHasDownstream(downstreamCount > 0);

        // Get fine-grained lineages
        List<FineGrainedLineageDto> fineGrainedLineages = getFineGrainedLineagesForEntity(
            entity.getExtractionRun().getRunId(), entity.getEntityId());
        dto.setFineGrainedLineages(fineGrainedLineages);

        // Convert schema metadata
        dto.setSchemaMetadata(convertSchemaMetadata(entity));

        return dto;
    }

    /**
     * Convert TableLineageInfo to LineageNodeDto
     */
    private LineageNodeDto convertToLineageNode(ProcessedTableLineage.TableLineageInfo tableInfo) {
        String tableId = tableInfo.getTable();
        String tableName = tableId; // Use table ID as name for now
        
        LineageNodeDto node = new LineageNodeDto(tableId, tableName, "table");
        
        // Convert transformations if present
        List<ProcessedTableLineage.TransformationEntry> transformationEntries = tableInfo.getTransformations();
        
        if (transformationEntries != null) {
            List<TransformationDto> transformations = transformationEntries.stream()
                .map(this::convertToTransformationDto)
                .collect(Collectors.toList());
            node.setTransformations(transformations);
        }
        
        return node;
    }

    /**
     * Convert TransformationEntry to TransformationDto
     */
    private TransformationDto convertToTransformationDto(ProcessedTableLineage.TransformationEntry transformationEntry) {
        String fileId = transformationEntry.getFileId();
        String transformationType = transformationEntry.getTransformationType();
        
        ProcessedTableLineage.LineRange lineRange = transformationEntry.getLines();
        
        TransformationLinesDto lines = null;
        if (lineRange != null) {
            lines = new TransformationLinesDto(lineRange.getStartLine(), lineRange.getEndLine());
        }
        
        return new TransformationDto(fileId, transformationType, lines);
    }

    /**
     * Get fine-grained lineages for an entity
     */
    private List<FineGrainedLineageDto> getFineGrainedLineagesForEntity(UUID runId, String entityId) {
        List<ProcessedColumnLineage> columnLineages = processedColumnLineageRepository
            .findByRunIdAndDownstreamTable(runId, entityId);

        // Group by downstream column to create FineGrainedLineageDto objects
        Map<String, List<ProcessedColumnLineage>> groupedByDownstream = columnLineages.stream()
            .collect(Collectors.groupingBy(ProcessedColumnLineage::getDownstreamColumn));

        return groupedByDownstream.entrySet().stream()
            .map(entry -> {
                String downstreamColumn = entry.getKey();
                List<ProcessedColumnLineage> lineagesForColumn = entry.getValue();

                List<FieldRefDto> upstreams = lineagesForColumn.stream()
                    .map(cl -> {
                        FieldRefDto upstream = new FieldRefDto(cl.getUpstreamTable(), cl.getUpstreamColumn());
                        
                        // Add transformation if available
                        if (cl.getTransformationType() != null && cl.getTransformationLines() != null) {
                            ProcessedColumnLineage.TransformationLines transformationLines = cl.getTransformationLines();
                            Integer startLine = transformationLines.getStartLine();
                            Integer endLine = transformationLines.getEndLine();
                            
                            if (startLine != null && endLine != null) {
                                TransformationLinesDto lines = new TransformationLinesDto(startLine, endLine);
                                TransformationDto transformation = new TransformationDto(
                                    cl.getFileId(), 
                                    cl.getTransformationType(), 
                                    lines
                                );
                                upstream.setTransformation(transformation);
                            }
                        }
                        
                        return upstream;
                    })
                    .collect(Collectors.toList());

                List<FieldRefDto> downstreams = Collections.singletonList(
                    new FieldRefDto(entityId, downstreamColumn)
                );

                return new FineGrainedLineageDto(upstreams, downstreams);
            })
            .collect(Collectors.toList());
    }

    /**
     * Convert schema metadata from ProcessedTable to DTO
     */
    private SchemaMetadataDto convertSchemaMetadata(ProcessedTable entity) {
        ProcessedTable.SchemaMetadata schemaMetadata = entity.getSchemaMetadata();
        if (schemaMetadata == null) {
            return new SchemaMetadataDto();
        }

        try {
            List<ProcessedTable.FieldMetadata> fields = schemaMetadata.getFields();
            
            if (fields == null) {
                return new SchemaMetadataDto();
            }

            List<SchemaFieldDto> fieldDtos = fields.stream()
                .map(field -> {
                    SchemaFieldDto fieldDto = new SchemaFieldDto();
                    fieldDto.setPath(field.getPath());
                    fieldDto.setNativeDataType(field.getNativeDataType());
                    fieldDto.setLabel(field.getLabel());
                    fieldDto.setDescription(field.getDescription());
                    fieldDto.setNullable(field.getNullable());
                    return fieldDto;
                })
                .collect(Collectors.toList());

            return new SchemaMetadataDto(fieldDtos);
        } catch (Exception e) {
            logger.warn("Failed to convert schema metadata: {}", e.getMessage());
            return new SchemaMetadataDto();
        }
    }
}
