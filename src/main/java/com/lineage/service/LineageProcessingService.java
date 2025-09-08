package com.lineage.service;

import com.lineage.entity.*;
import com.lineage.repository.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class LineageProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(LineageProcessingService.class);
    private static final int CONTEXT_LINES = 2;

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private LineageEdgeRepository lineageEdgeRepository;

    @Autowired
    private AggregatedTableRepository aggregatedTableRepository;

    @Autowired
    private FineGrainedLineageRepository fineGrainedLineageRepository;

    @Autowired
    private TableRelationshipRepository tableRelationshipRepository;

    @Autowired
    private FileRepository fileRepository;

    /**
     * Main processing method that replicates the process_lineage.json functionality
     */
    @Transactional
    public void processLineageForRun(UUID runId) {
        logger.info("Starting lineage processing for run: {}", runId);

        try {
            // Step 1: Aggregate table definitions (Pass 1 from JS script)
            Map<String, AggregatedTable> aggregatedTables = aggregateTableDefinitions(runId);

            // Step 2: Process column lineages and create fine-grained lineages (Pass 2 from JS script)
            processFineGrainedLineages(runId, aggregatedTables);

            // Step 3: Build table-level relationships (Pass 3 from JS script)
            buildTableRelationships(runId, aggregatedTables);

            logger.info("Completed lineage processing for run: {}. Processed {} tables", 
                       runId, aggregatedTables.size());

        } catch (Exception e) {
            logger.error("Error processing lineage for run {}: {}", runId, e.getMessage(), e);
            throw new RuntimeException("Failed to process lineage for run: " + runId, e);
        }
    }

    /**
     * Step 1: Aggregate table definitions from all files in the run
     * Equivalent to Pass 1 in the JavaScript script
     */
    private Map<String, AggregatedTable> aggregateTableDefinitions(UUID runId) {
        logger.info("Aggregating table definitions for run: {}", runId);

        List<TableEntity> allTables = tableRepository.findByRunId(runId);
        Map<String, AggregatedTable> aggregatedTables = new HashMap<>();

        for (TableEntity table : allTables) {
            String originalTableName = table.getTableName();
            if (originalTableName == null || originalTableName.trim().isEmpty()) {
                logger.warn("Skipping table with null/empty name in file: {}", table.getFile().getFilePath());
                continue;
            }

            String lcTableName = originalTableName.toLowerCase();
            String shortTableId = extractShortName(lcTableName);
            String derivedSource = deriveSource(lcTableName);

            if (shortTableId == null) {
                logger.warn("Could not derive valid short table ID from: {}", originalTableName);
                continue;
            }

            AggregatedTable aggregatedTable = aggregatedTables.get(shortTableId);
            if (aggregatedTable == null) {
                // Create new aggregated table
                aggregatedTable = new AggregatedTable();
                aggregatedTable.setExtractionRun(table.getFile().getExtractionRun());
                aggregatedTable.setEntityId(shortTableId);
                aggregatedTable.setEntityName(shortTableId);
                aggregatedTable.setSource(derivedSource);
                aggregatedTable.setEntityType("table");

                // Process columns from the JSON structure
                List<AggregatedTable.FieldMetadata> fields = extractFieldsFromTableColumns(table.getColumns());
                aggregatedTable.setColumnsCount(fields.size());
                
                AggregatedTable.SchemaMetadata schemaMetadata = new AggregatedTable.SchemaMetadata();
                schemaMetadata.setFields(fields);
                aggregatedTable.setSchemaMetadata(schemaMetadata);

                // Extract partition keys (assuming they're in the JSON structure)
                List<String> partitionKeys = extractPartitionKeysFromTableColumns(table.getColumns());
                aggregatedTable.setPartitionKeys(partitionKeys);

                aggregatedTables.put(shortTableId, aggregatedTable);
            } else {
                // Merge with existing aggregated table
                mergeTableDefinitions(aggregatedTable, table, derivedSource);
            }
        }

        // Save all aggregated tables
        List<AggregatedTable> savedTables = aggregatedTableRepository.saveAll(aggregatedTables.values());
        logger.info("Saved {} aggregated tables", savedTables.size());

        return aggregatedTables;
    }

    /**
     * Step 2: Process fine-grained lineages from column edges
     * Equivalent to Pass 2 in the JavaScript script
     */
    private void processFineGrainedLineages(UUID runId, Map<String, AggregatedTable> aggregatedTables) {
        logger.info("Processing fine-grained lineages for run: {}", runId);

        List<LineageEdge> columnEdges = lineageEdgeRepository.findByEdgeTypeAndRunId(LineageEdge.EdgeType.COLUMN_EDGE, runId);
        List<FineGrainedLineage> fineGrainedLineages = new ArrayList<>();

        for (LineageEdge edge : columnEdges) {
            String fromTableFull = edge.getFromTable() != null ? edge.getFromTable().toLowerCase() : null;
            String toTableFull = edge.getToTable() != null ? edge.getToTable().toLowerCase() : null;

            String fromTableName = extractShortName(fromTableFull);
            String toTableName = extractShortName(toTableFull);

            if (fromTableName == null || toTableName == null) {
                logger.warn("Skipping edge with invalid table names: {} -> {}", fromTableFull, toTableFull);
                continue;
            }

            AggregatedTable toTable = aggregatedTables.get(toTableName);
            if (toTable == null) {
                logger.warn("Target table {} not found in aggregated tables", toTableName);
                continue;
            }

            // Process from/to columns (they might be arrays or single values)
            List<String> fromColumns = parseColumnValue(edge.getFromColumn());
            List<String> toColumns = parseColumnValue(edge.getToColumn());

            // Create fine-grained lineages for all column combinations
            for (String fromColumn : fromColumns) {
                for (String toColumn : toColumns) {
                    if (fromColumn != null && toColumn != null) {
                        FineGrainedLineage lineage = createFineGrainedLineage(
                            toTable, toTableName, toColumn, fromTableName, fromColumn, edge);
                        fineGrainedLineages.add(lineage);
                    }
                }
            }
        }

        // Save all fine-grained lineages
        List<FineGrainedLineage> savedLineages = fineGrainedLineageRepository.saveAll(fineGrainedLineages);
        logger.info("Saved {} fine-grained lineages", savedLineages.size());
    }

    /**
     * Step 3: Build table-level relationships
     * Equivalent to Pass 3 in the JavaScript script
     */
    private void buildTableRelationships(UUID runId, Map<String, AggregatedTable> aggregatedTables) {
        logger.info("Building table relationships for run: {}", runId);

        List<LineageEdge> tableEdges = lineageEdgeRepository.findByEdgeTypeAndRunId(LineageEdge.EdgeType.TABLE_EDGE, runId);
        Map<String, TableRelationship> relationships = new HashMap<>();

        // Initialize relationships for all tables
        for (String tableName : aggregatedTables.keySet()) {
            TableRelationship relationship = new TableRelationship();
            relationship.setExtractionRun(aggregatedTables.get(tableName).getExtractionRun());
            relationship.setTableName(tableName);
            relationship.setUpstreamTables(new ArrayList<>());
            relationship.setDownstreamTables(new ArrayList<>());
            relationships.put(tableName, relationship);
        }

        // Process table edges
        for (LineageEdge edge : tableEdges) {
            String fromTableFull = edge.getFromTable() != null ? edge.getFromTable().toLowerCase() : null;
            String toTableFull = edge.getToTable() != null ? edge.getToTable().toLowerCase() : null;

            String fromTable = extractShortName(fromTableFull);
            String toTable = extractShortName(toTableFull);

            if (fromTable == null || toTable == null) {
                logger.warn("Skipping table edge with invalid names: {} -> {}", fromTableFull, toTableFull);
                continue;
            }

            // Create transformation entry
            TableRelationship.TransformationEntry transformationEntry = createTransformationEntry(edge);

            // Add upstream relationship to toTable
            TableRelationship toTableRelationship = relationships.get(toTable);
            if (toTableRelationship != null) {
                addUpstreamRelationship(toTableRelationship, fromTable, transformationEntry);
            }

            // Add downstream relationship to fromTable
            TableRelationship fromTableRelationship = relationships.get(fromTable);
            if (fromTableRelationship != null) {
                addDownstreamRelationship(fromTableRelationship, toTable, transformationEntry);
            }
        }

        // Clean up empty relationships and save
        List<TableRelationship> nonEmptyRelationships = relationships.values().stream()
            .filter(r -> !r.getUpstreamTables().isEmpty() || !r.getDownstreamTables().isEmpty())
            .collect(Collectors.toList());

        List<TableRelationship> savedRelationships = tableRelationshipRepository.saveAll(nonEmptyRelationships);
        logger.info("Saved {} table relationships", savedRelationships.size());
    }

    // Utility methods

    private String extractShortName(String fullId) {
        if (fullId == null || fullId.trim().isEmpty()) {
            return null;
        }
        String[] parts = fullId.split("\\.");
        String shortName = parts[parts.length - 1];
        return shortName.trim().isEmpty() ? null : shortName;
    }

    private String deriveSource(String tableId) {
        if (tableId == null || tableId.trim().isEmpty()) {
            return null;
        }
        String[] parts = tableId.split("\\.");
        if (parts.length > 1) {
            return String.join(".", Arrays.copyOf(parts, parts.length - 1));
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private List<AggregatedTable.FieldMetadata> extractFieldsFromTableColumns(Map<String, Object> columns) {
        List<AggregatedTable.FieldMetadata> fields = new ArrayList<>();
        
        if (columns != null && columns.containsKey("columns")) {
            Object columnsObj = columns.get("columns");
            if (columnsObj instanceof List) {
                List<Object> columnsList = (List<Object>) columnsObj;
                for (Object col : columnsList) {
                    String columnName = col instanceof String ? ((String) col).toLowerCase() : String.valueOf(col).toLowerCase();
                    fields.add(new AggregatedTable.FieldMetadata(columnName, "varchar"));
                }
            }
        }
        
        return fields;
    }

    @SuppressWarnings("unchecked")
    private List<String> extractPartitionKeysFromTableColumns(Map<String, Object> columns) {
        List<String> partitionKeys = new ArrayList<>();
        
        if (columns != null && columns.containsKey("partition_keys")) {
            Object partitionKeysObj = columns.get("partition_keys");
            if (partitionKeysObj instanceof List) {
                List<Object> partitionKeysList = (List<Object>) partitionKeysObj;
                for (Object pk : partitionKeysList) {
                    if (pk instanceof Map) {
                        Map<String, Object> pkMap = (Map<String, Object>) pk;
                        Object columnName = pkMap.get("column_name");
                        if (columnName != null) {
                            partitionKeys.add(columnName.toString().toLowerCase());
                        }
                    }
                }
            }
        }
        
        return partitionKeys;
    }

    private void mergeTableDefinitions(AggregatedTable existing, TableEntity newTable, String derivedSource) {
        // Update columns if new definition has more
        List<AggregatedTable.FieldMetadata> newFields = extractFieldsFromTableColumns(newTable.getColumns());
        if (newFields.size() > existing.getColumnsCount()) {
            existing.setColumnsCount(newFields.size());
            existing.getSchemaMetadata().setFields(newFields);
        }

        // Merge partition keys
        List<String> newPartitionKeys = extractPartitionKeysFromTableColumns(newTable.getColumns());
        Set<String> mergedKeys = new HashSet<>(existing.getPartitionKeys() != null ? existing.getPartitionKeys() : Collections.emptyList());
        mergedKeys.addAll(newPartitionKeys);
        existing.setPartitionKeys(new ArrayList<>(mergedKeys));

        // Update source if current is null and new one is not
        if (existing.getSource() == null && derivedSource != null) {
            existing.setSource(derivedSource);
        }
    }

    private List<String> parseColumnValue(String columnValue) {
        if (columnValue == null || columnValue.trim().isEmpty()) {
            return Collections.emptyList();
        }
        
        // For simplicity, assuming single column values for now
        // In a real implementation, you might need to parse JSON arrays
        return Collections.singletonList(columnValue.trim().toLowerCase());
    }

    private FineGrainedLineage createFineGrainedLineage(AggregatedTable toTable, String toTableName, 
                                                       String toColumn, String fromTableName, 
                                                       String fromColumn, LineageEdge edge) {
        FineGrainedLineage lineage = new FineGrainedLineage();
        lineage.setAggregatedTable(toTable);
        lineage.setDownstreamTable(toTableName);
        lineage.setDownstreamColumn(toColumn);

        // Create upstream reference
        FineGrainedLineage.UpstreamReference upstreamRef = new FineGrainedLineage.UpstreamReference();
        upstreamRef.setUrn(fromTableName);
        upstreamRef.setPath(fromColumn);

        // Create transformation info
        FineGrainedLineage.TransformationInfo transformationInfo = new FineGrainedLineage.TransformationInfo();
        transformationInfo.setFileId(getRelativeFilePath(edge.getFile().getFilePath()));
        transformationInfo.setTransformationCode(edge.getTransformationCode());
        transformationInfo.setType(edge.getTransformationType());

        // Create line info from transformation lines
        FineGrainedLineage.LineInfo lineInfo = extractLineInfo(edge.getTransformationLines());
        transformationInfo.setLines(lineInfo);

        upstreamRef.setTransformation(transformationInfo);
        lineage.setUpstreamReferences(Collections.singletonList(upstreamRef));

        return lineage;
    }

    private TableRelationship.TransformationEntry createTransformationEntry(LineageEdge edge) {
        TableRelationship.TransformationEntry entry = new TableRelationship.TransformationEntry();
        entry.setFileId(getRelativeFilePath(edge.getFile().getFilePath()));
        entry.setTransformationType(edge.getTransformationType());

        // Extract line range from transformation lines
        TableRelationship.LineRange lineRange = extractLineRange(edge.getTransformationLines());
        entry.setLines(lineRange);

        return entry;
    }

    private void addUpstreamRelationship(TableRelationship relationship, String upstreamTable, 
                                        TableRelationship.TransformationEntry transformation) {
        TableRelationship.TableLineageInfo upstreamInfo = relationship.getUpstreamTables().stream()
            .filter(u -> u.getTable().equals(upstreamTable))
            .findFirst()
            .orElse(null);

        if (upstreamInfo == null) {
            upstreamInfo = new TableRelationship.TableLineageInfo();
            upstreamInfo.setTable(upstreamTable);
            upstreamInfo.setTransformations(new ArrayList<>());
            relationship.getUpstreamTables().add(upstreamInfo);
        }

        upstreamInfo.getTransformations().add(transformation);
    }

    private void addDownstreamRelationship(TableRelationship relationship, String downstreamTable, 
                                          TableRelationship.TransformationEntry transformation) {
        TableRelationship.TableLineageInfo downstreamInfo = relationship.getDownstreamTables().stream()
            .filter(d -> d.getTable().equals(downstreamTable))
            .findFirst()
            .orElse(null);

        if (downstreamInfo == null) {
            downstreamInfo = new TableRelationship.TableLineageInfo();
            downstreamInfo.setTable(downstreamTable);
            downstreamInfo.setTransformations(new ArrayList<>());
            relationship.getDownstreamTables().add(downstreamInfo);
        }

        downstreamInfo.getTransformations().add(transformation);
    }

    @SuppressWarnings("unchecked")
    private FineGrainedLineage.LineInfo extractLineInfo(Map<String, Object> transformationLines) {
        FineGrainedLineage.LineInfo lineInfo = new FineGrainedLineage.LineInfo();
        
        if (transformationLines != null) {
            Object startLineObj = transformationLines.get("start_line");
            Object endLineObj = transformationLines.get("end_line");
            
            lineInfo.setStartLine(parseLineValue(startLineObj));
            lineInfo.setEndLine(parseLineValue(endLineObj));
            
            Object linesBeforeObj = transformationLines.get("lines_before_start_line");
            Object linesAfterObj = transformationLines.get("lines_after_end_line");
            
            lineInfo.setLinesBeforeStartLine(linesBeforeObj instanceof Number ? ((Number) linesBeforeObj).intValue() : 0);
            lineInfo.setLinesAfterEndLine(linesAfterObj instanceof Number ? ((Number) linesAfterObj).intValue() : 0);
        }
        
        return lineInfo;
    }

    @SuppressWarnings("unchecked")
    private TableRelationship.LineRange extractLineRange(Map<String, Object> transformationLines) {
        TableRelationship.LineRange lineRange = new TableRelationship.LineRange();
        
        if (transformationLines != null) {
            Object startLineObj = transformationLines.get("start_line");
            Object endLineObj = transformationLines.get("end_line");
            
            lineRange.setStartLine(parseLineValue(startLineObj));
            lineRange.setEndLine(parseLineValue(endLineObj));
        }
        
        return lineRange;
    }

    private Integer parseLineValue(Object value) {
        if (value instanceof Number) {
            int intValue = ((Number) value).intValue();
            return intValue > 0 ? intValue : null;
        }
        if (value instanceof String) {
            String strValue = ((String) value).trim();
            if (strValue.toLowerCase().startsWith("l")) {
                strValue = strValue.substring(1);
            }
            try {
                int intValue = Integer.parseInt(strValue);
                return intValue > 0 ? intValue : null;
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    private String getRelativeFilePath(String filePath) {
        if (filePath == null) {
            return null;
        }
        
        // Convert to relative path similar to the JavaScript script
        Path path = Paths.get(filePath);
        Path fileName = path.getFileName();
        if (fileName != null) {
            String name = fileName.toString();
            // Convert YAML file to Jinja2 file name
            if (name.endsWith("_dependencies.yaml") || name.endsWith("_table_dependencies.yaml")) {
                name = name.replace("_dependencies.yaml", ".jinja2")
                          .replace("_table_dependencies.yaml", ".jinja2");
            }
            return "code/" + path.getParent().getFileName() + "/" + name;
        }
        
        return filePath;
    }
}
