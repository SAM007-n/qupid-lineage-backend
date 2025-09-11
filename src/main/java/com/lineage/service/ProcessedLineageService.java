package com.lineage.service;

import com.lineage.entity.*;
import com.lineage.repository.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class ProcessedLineageService {

    private static final Logger logger = LoggerFactory.getLogger(ProcessedLineageService.class);

    @Autowired
    private TableRepository tableRepository;

    @Autowired
    private LineageEdgeRepository lineageEdgeRepository;

    @Autowired
    private ProcessedTableRepository processedTableRepository;

    @Autowired
    private ProcessedColumnLineageRepository processedColumnLineageRepository;

    @Autowired
    private ProcessedTableLineageRepository processedTableLineageRepository;

    @Autowired
    private ExtractionRunRepository extractionRunRepository;

    /**
     * Main processing method that creates processed/aggregated lineage data from raw database records
     * Implements the 3-pass logic: table aggregation, column lineages, table relationships
     * This replicates the process_lineage.json functionality but works purely with database data
     */
    @Transactional
    public void processLineageForRun(UUID runId) {
        logger.info("Starting processed lineage generation for run: {}", runId);

        try {
            // Get the extraction run
            ExtractionRun extractionRun = extractionRunRepository.findById(runId)
                .orElseThrow(() -> new RuntimeException("Extraction run not found: " + runId));

            // Clear existing processed data for this run (for reprocessing)
            clearProcessedDataForRun(runId);

            // Pass 1: Aggregate table definitions (equivalent to JS Pass 1)
            Map<String, ProcessedTable> processedTables = aggregateTableDefinitions(extractionRun);

            // Pass 2: Process column lineages (equivalent to JS Pass 2)
            processColumnLineages(extractionRun, processedTables);

            // Pass 3: Build table-level relationships (equivalent to JS Pass 3)
            buildTableRelationships(extractionRun, processedTables);

            logger.info("Completed processed lineage generation for run: {}. Processed {} tables", 
                       runId, processedTables.size());

        } catch (Exception e) {
            logger.error("Error processing lineage for run {}: {}", runId, e.getMessage(), e);
            throw new RuntimeException("Failed to process lineage for run: " + runId, e);
        }
    }

    /**
     * Clear existing processed data for a run (for reprocessing)
     */
    private void clearProcessedDataForRun(UUID runId) {
        logger.info("Clearing existing processed data for run: {}", runId);
        
        processedColumnLineageRepository.deleteByExtractionRunRunId(runId);
        processedTableLineageRepository.deleteByExtractionRunRunId(runId);
        processedTableRepository.deleteByExtractionRunRunId(runId);
        
        logger.info("Cleared existing processed data for run: {}", runId);
    }

    /**
     * Pass 1: Aggregate table definitions from all files in the run
     * Equivalent to Pass 1 in the JavaScript script
     */
    private Map<String, ProcessedTable> aggregateTableDefinitions(ExtractionRun extractionRun) {
        logger.info("Pass 1: Aggregating table definitions for run: {}", extractionRun.getRunId());

        List<TableEntity> allTables = tableRepository.findByRunId(extractionRun.getRunId());
        Map<String, ProcessedTable> processedTablesMap = new HashMap<>();

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

            ProcessedTable processedTable = processedTablesMap.get(shortTableId);
            if (processedTable == null) {
                // Create new processed table
                processedTable = new ProcessedTable(extractionRun, shortTableId, shortTableId);
                processedTable.setSource(derivedSource);
                processedTable.setEntityType("table");

                // Process columns from the JSON structure
                List<ProcessedTable.FieldMetadata> fields = extractFieldsFromTableColumns(table.getColumns());
                processedTable.setColumnsCount(fields.size());
                
                ProcessedTable.SchemaMetadata schemaMetadata = new ProcessedTable.SchemaMetadata();
                schemaMetadata.setFields(fields);
                processedTable.setSchemaMetadata(schemaMetadata);

                // Extract partition keys
                List<String> partitionKeys = extractPartitionKeysFromTableColumns(table.getColumns());
                processedTable.setPartitionKeys(partitionKeys);

                processedTablesMap.put(shortTableId, processedTable);
            } else {
                // Merge with existing processed table
                mergeTableDefinitions(processedTable, table, derivedSource);
            }
        }

        // Save all processed tables
        List<ProcessedTable> savedTables = processedTableRepository.saveAll(processedTablesMap.values());
        logger.info("Pass 1: Saved {} processed tables", savedTables.size());

        // Update the map with saved entities (to get IDs)
        Map<String, ProcessedTable> result = new HashMap<>();
        for (ProcessedTable saved : savedTables) {
            result.put(saved.getEntityId(), saved);
        }

        return result;
    }

    /**
     * Pass 2: Process column lineages from column edges
     * Equivalent to Pass 2 in the JavaScript script
     */
    private void processColumnLineages(ExtractionRun extractionRun, Map<String, ProcessedTable> processedTables) {
        logger.info("Pass 2: Processing column lineages for run: {}", extractionRun.getRunId());

        List<LineageEdge> columnEdges = lineageEdgeRepository.findByEdgeTypeAndRunId(
            LineageEdge.EdgeType.COLUMN_EDGE, extractionRun.getRunId());
        List<ProcessedColumnLineage> columnLineages = new ArrayList<>();

        for (LineageEdge edge : columnEdges) {
            String fromTableFull = edge.getFromTable() != null ? edge.getFromTable().toLowerCase() : null;
            String toTableFull = edge.getToTable() != null ? edge.getToTable().toLowerCase() : null;

            String fromTableName = extractShortName(fromTableFull);
            String toTableName = extractShortName(toTableFull);

            if (fromTableName == null || toTableName == null) {
                logger.warn("Skipping edge with invalid table names: {} -> {}", fromTableFull, toTableFull);
                continue;
            }

            ProcessedTable toTable = processedTables.get(toTableName);
            if (toTable == null) {
                logger.warn("Target table {} not found in processed tables", toTableName);
                continue;
            }

            // Process from/to columns
            List<String> fromColumns = parseColumnValue(edge.getFromColumn());
            List<String> toColumns = parseColumnValue(edge.getToColumn());

            // Create column lineages for all column combinations
            for (String fromColumn : fromColumns) {
                for (String toColumn : toColumns) {
                    if (fromColumn != null && toColumn != null) {
                        ProcessedColumnLineage columnLineage = createColumnLineage(
                            extractionRun, toTable, toTableName, toColumn, 
                            fromTableName, fromColumn, edge);
                        columnLineages.add(columnLineage);
                    }
                }
            }
        }

        // Save all column lineages
        List<ProcessedColumnLineage> savedLineages = processedColumnLineageRepository.saveAll(columnLineages);
        logger.info("Pass 2: Saved {} column lineages", savedLineages.size());
    }

    /**
     * Pass 3: Build table-level relationships
     * Equivalent to Pass 3 in the JavaScript script
     */
    private void buildTableRelationships(ExtractionRun extractionRun, Map<String, ProcessedTable> processedTables) {
        logger.info("Pass 3: Building table relationships for run: {}", extractionRun.getRunId());

        List<LineageEdge> tableEdges = lineageEdgeRepository.findByEdgeTypeAndRunId(
            LineageEdge.EdgeType.TABLE_EDGE, extractionRun.getRunId());
        Map<String, ProcessedTableLineage> relationships = new HashMap<>();

        // Initialize relationships for all tables
        for (String tableName : processedTables.keySet()) {
            ProcessedTableLineage relationship = new ProcessedTableLineage(extractionRun, tableName);
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
            ProcessedTableLineage.TransformationEntry transformationEntry = createTransformationEntry(edge);

            // Add upstream relationship to toTable
            ProcessedTableLineage toTableRelationship = relationships.get(toTable);
            if (toTableRelationship != null) {
                addUpstreamRelationship(toTableRelationship, fromTable, transformationEntry);
            }

            // Add downstream relationship to fromTable
            ProcessedTableLineage fromTableRelationship = relationships.get(fromTable);
            if (fromTableRelationship != null) {
                addDownstreamRelationship(fromTableRelationship, toTable, transformationEntry);
            }
        }

        // Clean up empty relationships and save
        List<ProcessedTableLineage> nonEmptyRelationships = relationships.values().stream()
            .filter(r -> !r.getUpstreamTables().isEmpty() || !r.getDownstreamTables().isEmpty())
            .collect(Collectors.toList());

        List<ProcessedTableLineage> savedRelationships = processedTableLineageRepository.saveAll(nonEmptyRelationships);
        logger.info("Pass 3: Saved {} table relationships", savedRelationships.size());
    }

    // Utility methods (equivalent to JavaScript utility functions)

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
    private List<ProcessedTable.FieldMetadata> extractFieldsFromTableColumns(Map<String, Object> columns) {
        List<ProcessedTable.FieldMetadata> fields = new ArrayList<>();
        
        if (columns != null && columns.containsKey("columns")) {
            Object columnsObj = columns.get("columns");
            if (columnsObj instanceof List) {
                List<Object> columnsList = (List<Object>) columnsObj;
                for (Object col : columnsList) {
                    String columnName = col instanceof String ? ((String) col).toLowerCase() : String.valueOf(col).toLowerCase();
                    fields.add(new ProcessedTable.FieldMetadata(columnName, "varchar"));
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

    private void mergeTableDefinitions(ProcessedTable existing, TableEntity newTable, String derivedSource) {
        // Update columns if new definition has more
        List<ProcessedTable.FieldMetadata> newFields = extractFieldsFromTableColumns(newTable.getColumns());
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

    private ProcessedColumnLineage createColumnLineage(ExtractionRun extractionRun, ProcessedTable toTable, 
                                                      String toTableName, String toColumn, 
                                                      String fromTableName, String fromColumn, 
                                                      LineageEdge edge) {
        ProcessedColumnLineage lineage = new ProcessedColumnLineage(
            extractionRun, toTable, toTableName, toColumn, fromTableName, fromColumn);
        
        lineage.setTransformationType(edge.getTransformationType());
        lineage.setFileId(getRelativeFilePath(edge.getFile().getFilePath()));

        // Extract transformation lines from the raw edge data
        ProcessedColumnLineage.TransformationLines transformationLines = extractTransformationLinesFromEdge(edge.getTransformationLines());
        lineage.setTransformationLines(transformationLines);

        // Use the transformation code already stored in the edge (if any)
        lineage.setTransformationCode(edge.getTransformationCode());

        return lineage;
    }

    private ProcessedTableLineage.TransformationEntry createTransformationEntry(LineageEdge edge) {
        ProcessedTableLineage.TransformationEntry entry = new ProcessedTableLineage.TransformationEntry();
        entry.setFileId(getRelativeFilePath(edge.getFile().getFilePath()));
        entry.setTransformationType(edge.getTransformationType());

        // Extract line range from transformation lines
        ProcessedTableLineage.LineRange lineRange = extractLineRange(edge.getTransformationLines());
        entry.setLines(lineRange);

        return entry;
    }

    private void addUpstreamRelationship(ProcessedTableLineage relationship, String upstreamTable, 
                                        ProcessedTableLineage.TransformationEntry transformation) {
        ProcessedTableLineage.TableLineageInfo upstreamInfo = relationship.getUpstreamTables().stream()
            .filter(u -> u.getTable().equals(upstreamTable))
            .findFirst()
            .orElse(null);

        if (upstreamInfo == null) {
            upstreamInfo = new ProcessedTableLineage.TableLineageInfo();
            upstreamInfo.setTable(upstreamTable);
            upstreamInfo.setTransformations(new ArrayList<>());
            relationship.getUpstreamTables().add(upstreamInfo);
        }

        upstreamInfo.getTransformations().add(transformation);
    }

    private void addDownstreamRelationship(ProcessedTableLineage relationship, String downstreamTable, 
                                          ProcessedTableLineage.TransformationEntry transformation) {
        ProcessedTableLineage.TableLineageInfo downstreamInfo = relationship.getDownstreamTables().stream()
            .filter(d -> d.getTable().equals(downstreamTable))
            .findFirst()
            .orElse(null);

        if (downstreamInfo == null) {
            downstreamInfo = new ProcessedTableLineage.TableLineageInfo();
            downstreamInfo.setTable(downstreamTable);
            downstreamInfo.setTransformations(new ArrayList<>());
            relationship.getDownstreamTables().add(downstreamInfo);
        }

        downstreamInfo.getTransformations().add(transformation);
    }

    @SuppressWarnings("unchecked")
    private ProcessedColumnLineage.TransformationLines extractTransformationLinesFromEdge(Map<String, Object> transformationLines) {
        ProcessedColumnLineage.TransformationLines lines = new ProcessedColumnLineage.TransformationLines();
        
        if (transformationLines != null) {
            Object startLineObj = transformationLines.get("start_line");
            Object endLineObj = transformationLines.get("end_line");
            Object linesBeforeObj = transformationLines.get("lines_before_start_line");
            Object linesAfterObj = transformationLines.get("lines_after_end_line");
            
            lines.setStartLine(parseLineValue(startLineObj));
            lines.setEndLine(parseLineValue(endLineObj));
            lines.setLinesBeforeStartLine(linesBeforeObj instanceof Number ? ((Number) linesBeforeObj).intValue() : 0);
            lines.setLinesAfterEndLine(linesAfterObj instanceof Number ? ((Number) linesAfterObj).intValue() : 0);
        }
        
        return lines;
    }

    @SuppressWarnings("unchecked")
    private ProcessedTableLineage.LineRange extractLineRange(Map<String, Object> transformationLines) {
        ProcessedTableLineage.LineRange lineRange = new ProcessedTableLineage.LineRange();
        
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
        
        // Simply return the file path as stored in the database
        // No need to convert YAML to Jinja2 paths since we're not processing files
        return filePath;
    }
}
