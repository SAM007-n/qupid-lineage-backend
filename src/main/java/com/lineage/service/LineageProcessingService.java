package com.lineage.service;

import com.lineage.entity.*;
import com.lineage.repository.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

/**
 * LineageProcessingService
 * Unified service that supports both batch (per-run) processing and
 * real-time incremental updates, consolidating previous ProcessedLineageService
 * and RealTimeProcessedLineageService responsibilities.
 */
@Service
public class LineageProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(LineageProcessingService.class);

    @Autowired private TableRepository tableRepository;
    @Autowired private LineageEdgeRepository lineageEdgeRepository;
    @Autowired private ProcessedTableRepository processedTableRepository;
    @Autowired private ProcessedColumnLineageRepository processedColumnLineageRepository;
    @Autowired private ProcessedTableLineageRepository processedTableLineageRepository;
    @Autowired private ExtractionRunRepository extractionRunRepository;

    // =========================
    // Batch processing (by run)
    // =========================
    @Transactional
    public void processLineageForRun(UUID runId) {
        logger.info("Starting processed lineage generation for run: {}", runId);
        ExtractionRun extractionRun = extractionRunRepository.findById(runId)
            .orElseThrow(() -> new RuntimeException("Extraction run not found: " + runId));

        clearProcessedDataForRun(runId);

        Map<String, ProcessedTable> processedTables = aggregateTableDefinitions(extractionRun);
        processColumnLineagesBatch(extractionRun, processedTables);
        buildTableRelationshipsBatch(extractionRun, processedTables);
        logger.info("Completed processed lineage generation for run: {}. Processed {} tables", runId, processedTables.size());
    }

    private void clearProcessedDataForRun(UUID runId) {
        processedColumnLineageRepository.deleteByExtractionRunRunId(runId);
        processedTableLineageRepository.deleteByExtractionRunRunId(runId);
        processedTableRepository.deleteByExtractionRunRunId(runId);
    }

    private Map<String, ProcessedTable> aggregateTableDefinitions(ExtractionRun extractionRun) {
        List<TableEntity> allTables = tableRepository.findByRunId(extractionRun.getRunId());
        Map<String, ProcessedTable> map = new HashMap<>();
        for (TableEntity table : allTables) {
            String originalTableName = table.getTableName();
            if (originalTableName == null || originalTableName.isBlank()) {
                continue;
            }
            String lc = originalTableName.toLowerCase();
            String shortId = extractShortName(lc);
            String source = deriveSource(lc);
            if (shortId == null) continue;

            ProcessedTable pt = map.get(shortId);
            if (pt == null) {
                pt = new ProcessedTable(extractionRun, shortId, shortId);
                pt.setSource(source);
                pt.setEntityType("table");
                List<ProcessedTable.FieldMetadata> fields = extractFieldsFromTableColumns(table.getColumns());
                pt.setColumnsCount(fields.size());
                ProcessedTable.SchemaMetadata schema = new ProcessedTable.SchemaMetadata();
                schema.setFields(fields);
                pt.setSchemaMetadata(schema);
                pt.setPartitionKeys(extractPartitionKeysFromTableColumns(table.getColumns()));
                map.put(shortId, pt);
            } else {
                mergeTableDefinitions(pt, table, source);
            }
        }
        processedTableRepository.saveAll(map.values());
        return map;
    }

    private void processColumnLineagesBatch(ExtractionRun extractionRun, Map<String, ProcessedTable> processedTables) {
        List<LineageEdge> columnEdges = lineageEdgeRepository.findByEdgeTypeAndRunId(LineageEdge.EdgeType.COLUMN_EDGE, extractionRun.getRunId());
        List<ProcessedColumnLineage> columnLineages = new ArrayList<>();
        for (LineageEdge edge : columnEdges) {
            String fromTable = extractShortName(edge.getFromTable() != null ? edge.getFromTable().toLowerCase() : null);
            String toTable = extractShortName(edge.getToTable() != null ? edge.getToTable().toLowerCase() : null);
            if (fromTable == null || toTable == null) continue;
            ProcessedTable target = processedTables.get(toTable);
            if (target == null) continue;
            List<String> fromCols = parseColumnValue(edge.getFromColumn());
            List<String> toCols = parseColumnValue(edge.getToColumn());
            for (String fc : fromCols) for (String tc : toCols) if (fc != null && tc != null) {
                ProcessedColumnLineage pcl = new ProcessedColumnLineage(extractionRun, target, toTable, tc, fromTable, fc);
                pcl.setTransformationType(edge.getTransformationType());
                pcl.setFileId(getRelativeFilePath(edge.getFile().getFilePath()));
                ProcessedColumnLineage.TransformationLines lines = extractTransformationLinesFromEdge(edge.getTransformationLines());
                pcl.setTransformationLines(lines);
                pcl.setTransformationCode(edge.getTransformationCode());
                columnLineages.add(pcl);
            }
        }
        processedColumnLineageRepository.saveAll(columnLineages);
    }

    private void buildTableRelationshipsBatch(ExtractionRun extractionRun, Map<String, ProcessedTable> processedTables) {
        List<LineageEdge> tableEdges = lineageEdgeRepository.findByEdgeTypeAndRunId(LineageEdge.EdgeType.TABLE_EDGE, extractionRun.getRunId());
        Map<String, ProcessedTableLineage> rel = new HashMap<>();
        for (String t : processedTables.keySet()) {
            ProcessedTableLineage r = new ProcessedTableLineage(extractionRun, t);
            r.setUpstreamTables(new ArrayList<>());
            r.setDownstreamTables(new ArrayList<>());
            rel.put(t, r);
        }
        for (LineageEdge edge : tableEdges) {
            String from = extractShortName(edge.getFromTable() != null ? edge.getFromTable().toLowerCase() : null);
            String to = extractShortName(edge.getToTable() != null ? edge.getToTable().toLowerCase() : null);
            if (from == null || to == null) continue;
            ProcessedTableLineage.TransformationEntry te = createTransformationEntry(edge);
            ProcessedTableLineage toRel = rel.computeIfAbsent(to, k -> {
                ProcessedTableLineage r = new ProcessedTableLineage(extractionRun, k);
                r.setUpstreamTables(new ArrayList<>());
                r.setDownstreamTables(new ArrayList<>());
                return r;
            });
            // Deduplicate by both table and transformation lines
            ProcessedTableLineage.TableLineageInfo up = toRel.getUpstreamTables().stream()
                .filter(u -> u.getTable().equals(from))
                .findFirst().orElseGet(() -> {
                    ProcessedTableLineage.TableLineageInfo ni = new ProcessedTableLineage.TableLineageInfo();
                    ni.setTable(from);
                    ni.setTransformations(new ArrayList<>());
                    toRel.getUpstreamTables().add(ni);
                    return ni;
                });
            if (up.getTransformations().stream().noneMatch(x -> Objects.equals(x.getFileId(), te.getFileId()) &&
                    ((x.getLines() == null && te.getLines() == null) || (x.getLines() != null && te.getLines() != null &&
                        Objects.equals(x.getLines().getStartLine(), te.getLines().getStartLine()) &&
                        Objects.equals(x.getLines().getEndLine(), te.getLines().getEndLine()))))) {
                up.getTransformations().add(te);
            }

            ProcessedTableLineage fromRel = rel.computeIfAbsent(from, k -> {
                ProcessedTableLineage r = new ProcessedTableLineage(extractionRun, k);
                r.setUpstreamTables(new ArrayList<>());
                r.setDownstreamTables(new ArrayList<>());
                return r;
            });
            ProcessedTableLineage.TableLineageInfo dn = fromRel.getDownstreamTables().stream()
                .filter(d -> d.getTable().equals(to))
                .findFirst().orElseGet(() -> {
                    ProcessedTableLineage.TableLineageInfo ni = new ProcessedTableLineage.TableLineageInfo();
                    ni.setTable(to);
                    ni.setTransformations(new ArrayList<>());
                    fromRel.getDownstreamTables().add(ni);
                    return ni;
                });
            if (dn.getTransformations().stream().noneMatch(x -> Objects.equals(x.getFileId(), te.getFileId()) &&
                    ((x.getLines() == null && te.getLines() == null) || (x.getLines() != null && te.getLines() != null &&
                        Objects.equals(x.getLines().getStartLine(), te.getLines().getStartLine()) &&
                        Objects.equals(x.getLines().getEndLine(), te.getLines().getEndLine()))))) {
                dn.getTransformations().add(te);
            }
        }
        List<ProcessedTableLineage> nonEmpty = rel.values().stream()
            .filter(r -> !r.getUpstreamTables().isEmpty() || !r.getDownstreamTables().isEmpty())
            .collect(Collectors.toList());
        processedTableLineageRepository.saveAll(nonEmpty);
    }

    // =========================
    // Real-time processing APIs
    // =========================
    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processNewTable(TableEntity tableEntity) { processTableEntity(tableEntity); }

    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processUpdatedTable(TableEntity tableEntity) { processTableEntity(tableEntity); }

    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processNewLineageEdge(LineageEdge lineageEdge) { processLineageEdge(lineageEdge); }

    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processUpdatedLineageEdge(LineageEdge lineageEdge) { processLineageEdge(lineageEdge); }

    @Transactional
    public void processTableEntity(TableEntity tableEntity) {
        String full = tableEntity.getTableName().toLowerCase();
        String shortId = extractShortName(full);
        String source = deriveSource(full);
        if (shortId == null) return;
        ExtractionRun run = tableEntity.getFile().getExtractionRun();
        Optional<ProcessedTable> existing = processedTableRepository.findByExtractionRunRunIdAndEntityId(run.getRunId(), shortId);
        ProcessedTable pt = existing.orElseGet(() -> {
            ProcessedTable n = new ProcessedTable();
            n.setExtractionRun(run);
            n.setEntityId(shortId);
            n.setEntityName(shortId);
            n.setSource(source);
            n.setEntityType("table");
            return n;
        });
        if (pt.getSchemaMetadata() == null) pt.setSchemaMetadata(new ProcessedTable.SchemaMetadata());
        List<ProcessedTable.FieldMetadata> fields = extractFieldsFromTableColumns(tableEntity.getColumns());
        if (fields.size() > (pt.getSchemaMetadata().getFields() == null ? 0 : pt.getSchemaMetadata().getFields().size())) {
            pt.setColumnsCount(fields.size());
            pt.getSchemaMetadata().setFields(fields);
        }
        List<String> pk = extractPartitionKeysFromTableColumns(tableEntity.getColumns());
        if (pk != null && !pk.isEmpty()) {
            Set<String> merged = new LinkedHashSet<>(pt.getPartitionKeys() == null ? List.of() : pt.getPartitionKeys());
            merged.addAll(pk);
            pt.setPartitionKeys(new ArrayList<>(merged));
        }
        if (pt.getSource() == null && source != null) pt.setSource(source);
        processedTableRepository.save(pt);
    }

    @Transactional
    public void processLineageEdge(LineageEdge lineageEdge) {
        ExtractionRun run = lineageEdge.getFile().getExtractionRun();
        if (lineageEdge.getEdgeType() == LineageEdge.EdgeType.COLUMN_EDGE) {
            processColumnLineage(lineageEdge, run);
        } else {
            processTableLineage(lineageEdge, run);
        }
    }

    private void processColumnLineage(LineageEdge edge, ExtractionRun run) {
        String fromTable = extractShortName(edge.getFromTable() != null ? edge.getFromTable().toLowerCase() : null);
        String toTable = extractShortName(edge.getToTable() != null ? edge.getToTable().toLowerCase() : null);
        if (fromTable == null || toTable == null) return;
        Optional<ProcessedTable> targetOpt = processedTableRepository.findByExtractionRunRunIdAndEntityId(run.getRunId(), toTable);
        if (targetOpt.isEmpty()) return;
        ProcessedTable target = targetOpt.get();
        List<String> fromCols = parseColumnValue(edge.getFromColumn());
        List<String> toCols = parseColumnValue(edge.getToColumn());
        for (String fc : fromCols) for (String tc : toCols) if (fc != null && tc != null) {
            List<ProcessedColumnLineage> existing = processedColumnLineageRepository
                .findByRunIdAndColumns(run.getRunId(), toTable, tc, fromTable, fc);
            if (existing.isEmpty()) {
                ProcessedColumnLineage pcl = new ProcessedColumnLineage();
                pcl.setExtractionRun(run);
                pcl.setProcessedTable(target);
                pcl.setDownstreamTable(toTable);
                pcl.setDownstreamColumn(tc);
                pcl.setUpstreamTable(fromTable);
                pcl.setUpstreamColumn(fc);
                pcl.setTransformationType(edge.getTransformationType());
                pcl.setTransformationCode(edge.getTransformationCode());
                pcl.setFileId(getRelativeFilePath(edge.getFile().getFilePath()));
                ProcessedColumnLineage.TransformationLines lines = extractTransformationLinesFromEdge(edge.getTransformationLines());
                pcl.setTransformationLines(lines);
                processedColumnLineageRepository.save(pcl);
            }
        }
    }

    private void processTableLineage(LineageEdge edge, ExtractionRun run) {
        String fromTable = extractShortName(edge.getFromTable() != null ? edge.getFromTable().toLowerCase() : null);
        String toTable = extractShortName(edge.getToTable() != null ? edge.getToTable().toLowerCase() : null);
        if (fromTable == null || toTable == null) return;
        ProcessedTableLineage.TransformationEntry te = createTransformationEntry(edge);
        updateTableLineageRelationship(run, toTable, fromTable, te, true);
        updateTableLineageRelationship(run, fromTable, toTable, te, false);
    }

    private void updateTableLineageRelationship(ExtractionRun run, String tableName, String relatedTable, ProcessedTableLineage.TransformationEntry te, boolean isUpstream) {
        Optional<ProcessedTableLineage> existingOpt = processedTableLineageRepository.findByExtractionRunRunIdAndTableName(run.getRunId(), tableName);
        ProcessedTableLineage tl = existingOpt.orElseGet(() -> {
            ProcessedTableLineage n = new ProcessedTableLineage();
            n.setExtractionRun(run);
            n.setTableName(tableName);
            n.setUpstreamTables(new ArrayList<>());
            n.setDownstreamTables(new ArrayList<>());
            return n;
        });
        List<ProcessedTableLineage.TableLineageInfo> list = isUpstream ? tl.getUpstreamTables() : tl.getDownstreamTables();
        ProcessedTableLineage.TableLineageInfo info = list.stream().filter(r -> r.getTable().equals(relatedTable)).findFirst().orElseGet(() -> {
            ProcessedTableLineage.TableLineageInfo ni = new ProcessedTableLineage.TableLineageInfo();
            ni.setTable(relatedTable);
            ni.setTransformations(new ArrayList<>());
            list.add(ni);
            return ni;
        });
        info.getTransformations().add(te);
        processedTableLineageRepository.save(tl);
    }

    // =========================
    // Shared helpers
    // =========================
    private String extractShortName(String fullId) {
        if (fullId == null || fullId.trim().isEmpty()) return null;
        String cleaned = stripTemplates(fullId.trim());
        String[] parts = cleaned.split("\\.");
        String shortName = parts[parts.length - 1];
        return shortName.trim().isEmpty() ? null : shortName;
    }

    private String deriveSource(String tableId) {
        if (tableId == null || tableId.trim().isEmpty()) return "unknown_schema";
        String cleaned = stripTemplates(tableId.trim());
        String[] parts = cleaned.split("\\.");
        if (parts.length > 1) {
            String prefix = String.join(".", Arrays.copyOf(parts, parts.length - 1));
            if (prefix.isBlank()) return "unknown_schema";
            return prefix;
        }
        return "unknown_schema";
    }

    private String stripTemplates(String s) {
        if (s == null) return null;
        String out = s.replaceAll("\\{\\{.*?\\}\\}", "").replaceAll("\\{%.*?%\\}", "");
        out = out.replaceAll("\u00A0", " ");
        return out;
    }

    @SuppressWarnings("unchecked")
    private List<ProcessedTable.FieldMetadata> extractFieldsFromTableColumns(Map<String, Object> columns) {
        List<ProcessedTable.FieldMetadata> fields = new ArrayList<>();
        if (columns == null || columns.isEmpty()) return fields;
        if (columns.containsKey("columns")) {
            Object columnsObj = columns.get("columns");
            if (columnsObj instanceof List) {
                List<Object> list = (List<Object>) columnsObj;
                for (Object col : list) {
                    if (col == null) continue;
                    String name = (col instanceof String ? (String) col : String.valueOf(col)).toLowerCase();
                    fields.add(new ProcessedTable.FieldMetadata(name, "varchar"));
                }
                LinkedHashMap<String, ProcessedTable.FieldMetadata> dedup = new LinkedHashMap<>();
                for (ProcessedTable.FieldMetadata f : fields) dedup.putIfAbsent(f.getPath(), f);
                return new ArrayList<>(dedup.values());
            }
        }
        List<ProcessedTable.FieldMetadata> alt = new ArrayList<>();
        for (Map.Entry<String, Object> e : columns.entrySet()) {
            String k = e.getKey();
            if (k == null) continue;
            k = k.toLowerCase();
            if (k.startsWith("column_") || k.matches("column_\\d+")) {
                Object v = e.getValue();
                if (v == null) continue;
                String name = (v instanceof String ? (String) v : String.valueOf(v)).toLowerCase();
                alt.add(new ProcessedTable.FieldMetadata(name, "varchar"));
            }
        }
        if (!alt.isEmpty()) {
            LinkedHashMap<String, ProcessedTable.FieldMetadata> dedup = new LinkedHashMap<>();
            for (ProcessedTable.FieldMetadata f : alt) dedup.putIfAbsent(f.getPath(), f);
            return new ArrayList<>(dedup.values());
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    private List<String> extractPartitionKeysFromTableColumns(Map<String, Object> columns) {
        List<String> keys = new ArrayList<>();
        if (columns != null && columns.containsKey("partition_keys")) {
            Object obj = columns.get("partition_keys");
            if (obj instanceof List) for (Object pk : (List<Object>) obj) {
                    if (pk instanceof Map) {
                    Object name = ((Map<String, Object>) pk).get("column_name");
                    if (name != null) keys.add(name.toString().toLowerCase());
                }
            }
        }
        return keys;
    }

    private void mergeTableDefinitions(ProcessedTable existing, TableEntity newTable, String derivedSource) {
        List<ProcessedTable.FieldMetadata> newFields = extractFieldsFromTableColumns(newTable.getColumns());
        if (newFields.size() > existing.getColumnsCount()) {
            existing.setColumnsCount(newFields.size());
            if (existing.getSchemaMetadata() == null) existing.setSchemaMetadata(new ProcessedTable.SchemaMetadata());
            existing.getSchemaMetadata().setFields(newFields);
        }
        List<String> newPartition = extractPartitionKeysFromTableColumns(newTable.getColumns());
        Set<String> merged = new LinkedHashSet<>(existing.getPartitionKeys() == null ? List.of() : existing.getPartitionKeys());
        merged.addAll(newPartition);
        existing.setPartitionKeys(new ArrayList<>(merged));
        if (existing.getSource() == null && derivedSource != null) existing.setSource(derivedSource);
    }

    private List<String> parseColumnValue(String columnValue) {
        if (columnValue == null || columnValue.trim().isEmpty()) return Collections.emptyList();
        return Collections.singletonList(columnValue.trim().toLowerCase());
    }

    private ProcessedColumnLineage.TransformationLines extractTransformationLinesFromEdge(Map<String, Object> transformationLines) {
        ProcessedColumnLineage.TransformationLines lines = new ProcessedColumnLineage.TransformationLines();
        if (transformationLines != null) {
            Object s = transformationLines.get("start_line");
            Object e = transformationLines.get("end_line");
            Object b = transformationLines.get("lines_before_start_line");
            Object a = transformationLines.get("lines_after_end_line");
            lines.setStartLine(parseLineValue(s));
            lines.setEndLine(parseLineValue(e));
            lines.setLinesBeforeStartLine(b instanceof Number ? ((Number) b).intValue() : 0);
            lines.setLinesAfterEndLine(a instanceof Number ? ((Number) a).intValue() : 0);
        }
        return lines;
    }

    private ProcessedTableLineage.TransformationEntry createTransformationEntry(LineageEdge edge) {
        ProcessedTableLineage.TransformationEntry entry = new ProcessedTableLineage.TransformationEntry();
        entry.setFileId(getRelativeFilePath(edge.getFile().getFilePath()));
        entry.setTransformationType(edge.getTransformationType());
        ProcessedTableLineage.LineRange lr = extractLineRange(edge.getTransformationLines());
        entry.setLines(lr);
        return entry;
    }

    private ProcessedTableLineage.LineRange extractLineRange(Map<String, Object> transformationLines) {
        ProcessedTableLineage.LineRange lr = new ProcessedTableLineage.LineRange();
        if (transformationLines != null) {
            Object s = transformationLines.get("start_line");
            Object e = transformationLines.get("end_line");
            lr.setStartLine(parseLineValue(s));
            lr.setEndLine(parseLineValue(e));
        }
        return lr;
    }

    private Integer parseLineValue(Object value) {
        if (value instanceof Number) {
            int v = ((Number) value).intValue();
            return v > 0 ? v : null;
        }
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.toLowerCase().startsWith("l")) s = s.substring(1);
            try { int v = Integer.parseInt(s); return v > 0 ? v : null; } catch (NumberFormatException ex) { return null; }
        }
        return null;
    }

    private String getRelativeFilePath(String filePath) { return filePath; }
}


