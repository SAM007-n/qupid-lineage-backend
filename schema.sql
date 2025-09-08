-- QupidLineage Backend Database Schema
-- PostgreSQL 15+ compatible

-- Create database (run this separately if needed)
-- CREATE DATABASE lineage_extractor;

-- Connect to the database
-- \c lineage_extractor;

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS job_status CASCADE;
DROP TABLE IF EXISTS lineage_edges CASCADE;
DROP TABLE IF EXISTS tables CASCADE;
DROP TABLE IF EXISTS files CASCADE;
DROP TABLE IF EXISTS extraction_runs CASCADE;

-- Create extraction_runs table
CREATE TABLE extraction_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    repository_url VARCHAR(500) NOT NULL,
    branch VARCHAR(100) NOT NULL DEFAULT 'main',
    commit_hash VARCHAR(100),
    commit_timestamp TIMESTAMP,
    run_mode VARCHAR(20) NOT NULL DEFAULT 'FULL' CHECK (run_mode IN ('FULL', 'INCREMENTAL')),
    phase VARCHAR(20) NOT NULL DEFAULT 'STARTED' CHECK (phase IN ('STARTED', 'COMPLETED', 'FAILED')),
    triggered_by VARCHAR(100) NOT NULL,
    extractor_version VARCHAR(50) NOT NULL DEFAULT '1.0.0',
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    stats JSONB DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create files table
CREATE TABLE files (
    file_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES extraction_runs(run_id) ON DELETE CASCADE,
    file_path VARCHAR(500) NOT NULL,
    file_type VARCHAR(20) NOT NULL CHECK (file_type IN ('SQL', 'JINJA2')),
    file_url VARCHAR(500),
    file_hash VARCHAR(64) NOT NULL,
    last_modified_at TIMESTAMP,
    extracted_at TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS' CHECK (status IN ('SUCCESS', 'FAILED', 'SKIPPED')),
    raw_sql_content TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create tables table
CREATE TABLE tables (
    table_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    table_role VARCHAR(50) NOT NULL CHECK (table_role IN ('SOURCE', 'TARGET', 'INTERMEDIATE')),
    table_schema VARCHAR(100),
    columns JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create lineage_edges table
CREATE TABLE lineage_edges (
    edge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id UUID NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    edge_type VARCHAR(50) NOT NULL CHECK (edge_type IN ('TABLE_EDGE', 'COLUMN_EDGE')),
    from_table VARCHAR(255) NOT NULL,
    from_column VARCHAR(255),
    to_table VARCHAR(255) NOT NULL,
    to_column VARCHAR(255),
    transformation_type VARCHAR(100) NOT NULL,
    transformation_lines JSONB DEFAULT '{}',
    transformation_code TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create job_status table
CREATE TABLE job_status (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES extraction_runs(run_id) ON DELETE CASCADE,
    pod_id VARCHAR(100),
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    total_files INTEGER NOT NULL DEFAULT 0,
    processed_files INTEGER NOT NULL DEFAULT 0,
    succeeded_files INTEGER NOT NULL DEFAULT 0,
    failed_files INTEGER NOT NULL DEFAULT 0,
    current_file VARCHAR(500),
    processing_speed DECIMAL(10,2),
    estimated_time_remaining DECIMAL(10,2),
    current_phase VARCHAR(100),
    error_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create additional tables for extended functionality
CREATE TABLE extraction_logs (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES extraction_runs(run_id) ON DELETE CASCADE,
    log_level VARCHAR(20) NOT NULL CHECK (log_level IN ('DEBUG', 'INFO', 'WARN', 'ERROR')),
    message TEXT NOT NULL,
    details JSONB DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE aggregated_tables (
    agg_table_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES extraction_runs(run_id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    table_schema VARCHAR(100),
    total_references INTEGER NOT NULL DEFAULT 0,
    source_files JSONB DEFAULT '[]',
    target_files JSONB DEFAULT '[]',
    columns_info JSONB DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fine_grained_lineage (
    lineage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES extraction_runs(run_id) ON DELETE CASCADE,
    source_table VARCHAR(255) NOT NULL,
    source_column VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    target_column VARCHAR(255) NOT NULL,
    transformation_logic TEXT,
    confidence_score DECIMAL(3,2) DEFAULT 1.0,
    file_path VARCHAR(500),
    line_number INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE table_relationships (
    relationship_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES extraction_runs(run_id) ON DELETE CASCADE,
    parent_table VARCHAR(255) NOT NULL,
    child_table VARCHAR(255) NOT NULL,
    relationship_type VARCHAR(50) NOT NULL,
    join_conditions JSONB DEFAULT '{}',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_extraction_runs_repo_url ON extraction_runs(repository_url);
CREATE INDEX idx_extraction_runs_created_at ON extraction_runs(created_at DESC);
CREATE INDEX idx_extraction_runs_phase ON extraction_runs(phase);
CREATE INDEX idx_extraction_runs_branch ON extraction_runs(branch);

CREATE INDEX idx_files_run_id ON files(run_id);
CREATE INDEX idx_files_file_path ON files(file_path);
CREATE INDEX idx_files_status ON files(status);
CREATE INDEX idx_files_file_type ON files(file_type);

CREATE INDEX idx_tables_file_id ON tables(file_id);
CREATE INDEX idx_tables_table_name ON tables(table_name);
CREATE INDEX idx_tables_table_role ON tables(table_role);
CREATE INDEX idx_tables_table_schema ON tables(table_schema);

CREATE INDEX idx_lineage_edges_file_id ON lineage_edges(file_id);
CREATE INDEX idx_lineage_edges_from_table ON lineage_edges(from_table);
CREATE INDEX idx_lineage_edges_to_table ON lineage_edges(to_table);
CREATE INDEX idx_lineage_edges_edge_type ON lineage_edges(edge_type);

CREATE INDEX idx_job_status_run_id ON job_status(run_id);
CREATE INDEX idx_job_status_status ON job_status(status);
CREATE INDEX idx_job_status_last_updated ON job_status(last_updated DESC);

CREATE INDEX idx_extraction_logs_run_id ON extraction_logs(run_id);
CREATE INDEX idx_extraction_logs_level ON extraction_logs(log_level);
CREATE INDEX idx_extraction_logs_created_at ON extraction_logs(created_at DESC);

CREATE INDEX idx_aggregated_tables_run_id ON aggregated_tables(run_id);
CREATE INDEX idx_aggregated_tables_table_name ON aggregated_tables(table_name);

CREATE INDEX idx_fine_grained_lineage_run_id ON fine_grained_lineage(run_id);
CREATE INDEX idx_fine_grained_lineage_source ON fine_grained_lineage(source_table, source_column);
CREATE INDEX idx_fine_grained_lineage_target ON fine_grained_lineage(target_table, target_column);

CREATE INDEX idx_table_relationships_run_id ON table_relationships(run_id);
CREATE INDEX idx_table_relationships_parent ON table_relationships(parent_table);
CREATE INDEX idx_table_relationships_child ON table_relationships(child_table);

-- Create trigger function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- Create triggers
CREATE TRIGGER update_extraction_runs_updated_at 
    BEFORE UPDATE ON extraction_runs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create views for common queries
CREATE VIEW extraction_run_summary AS
SELECT 
    er.run_id,
    er.repository_url,
    er.branch,
    er.phase,
    er.started_at,
    er.finished_at,
    COUNT(f.file_id) as total_files,
    COUNT(CASE WHEN f.status = 'SUCCESS' THEN 1 END) as successful_files,
    COUNT(CASE WHEN f.status = 'FAILED' THEN 1 END) as failed_files,
    COUNT(t.table_id) as total_tables,
    COUNT(le.edge_id) as total_edges
FROM extraction_runs er
LEFT JOIN files f ON er.run_id = f.run_id
LEFT JOIN tables t ON f.file_id = t.file_id
LEFT JOIN lineage_edges le ON f.file_id = le.file_id
GROUP BY er.run_id, er.repository_url, er.branch, er.phase, er.started_at, er.finished_at;

CREATE VIEW table_lineage_summary AS
SELECT 
    t.table_name,
    t.table_schema,
    t.table_role,
    COUNT(CASE WHEN le.edge_type = 'TABLE_EDGE' AND le.from_table = t.table_name THEN 1 END) as outgoing_edges,
    COUNT(CASE WHEN le.edge_type = 'TABLE_EDGE' AND le.to_table = t.table_name THEN 1 END) as incoming_edges,
    f.file_path,
    er.run_id
FROM tables t
JOIN files f ON t.file_id = f.file_id
JOIN extraction_runs er ON f.run_id = er.run_id
LEFT JOIN lineage_edges le ON f.file_id = le.file_id
GROUP BY t.table_name, t.table_schema, t.table_role, f.file_path, er.run_id;

-- Insert sample data for testing
INSERT INTO extraction_runs (repository_url, branch, triggered_by, extractor_version)
VALUES 
    ('https://github.com/example/sample-repo', 'main', 'system', '1.0.0'),
    ('https://github.com/dbt-labs/jaffle_shop', 'main', 'test-user', '1.0.0');

-- Create a user for the application (optional, adjust as needed)
-- CREATE USER lineage_user WITH PASSWORD 'lineage_password';
-- GRANT ALL PRIVILEGES ON DATABASE lineage_extractor TO lineage_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO lineage_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO lineage_user;

-- Display schema information
SELECT 'Schema created successfully!' as message;

-- Show table counts
SELECT 
    'extraction_runs' as table_name, COUNT(*) as record_count FROM extraction_runs
UNION ALL
SELECT 'files', COUNT(*) FROM files
UNION ALL
SELECT 'tables', COUNT(*) FROM tables
UNION ALL
SELECT 'lineage_edges', COUNT(*) FROM lineage_edges
UNION ALL
SELECT 'job_status', COUNT(*) FROM job_status
UNION ALL
SELECT 'extraction_logs', COUNT(*) FROM extraction_logs
UNION ALL
SELECT 'aggregated_tables', COUNT(*) FROM aggregated_tables
UNION ALL
SELECT 'fine_grained_lineage', COUNT(*) FROM fine_grained_lineage
UNION ALL
SELECT 'table_relationships', COUNT(*) FROM table_relationships;
