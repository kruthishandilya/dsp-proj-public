-- Create Airflow metadata database (separate from app data)
CREATE DATABASE airflow_db;

-- Create ingestion_stats table in app_db (default database)
CREATE TABLE IF NOT EXISTS ingestion_stats (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    total_rows INTEGER NOT NULL,
    failed_rows INTEGER NOT NULL,
    success_rate FLOAT NOT NULL,
    criticality VARCHAR(20) NOT NULL DEFAULT 'NONE',
    null_errors INTEGER DEFAULT 0,
    range_errors INTEGER DEFAULT 0,
    type_errors INTEGER DEFAULT 0,
    categorical_errors INTEGER DEFAULT 0,
    schema_errors INTEGER DEFAULT 0,
    error_details TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
