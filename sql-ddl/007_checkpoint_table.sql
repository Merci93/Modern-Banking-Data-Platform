-- Checkpointing table for data transfer from MinIO to Databricks
CREATE TABLE ingestion_checkpoint (
    table_name      VARCHAR(100) PRIMARY KEY,
    object_key      TEXT NOT NULL,
    updated_at      TIMESTAMP DEFAULT NOW()
);