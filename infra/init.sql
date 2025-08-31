CREATE TABLE IF NOT EXISTS sources (
  id SERIAL PRIMARY KEY, name TEXT NOT NULL, source_type TEXT NOT NULL, base_url TEXT, created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS tables (
  id SERIAL PRIMARY KEY, source_id INT REFERENCES sources(id) ON DELETE CASCADE, table_name TEXT NOT NULL, latest_version INT DEFAULT 1, UNIQUE(source_id, table_name)
);
CREATE TABLE IF NOT EXISTS table_versions (
  id SERIAL PRIMARY KEY, table_id INT REFERENCES tables(id) ON DELETE CASCADE, version INT NOT NULL, schema_json JSONB NOT NULL, schema_hash TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW(), UNIQUE(table_id, version)
);
CREATE TABLE IF NOT EXISTS columns (
  id SERIAL PRIMARY KEY, table_version_id INT REFERENCES table_versions(id) ON DELETE CASCADE, name TEXT NOT NULL, data_type TEXT NOT NULL, is_nullable BOOLEAN DEFAULT TRUE, is_primary_key BOOLEAN DEFAULT FALSE, ordinal_position INT
);
CREATE TABLE IF NOT EXISTS schema_changes (
  id SERIAL PRIMARY KEY, table_id INT REFERENCES tables(id) ON DELETE CASCADE, from_version INT, to_version INT, change_summary JSONB, created_at TIMESTAMP DEFAULT NOW()
);