#!/usr/bin/env bash
set -e
source .venv/bin/activate || true

# 1. discover
python3 -m src.discover --config config/problem.yaml --output urls.txt

# 2. extract
python3 -m src.scraper.extract --urls_file urls.txt

# 3. infer schemas
python3 -m src.schema.infer --input_dir data/raw --prefix bfsi

# 4. upsert schemas into registry

#docker exec -it llm-driven-schema-data-ingestion-cicd-postgres-1 psql -U your_user -d your_db -c "ALTER TABLE sources ADD CONSTRAINT sources_name_unique UNIQUE (name);"

python3 - <<'PY'
import json, glob
from src.schema_registry.db import upsert_schema
for f in glob.glob("data/schemas/*_schema.json"):
    s=json.load(open(f))
    upsert_schema(source_name=s.get("sample_file", f), source_type="download", table_name=s["table_name"], schema_json=s)
    print("upserted", s["table_name"])
PY

# 5. generate pipelines
python3 -m src.pipeline.generate

# 6. apply pipelines
python3 -m src.pipeline.apply