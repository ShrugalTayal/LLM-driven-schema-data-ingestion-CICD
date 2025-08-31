# src/pipeline/generate.py
from pathlib import Path
from sqlalchemy import create_engine, text
from src.utils import DB_URL
from src.ai_codegen.hf_codegen import gen_from_schema

OUT_SQL = Path("generated/sql"); OUT_SQL.mkdir(parents=True, exist_ok=True)
OUT_ING = Path("generated/ingestion"); OUT_ING.mkdir(parents=True, exist_ok=True)

def main():
    engine = create_engine(DB_URL, future=True)
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT t.table_name, tv.schema_json
            FROM tables t
            JOIN table_versions tv ON tv.table_id=t.id AND tv.version=t.latest_version
        """)).all()
    for table_name, schema_json in rows:
        out = gen_from_schema(schema_json)
        OUT_SQL.joinpath(f"{table_name}.sql").write_text(out["ddl"])
        OUT_ING.joinpath(f"ingest_{table_name}.py").write_text(out["ingest_python"])
        print("generated:", table_name)

if __name__ == "__main__":
    main()