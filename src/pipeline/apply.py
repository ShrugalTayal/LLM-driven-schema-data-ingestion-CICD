# src/pipeline/apply.py
from pathlib import Path
from sqlalchemy import create_engine, text
from src.utils import TARGET_DB_URL
import subprocess

engine = create_engine(TARGET_DB_URL, future=True)

def apply_sql():
    for p in sorted(Path("generated/sql").glob("*.sql")):
        sql = p.read_text()
        with engine.begin() as conn:
            conn.execute(text(sql))
        print("applied:", p.name)

def run_ingestions():
    for p in sorted(Path("generated/ingestion").glob("ingest_*.py")):
        try:
            subprocess.check_call(["python", str(p), "--source", ""])
        except subprocess.CalledProcessError:
            print(f"Script {p.name} may require a real --source arg. Run manually.")

if __name__ == "__main__":
    apply_sql()
    run_ingestions()