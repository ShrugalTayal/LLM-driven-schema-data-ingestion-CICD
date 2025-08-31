# src/pipeline/apply.py
from pathlib import Path
from sqlalchemy import create_engine, text
from src.utils import TARGET_DB_URL
import subprocess

engine = create_engine(TARGET_DB_URL, future=True)

def apply_sql():
    """Apply all SQL files in generated/sql."""
    sql_dir = Path(__file__).parent.parent.parent / "generated" / "sql"
    for p in sorted(sql_dir.glob("*.sql")):
        sql = p.read_text()
        with engine.begin() as conn:
            conn.execute(text(sql))
        print("applied:", p.name)

def run_ingestions():
    """Run all ingestion scripts in generated/ingestion using matching CSVs from data/raw."""
    ingestion_dir = Path(__file__).parent.parent.parent / "generated" / "ingestion"
    data_dir = Path(__file__).parent.parent.parent / "data" / "raw"

    ingestion_scripts = sorted(ingestion_dir.glob("ingest_*.py"))
    csv_files = sorted(data_dir.glob("*.csv"))

    if not csv_files:
        print("No CSV files found in 'data/raw/'. Skipping ingestion.")
        return

    for script in ingestion_scripts:
        # Try to find a CSV file that matches the script name (ignoring 'ingest_' prefix)
        script_key = script.stem.replace("ingest_", "").lower()
        matched_csv = None
        for csv in csv_files:
            if script_key in csv.stem.lower():
                matched_csv = csv
                break

        if not matched_csv:
            print(f"No matching CSV found for {script.name}. Skipping.")
            continue

        try:
            print(f"Running {script.name} with CSV: {matched_csv.name}")
            subprocess.check_call(["python", str(script), "--source", str(matched_csv)])
        except subprocess.CalledProcessError:
            print(f"Script {script.name} failed. CSV used: {matched_csv}")

if __name__ == "__main__":
    apply_sql()
    run_ingestions()