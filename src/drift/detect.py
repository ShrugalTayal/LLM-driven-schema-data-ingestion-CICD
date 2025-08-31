# src/drift/detect.py
from sqlalchemy import create_engine, text
from src.utils import DB_URL
from deepdiff import DeepDiff
import json

engine = create_engine(DB_URL, future=True)

def main():
    with engine.begin() as conn:
        tables = conn.execute(text("SELECT id, table_name FROM tables")).all()
        for tid, name in tables:
            vers = conn.execute(text("""
              SELECT version, schema_json FROM table_versions WHERE table_id=:tid ORDER BY version DESC LIMIT 2
            """), dict(tid=tid)).all()
            if len(vers) < 2:
                print(name, "only one version")
                continue
            newer, older = vers[0][1], vers[1][1]
            dd = DeepDiff(older, newer, ignore_order=True).to_dict()
            if dd:
                print(name, "drift detected:", json.dumps(dd, indent=2))
            else:
                print(name, "no drift")

if __name__ == "__main__":
    main()