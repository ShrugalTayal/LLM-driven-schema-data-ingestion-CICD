# src/ai_codegen/hf_codegen.py
import os, re, json
from huggingface_hub import InferenceClient
from src.utils import HF_TOKEN, HF_MODEL, TARGET_DB_URL

_client = InferenceClient(model=HF_MODEL, token=HF_TOKEN) if HF_TOKEN else None

PROMPT = """You are a code generator. Input schema JSON:
{schema_json}
Return ONLY valid JSON with keys "ddl" and "ingest_python".
- ddl: PostgreSQL CREATE TABLE (IF NOT EXISTS).
- ingest_python: Python script that reads CSV (--source) and writes to TARGET_DB_URL env var using SQLAlchemy.
"""

# ------------------------
# Helpers
# ------------------------
def sanitize_name(name: str) -> str:
    """Make sure table/column names are Postgres-safe."""
    safe = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    if safe[0].isdigit():
        safe = f"t_{safe}"
    return safe.lower()

def _extract_json(text: str):
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r'(\{.*\})', text, re.S)
        if m:
            try:
                return json.loads(m.group(1))
            except:
                return None
        return None

# ------------------------
# Main codegen function
# ------------------------
def gen_from_schema(schema_json: dict):
    # fallback
    def fallback():
        table_name = sanitize_name(schema_json["table_name"])
        cols = schema_json.get("columns", [])
        parts = []
        for c in cols:
            col_name = sanitize_name(c["name"])
            col_type = c.get("data_type", "TEXT")
            line = f"{col_name} {col_type}"
            if not c.get("is_nullable", True):
                line += " NOT NULL"
            parts.append(line)

        ddl = f'CREATE TABLE IF NOT EXISTS {table_name} (\n  ' + ",\n  ".join(parts) + "\n);"

        cols_list = [sanitize_name(c["name"]) for c in cols]
        py = f"""import os, argparse, pandas as pd
from sqlalchemy import create_engine
engine = create_engine(os.getenv("TARGET_DB_URL"))

def ingest(source):
    df = pd.read_csv(source)
    keep = {cols_list}
    df = df[[c for c in keep if c in df.columns]]
    df.to_sql('{table_name}', engine, if_exists='append', index=False)

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--source', required=True)
    args = p.parse_args()
    ingest(args.source)
"""
        return {"ddl": ddl, "ingest_python": py}

    # If no client available, fallback
    if not _client:
        return fallback()

    prompt = PROMPT.format(schema_json=json.dumps(schema_json, indent=2))
    try:
        resp = _client.text_generation(prompt, max_new_tokens=512, temperature=0.0)

        # Extract text
        text = ""
        if isinstance(resp, (list, tuple)) and resp:
            text = resp[0].get("generated_text", str(resp[0]))
        elif isinstance(resp, dict):
            text = resp.get("generated_text") or str(resp)
        else:
            text = str(resp)

        data = _extract_json(text)
        if not data or "ddl" not in data or "ingest_python" not in data:
            return fallback()

        # sanitize table + column names inside LLM response
        data["ddl"] = re.sub(r'[^a-zA-Z0-9_]', '_', data["ddl"])
        if "TARGET_DB_URL" not in data["ingest_python"]:
            data["ingest_python"] = "import os\n" + data["ingest_python"]

        return data
    except Exception as e:
        print("HF codegen failed:", e)
        return fallback()