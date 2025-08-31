import os, json, argparse
from pathlib import Path
import pandas as pd

RAW = Path("data/raw")
SCHEMAS = Path("data/schemas")
SCHEMAS.mkdir(parents=True, exist_ok=True)

def infer_from_dataframe(df: pd.DataFrame, source: str):
    cols = []
    for i, (c, dt) in enumerate(df.dtypes.items(), start=1):
        try:
            cols.append({
                "name": str(c),
                "data_type": str(dt),
                "is_nullable": bool(df[c].isnull().any()),
                "ordinal_position": i
            })
        except Exception as e:
            raise RuntimeError(f"Error processing column '{c}' in {source}: {e}")
    return cols

def infer_from_csv(path: Path):
    try:
        if path.suffix.lower() == ".tsv":
            df = pd.read_csv(path, sep="\t", nrows=500)
        else:
            df = pd.read_csv(path, nrows=500)
        return infer_from_dataframe(df, path.name)
    except Exception as e:
        raise RuntimeError(f"Error reading {path.name}: {e}")

def infer_from_json(path: Path):
    try:
        df = pd.read_json(path, lines=True)
        return infer_from_dataframe(df, path.name)
    except Exception as e:
        raise RuntimeError(f"Error reading {path.name}: {e}")

def infer_from_html(path: Path):
    try:
        tables = pd.read_html(path)
        if not tables:
            raise ValueError("No tables found in HTML")
        df = tables[0]
        return infer_from_dataframe(df, path.name)
    except Exception as e:
        raise RuntimeError(f"Error reading HTML {path.name}: {e}")

def infer_schema(file_path: str, prefix: str = "tbl"):
    p = Path(file_path)
    try:
        if p.suffix.lower() in [".csv", ".tsv"]:
            cols = infer_from_csv(p)
        elif p.suffix.lower() == ".json":
            cols = infer_from_json(p)
        elif p.suffix.lower() == ".html":
            cols = infer_from_html(p)
        else:
            raise ValueError(f"Unsupported file type: {p.suffix}")

        schema = {
            "table_name": f"{prefix}_{p.stem}",
            "columns": cols,
            "sample_file": str(p)
        }

        out = SCHEMAS / f"{p.stem}_schema.json"
        try:
            out.write_text(json.dumps(schema, indent=2))
            print("✅ Schema written:", out)
        except Exception as e:
            raise RuntimeError(f"Error writing schema for {p.name}: {e}")

        return str(out)

    except Exception as e:
        print("⚠️ skip:", p.name, "-", e)
        return None

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input_dir", default="data/raw")
    p.add_argument("--prefix", default="tbl")
    args = p.parse_args()

    for f in Path(args.input_dir).glob("*"):
        infer_schema(str(f), prefix=args.prefix)
