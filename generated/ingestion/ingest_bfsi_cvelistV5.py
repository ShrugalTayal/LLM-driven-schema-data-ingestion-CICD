import os, argparse, pandas as pd
from sqlalchemy import create_engine
engine = create_engine(os.getenv("TARGET_DB_URL"))
def ingest(source):
    df = pd.read_csv(source)
    keep = ['Name', 'Name.1', 'Name.2', 'Last commit message', 'Last commit date']
    df = df[[c for c in keep if c in df.columns]]
    df.to_sql('bfsi_cvelistV5', engine, if_exists='append', index=False)
if __name__ == '__main__':
    p=argparse.ArgumentParser(); p.add_argument('--source', required=True); args=p.parse_args(); ingest(args.source)
