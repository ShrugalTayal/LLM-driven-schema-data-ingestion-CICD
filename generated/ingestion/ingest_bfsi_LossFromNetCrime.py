import os, argparse, pandas as pd
from sqlalchemy import create_engine
engine = create_engine(os.getenv("TARGET_DB_URL"))

def ingest(source):
    df = pd.read_csv(source)
    keep = ['country', 't_2019_complaints', 't_2019_losses', 't_2020_complaints', 't_2020_losses', 't_2021_complaints', 't_2021_losses', 't_2022_complaints', 't_2022_losses', 't_2023_complaints', 't_2023_losses', 't_2024_complaints', 't_2024_losses']
    df = df[[c for c in keep if c in df.columns]]
    df.to_sql('bfsi_lossfromnetcrime', engine, if_exists='append', index=False)

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--source', required=True)
    args = p.parse_args()
    ingest(args.source)
