import os, argparse, pandas as pd
from sqlalchemy import create_engine
engine = create_engine(os.getenv("TARGET_DB_URL"))
def ingest(source):
    df = pd.read_csv(source)
    keep = ['Country', '2019_Complaints', '2019_Losses', '2020_Complaints', '2020_Losses', '2021_Complaints', '2021_Losses', '2022_Complaints', '2022_Losses', '2023_Complaints', '2023_Losses', '2024_Complaints', '2024_Losses']
    df = df[[c for c in keep if c in df.columns]]
    df.to_sql('bfsi_LossFromNetCrime', engine, if_exists='append', index=False)
if __name__ == '__main__':
    p=argparse.ArgumentParser(); p.add_argument('--source', required=True); args=p.parse_args(); ingest(args.source)
