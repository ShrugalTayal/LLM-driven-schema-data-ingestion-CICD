import os, argparse, pandas as pd
from sqlalchemy import create_engine
engine = create_engine(os.getenv("TARGET_DB_URL"))
def ingest(source):
    df = pd.read_csv(source)
    keep = ['Country', 'Year', 'Attack Type', 'Target Industry', 'Financial Loss (in Million $)', 'Number of Affected Users', 'Attack Source', 'Security Vulnerability Type', 'Defense Mechanism Used', 'Incident Resolution Time (in Hours)']
    df = df[[c for c in keep if c in df.columns]]
    df.to_sql('bfsi_Global_Cybersecurity_Threats_2015-2024', engine, if_exists='append', index=False)
if __name__ == '__main__':
    p=argparse.ArgumentParser(); p.add_argument('--source', required=True); args=p.parse_args(); ingest(args.source)
