import os, argparse, pandas as pd
from sqlalchemy import create_engine
engine = create_engine(os.getenv("TARGET_DB_URL"))

def ingest(source):
    df = pd.read_csv(source)
    keep = ['country', 'year', 'attack_type', 'target_industry', 'financial_loss__in_million___', 'number_of_affected_users', 'attack_source', 'security_vulnerability_type', 'defense_mechanism_used', 'incident_resolution_time__in_hours_']
    df = df[[c for c in keep if c in df.columns]]
    df.to_sql('bfsi_global_cybersecurity_threats_2015_2024', engine, if_exists='append', index=False)

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--source', required=True)
    args = p.parse_args()
    ingest(args.source)
