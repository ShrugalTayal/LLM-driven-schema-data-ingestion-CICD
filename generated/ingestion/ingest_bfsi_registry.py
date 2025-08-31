import os, argparse, pandas as pd
from sqlalchemy import create_engine
engine = create_engine(os.getenv("TARGET_DB_URL"))

def ingest(source):
    df = pd.read_csv(source)
    keep = ['identifier', 'name', 'homepage', 'description', 'pattern', 'example', 'email', 'uri_format', 'download_owl', 'download_obo', 'synonyms', 'deprecated', 'aberowl', 'agroportal', 'bartoc', 'biocontext', 'bioportal', 'cellosaurus', 'cheminf', 'cropoct', 'ecoportal', 'edam', 'hl7', 'integbio', 'lov', 'miriam', 'n2t', 'obofoundry', 'ols', 'ontobee', 'pathguide', 'prefixcc', 'prefixcommons', 're3data', 'rrid', 'togoid', 'uniprot', 'wikidata', 'wikidata_entity', 'zazuko', 'part_of', 'provides', 'has_canonical']
    df = df[[c for c in keep if c in df.columns]]
    df.to_sql('bfsi_registry', engine, if_exists='append', index=False)

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--source', required=True)
    args = p.parse_args()
    ingest(args.source)
