import os, json, hashlib
from dotenv import load_dotenv
load_dotenv()

def compute_hash(obj: dict) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()

DB_URL = os.getenv("DATABASE_URL")
TARGET_DB_URL = os.getenv("TARGET_DB_URL")
HF_TOKEN = os.getenv("HUGGINGFACEHUB_API_TOKEN")
HF_MODEL = os.getenv("HF_MODEL", "mistralai/Mistral-7B-Instruct-v0.2")