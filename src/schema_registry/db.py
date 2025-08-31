# src/schema_registry/db.py
import json
from sqlalchemy import create_engine, text
from src.utils import DB_URL, compute_hash

engine = create_engine(DB_URL, future=True)

def upsert_schema(source_name: str, source_type: str, table_name: str, schema_json: dict):
    schema_hash = compute_hash(schema_json)
    with engine.begin() as conn:
        src = conn.execute(text("""
            INSERT INTO sources(name, source_type, base_url) VALUES (:n, :s, NULL)
            ON CONFLICT (name) DO UPDATE SET source_type = EXCLUDED.source_type RETURNING id
        """), dict(n=source_name, s=source_type)).first()
        source_id = src[0]
        tbl = conn.execute(text("""
            INSERT INTO tables(source_id, table_name) VALUES (:sid, :tname)
            ON CONFLICT (source_id, table_name) DO UPDATE SET table_name=EXCLUDED.table_name RETURNING id
        """), dict(sid=source_id, tname=table_name)).first()
        table_id = tbl[0]
        last = conn.execute(text("""SELECT id, version, schema_hash FROM table_versions WHERE table_id=:tid ORDER BY version DESC LIMIT 1"""), dict(tid=table_id)).first()
        if last and last[2] == schema_hash:
            return {"upserted": False}
        newv = 1 if not last else last[1] + 1
        tv = conn.execute(text("""INSERT INTO table_versions(table_id, version, schema_json, schema_hash) VALUES (:tid,:ver,:sj,:sh) RETURNING id"""), dict(tid=table_id, ver=newv, sj=json.dumps(schema_json), sh=schema_hash)).first()
        tv_id = tv[0]
        conn.execute(text("DELETE FROM columns WHERE table_version_id=:tvid"), dict(tvid=tv_id))
        for i, c in enumerate(schema_json.get("columns", []), 1):
            conn.execute(text("""INSERT INTO columns(table_version_id, name, data_type, is_nullable, is_primary_key, ordinal_position) VALUES (:tvid,:name,:dtype,:nullable,:pk,:pos)"""), dict(tvid=tv_id, name=c['name'], dtype=c.get('data_type','TEXT'), nullable=c.get('is_nullable', True), pk=c.get('is_primary_key', False), pos=i))
        conn.execute(text("UPDATE tables SET latest_version=:ver WHERE id=:tid"), dict(ver=newv, tid=table_id))
        if last:
            conn.execute(text("""INSERT INTO schema_changes(table_id, from_version, to_version, change_summary) VALUES (:tid, :fromv, :tov, :sum)"""), dict(tid=table_id, fromv=last[1], tov=newv, sum=json.dumps({"note":"auto bump"})))
    return {"upserted": True, "table_id": table_id, "version": newv}