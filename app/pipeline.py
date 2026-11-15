"""Incremental, date-bounded e-commerce pipeline (bronze -> silver -> gold)."""
from __future__ import annotations
import csv
import hashlib
import json
import logging
import os
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import psycopg
import requests
from psycopg.rows import dict_row

LOG = logging.getLogger("commerce_pipeline")
API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
WAREHOUSE_DSN = os.getenv("DATABASE_URL", "postgresql://loader:local-only@localhost:5432/commerce")
SOURCE_DSN = os.getenv("SOURCE_DATABASE_URL", "postgresql://extractor:local-only@localhost:5432/commerce")
DATA_ROOT = Path(os.getenv("DATA_ROOT", "data"))


def fetch_api_orders(start: date, end: date, session=requests, retries: int = 5, updated_since: str | None = None) -> list[dict]:
    """Fetch all pages, respecting Retry-After for rate limits and transient errors."""
    rows, page = [], 1
    while True:
        for attempt in range(retries):
            try:
                params={"start_date": start.isoformat(), "end_date": end.isoformat(), "page": page, "page_size": 100}
                if updated_since: params["updated_since"] = updated_since
                response = session.get(f"{API_BASE}/orders", params=params, timeout=20)
                if response.status_code == 429 or response.status_code >= 500:
                    delay = float(response.headers.get("Retry-After", min(2 ** attempt, 30)))
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                payload = response.json()
                break
            except requests.RequestException:
                if attempt + 1 == retries:
                    raise
                time.sleep(min(2 ** attempt, 30))
        else:
            raise RuntimeError(f"API request exhausted retries on page {page}")
        rows.extend(payload["data"])
        if not payload.get("has_more", False):
            return rows
        page += 1


def validate_record(row: dict, kind: str) -> list[str]:
    """Return all validation failures; 18 explicit checks across source entities."""
    errors = []
    required = {
        "order": ["order_id", "customer_id", "product_id", "order_ts", "country", "quantity", "unit_price", "status", "updated_at"],
        "customer": ["customer_id", "full_name", "country", "updated_at"],
        "product": ["product_id", "product_name", "category", "unit_price", "updated_at"],
        "shipment": ["shipment_id", "order_id", "shipped_at", "carrier", "status"],
    }[kind]
    for key in required:
        if row.get(key) is None or row.get(key) == "": errors.append(f"{key}_required")
    if kind == "order":
        checks = [("order_id", lambda x: isinstance(x, int)), ("customer_id", lambda x: isinstance(x, int)), ("product_id", lambda x: isinstance(x, int)),
                  ("quantity", lambda x: isinstance(x, int) and 1 <= x <= 1000), ("unit_price", lambda x: isinstance(x, (int, float)) and 0 <= x <= 1000000),
                  ("country", lambda x: isinstance(x, str) and len(x) == 2 and x.isalpha()), ("status", lambda x: x in {"completed", "cancelled", "refunded", "pending"}),
                  ("order_ts", lambda x: isinstance(x, str) and "T" in x), ("updated_at", lambda x: isinstance(x, str) and "T" in x)]
    elif kind == "customer":
        checks = [("customer_id", lambda x: isinstance(x, int)), ("full_name", lambda x: isinstance(x, str) and len(x) <= 200),
                  ("email", lambda x: x is None or (isinstance(x, str) and "@" in x)), ("country", lambda x: isinstance(x, str) and len(x) == 2),
                  ("segment", lambda x: x is None or x in {"standard", "premium", "vip"}), ("updated_at", lambda x: isinstance(x, str))]
    elif kind == "product":
        checks = [("product_id", lambda x: isinstance(x, int)), ("product_name", lambda x: isinstance(x, str) and len(x) <= 200),
                  ("category", lambda x: isinstance(x, str) and len(x) > 0), ("unit_price", lambda x: isinstance(x, (int, float)) and x >= 0),
                  ("updated_at", lambda x: isinstance(x, str))]
    else:
        checks = [("shipment_id", lambda x: isinstance(x, int)), ("order_id", lambda x: isinstance(x, int)),
                  ("shipped_at", lambda x: isinstance(x, str) and "T" in x), ("carrier", lambda x: isinstance(x, str) and len(x) <= 100),
                  ("status", lambda x: x in {"shipped", "delivered", "returned"})]
    for key, predicate in checks:
        if key in row and row[key] is not None and not predicate(row[key]): errors.append(f"{key}_invalid")
    return errors


def validate_rows(rows: Iterable[dict], kind: str) -> tuple[list[dict], list[dict]]:
    good, bad = [], []
    seen = set()
    id_key = {"order": "order_id", "customer": "customer_id", "product": "product_id", "shipment": "shipment_id"}[kind]
    for row in rows:
        failures = validate_record(row, kind)
        key = row.get(id_key)
        if key in seen: failures.append("duplicate_business_key")
        seen.add(key)
        if failures: bad.append({"record": row, "errors": failures, "entity": kind})
        else: good.append(row)
    return good, bad


def read_file_drops(day: date, root: Path = DATA_ROOT) -> tuple[list[dict], list[dict]]:
    path = root / "file_drops" / f"shipments_{day.isoformat()}.csv"
    if not path.exists(): return [], []
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if len(raw) > 10 * 1024 * 1024:
        raise ValueError(f"File drop exceeds 10 MiB: {path.name}")
    with psycopg.connect(WAREHOUSE_DSN) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS ops.ingested_files (file_name text PRIMARY KEY, sha256 text UNIQUE NOT NULL, ingested_at timestamptz NOT NULL DEFAULT now())")
        exists = conn.execute("SELECT 1 FROM ops.ingested_files WHERE sha256=%s", (digest,)).fetchone()
        if exists: return [], []
        with path.open(newline="", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))
        for row in rows:
            for key in ("shipment_id", "order_id"):
                try: row[key] = int(row[key])
                except (ValueError, TypeError): row[key] = row.get(key)
        good, bad = validate_rows(rows, "shipment")
        conn.execute("INSERT INTO ops.ingested_files(file_name,sha256) VALUES (%s,%s) ON CONFLICT(file_name) DO UPDATE SET sha256=EXCLUDED.sha256, ingested_at=now()", (path.name, digest))
    return good, bad


def _init_warehouse(conn):
    conn.execute("""
      CREATE TABLE IF NOT EXISTS ops.pipeline_runs(run_id text PRIMARY KEY, logical_date date NOT NULL, started_at timestamptz NOT NULL,
        finished_at timestamptz, status text NOT NULL, source_counts jsonb NOT NULL DEFAULT '{}'::jsonb, quarantine_count int NOT NULL DEFAULT 0, error text);
      CREATE TABLE IF NOT EXISTS ops.quarantine(entity text NOT NULL, business_key text, record jsonb NOT NULL, errors jsonb NOT NULL, run_id text NOT NULL, quarantined_at timestamptz NOT NULL DEFAULT now());
      CREATE TABLE IF NOT EXISTS ops.watermarks(source text PRIMARY KEY, high_watermark timestamptz NOT NULL);
      CREATE TABLE IF NOT EXISTS warehouse.dim_date(date_key int PRIMARY KEY, full_date date UNIQUE NOT NULL, year int, month int, day int, weekday int);
      CREATE TABLE IF NOT EXISTS warehouse.dim_country(country_key int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, country_code text UNIQUE NOT NULL);
      CREATE TABLE IF NOT EXISTS warehouse.dim_product(product_key int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, product_id int NOT NULL, product_name text NOT NULL, category text NOT NULL, unit_price numeric(12,2) NOT NULL, valid_from timestamptz NOT NULL, valid_to timestamptz, is_current boolean NOT NULL, UNIQUE(product_id,valid_from));
      CREATE TABLE IF NOT EXISTS warehouse.dim_customer(customer_key int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, customer_id int NOT NULL, full_name text NOT NULL, email_hash text, country text NOT NULL, segment text, valid_from timestamptz NOT NULL, valid_to timestamptz, is_current boolean NOT NULL, UNIQUE(customer_id,valid_from));
      CREATE TABLE IF NOT EXISTS warehouse.fact_orders(order_id bigint PRIMARY KEY, date_key int NOT NULL REFERENCES warehouse.dim_date, customer_key int NOT NULL REFERENCES warehouse.dim_customer,
        product_key int NOT NULL REFERENCES warehouse.dim_product, country_key int NOT NULL REFERENCES warehouse.dim_country, quantity int NOT NULL, unit_price numeric(12,2) NOT NULL,
        revenue numeric(14,2) NOT NULL, status text NOT NULL, source_updated_at timestamptz NOT NULL);
      CREATE TABLE IF NOT EXISTS warehouse.fact_shipments(shipment_id bigint PRIMARY KEY, order_id bigint NOT NULL, shipped_date date, carrier text NOT NULL, status text NOT NULL);
    """)
    conn.execute("CREATE OR REPLACE VIEW warehouse.freshness_volume AS SELECT logical_date, status, started_at, finished_at, source_counts, quarantine_count, CASE WHEN finished_at IS NULL THEN NULL ELSE finished_at-started_at END AS duration FROM ops.pipeline_runs")


def _parquet(rows: list[dict], entity: str, layer: str, day: date):
    path = DATA_ROOT / "lake" / layer / entity / f"dt={day.isoformat()}" / "part-000.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _load_dim(conn, table: str, rows: list[dict], effective: datetime):
    if table == "dim_customer":
        for r in rows:
            row_effective = datetime.fromisoformat(r["updated_at"]) if r.get("updated_at") else effective
            email_hash = r.get("email_hash")
            cur = conn.execute("SELECT customer_key,full_name,email_hash,country,segment FROM warehouse.dim_customer WHERE customer_id=%s AND is_current", (r["customer_id"],)).fetchone()
            attrs = (r["full_name"], email_hash, r["country"], r.get("segment"))
            if cur and tuple(cur[1:]) != attrs:
                conn.execute("UPDATE warehouse.dim_customer SET valid_to=%s,is_current=false WHERE customer_key=%s", (row_effective, cur[0]))
                cur = None
            if not cur:
                conn.execute("INSERT INTO warehouse.dim_customer(customer_id,full_name,email_hash,country,segment,valid_from,is_current) VALUES (%s,%s,%s,%s,%s,%s,true)", (r["customer_id"], *attrs, row_effective))
    else:
        for r in rows:
            row_effective = datetime.fromisoformat(r["updated_at"]) if r.get("updated_at") else effective
            cur = conn.execute("SELECT product_key,product_name,category,unit_price FROM warehouse.dim_product WHERE product_id=%s AND is_current", (r["product_id"],)).fetchone()
            attrs = (r["product_name"], r["category"], r["unit_price"])
            if cur and tuple(cur[1:]) != attrs:
                conn.execute("UPDATE warehouse.dim_product SET valid_to=%s,is_current=false WHERE product_key=%s", (row_effective, cur[0]))
                cur = None
            if not cur:
                conn.execute("INSERT INTO warehouse.dim_product(product_id,product_name,category,unit_price,valid_from,is_current) VALUES (%s,%s,%s,%s,%s,true)", (r["product_id"], *attrs, row_effective))


def run_pipeline(logical_date: date, api_session=requests) -> dict:
    """Process one UTC day; safe to retry and to run repeatedly."""
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    LOG.info(json.dumps({"event":"run_started", "run_id":run_id,"logical_date":logical_date.isoformat()}))
    with psycopg.connect(WAREHOUSE_DSN) as conn:
        _init_warehouse(conn)
        conn.execute("INSERT INTO ops.pipeline_runs(run_id,logical_date,started_at,status) VALUES (%s,%s,%s,'running')", (run_id,logical_date,started))
    try:
        with psycopg.connect(WAREHOUSE_DSN) as wm_conn:
            _init_warehouse(wm_conn)
            wm_row = wm_conn.execute("SELECT high_watermark FROM ops.watermarks WHERE source='orders_api'").fetchone()
            last_success = wm_conn.execute("SELECT max(logical_date) FROM ops.pipeline_runs WHERE status='success'").fetchone()[0]
        is_backfill = last_success is not None and logical_date <= last_success
        # Watermark forward daily runs; historical/backfill dates always extract their complete slice.
        since = wm_row[0].isoformat() if not is_backfill and wm_row and logical_date > wm_row[0].date() else None
        orders = fetch_api_orders(logical_date, logical_date, api_session, updated_since=since)
        end_of_day = datetime.combine(logical_date, datetime.max.time(), timezone.utc)
        with psycopg.connect(WAREHOUSE_DSN) as wm_conn:
            pg_marks = dict(wm_conn.execute("SELECT source,high_watermark FROM ops.watermarks WHERE source IN ('postgres_customers','postgres_products')").fetchall())
        with psycopg.connect(SOURCE_DSN, row_factory=dict_row) as src:
            def extract_snapshot(table: str, watermark_name: str, columns: str):
                mark = pg_marks.get(watermark_name)
                if not is_backfill and mark and logical_date > mark.date():
                    sql = f"SELECT {columns} FROM source.{table} WHERE updated_at > %s AND updated_at <= %s ORDER BY updated_at"
                    return src.execute(sql, (mark, end_of_day)).fetchall()
                sql = f"SELECT {columns} FROM source.{table} WHERE updated_at <= %s ORDER BY updated_at"
                return src.execute(sql, (end_of_day,)).fetchall()
            customers = extract_snapshot("customers", "postgres_customers", "customer_id,full_name,email,country,segment,updated_at::text")
            products = extract_snapshot("products", "postgres_products", "product_id,product_name,category,unit_price::float,updated_at::text")
        shipments, file_bad = read_file_drops(logical_date)
        valid_orders, bad_orders = validate_rows(orders, "order")
        valid_customers, bad_customers = validate_rows([dict(r) for r in customers], "customer")
        valid_products, bad_products = validate_rows([dict(r) for r in products], "product")
        all_bad = bad_orders + bad_customers + bad_products + file_bad
        _parquet(orders, "orders", "bronze", logical_date)
        _parquet([dict(r) for r in customers], "customers", "bronze", logical_date)
        _parquet([dict(r) for r in products], "products", "bronze", logical_date)
        _parquet(shipments, "shipments", "bronze", logical_date)
        _parquet(valid_orders, "orders", "silver", logical_date)
        # Keep direct identifiers out of lake and warehouse layers.
        protected_customers = [{**r, "full_name": "customer_" + hashlib.sha256(r["full_name"].encode()).hexdigest()[:12],
                                "email_hash": hashlib.sha256(r["email"].lower().encode()).hexdigest() if r.get("email") else None}
                               for r in valid_customers]
        for customer in protected_customers: customer.pop("email", None)
        _parquet(protected_customers, "customers", "silver", logical_date)
        _parquet(valid_products, "products", "silver", logical_date)
        _parquet(shipments, "shipments", "silver", logical_date)
        with psycopg.connect(WAREHOUSE_DSN) as conn:
            _init_warehouse(conn)
            conn.execute("DELETE FROM ops.quarantine WHERE run_id=%s", (run_id,))
            for b in all_bad:
                rec=b["record"]; key=next((rec.get(k) for k in ("order_id","customer_id","product_id","shipment_id") if rec.get(k) is not None),None)
                conn.execute("INSERT INTO ops.quarantine(entity,business_key,record,errors,run_id) VALUES (%s,%s,%s,%s,%s)", (b["entity"],str(key),json.dumps(rec,default=str),json.dumps(b["errors"]),run_id))
            _load_dim(conn,"dim_customer",protected_customers,started)
            _load_dim(conn,"dim_product",valid_products,started)
            for o in valid_orders:
                ts=datetime.fromisoformat(o["order_ts"]); date_key=int(ts.strftime("%Y%m%d"))
                conn.execute("INSERT INTO warehouse.dim_date(date_key,full_date,year,month,day,weekday) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(date_key) DO NOTHING",(date_key,ts.date(),ts.year,ts.month,ts.day,ts.weekday()))
                conn.execute("INSERT INTO warehouse.dim_country(country_code) VALUES (%s) ON CONFLICT DO NOTHING",(o["country"],))
                ckey=conn.execute("SELECT customer_key FROM warehouse.dim_customer WHERE customer_id=%s AND valid_from<=%s AND (valid_to IS NULL OR valid_to>%s) ORDER BY valid_from DESC LIMIT 1",(o["customer_id"],ts,ts)).fetchone()
                pkey=conn.execute("SELECT product_key FROM warehouse.dim_product WHERE product_id=%s AND valid_from<=%s AND (valid_to IS NULL OR valid_to>%s) ORDER BY valid_from DESC LIMIT 1",(o["product_id"],ts,ts)).fetchone()
                # A newly observed source row can arrive after the event it describes; keep the fact loadable
                # while the effective-dated source change is incorporated on a subsequent reconciliation.
                if not ckey: ckey=conn.execute("SELECT customer_key FROM warehouse.dim_customer WHERE customer_id=%s AND is_current",(o["customer_id"],)).fetchone()
                if not pkey: pkey=conn.execute("SELECT product_key FROM warehouse.dim_product WHERE product_id=%s AND is_current",(o["product_id"],)).fetchone()
                country_key=conn.execute("SELECT country_key FROM warehouse.dim_country WHERE country_code=%s",(o["country"],)).fetchone()[0]
                conn.execute("""INSERT INTO warehouse.fact_orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(order_id) DO UPDATE SET date_key=EXCLUDED.date_key,customer_key=EXCLUDED.customer_key,product_key=EXCLUDED.product_key,country_key=EXCLUDED.country_key,quantity=EXCLUDED.quantity,unit_price=EXCLUDED.unit_price,revenue=EXCLUDED.revenue,status=EXCLUDED.status,source_updated_at=EXCLUDED.source_updated_at""",
                    (o["order_id"],date_key,ckey[0],pkey[0],country_key,o["quantity"],o["unit_price"],o["quantity"]*o["unit_price"],o["status"],o["updated_at"]))
            for s in shipments:
                conn.execute("INSERT INTO warehouse.fact_shipments VALUES (%s,%s,%s,%s,%s) ON CONFLICT(shipment_id) DO UPDATE SET order_id=EXCLUDED.order_id,shipped_date=EXCLUDED.shipped_date,carrier=EXCLUDED.carrier,status=EXCLUDED.status",(s["shipment_id"],s["order_id"],s["shipped_at"][:10],s["carrier"],s["status"]))
            counts={"api_orders":len(orders),"source_customers":len(valid_customers),"source_products":len(valid_products),"shipments":len(shipments)}
            if valid_orders:
                max_updated=max(datetime.fromisoformat(o["updated_at"]) for o in valid_orders)
                conn.execute("INSERT INTO ops.watermarks(source,high_watermark) VALUES ('orders_api',%s) ON CONFLICT(source) DO UPDATE SET high_watermark=GREATEST(ops.watermarks.high_watermark,EXCLUDED.high_watermark)",(max_updated,))
            for source_name, source_rows in (("postgres_customers", valid_customers), ("postgres_products", valid_products)):
                source_max=max((datetime.fromisoformat(r["updated_at"]) for r in source_rows),default=None)
                if source_max:
                    conn.execute("INSERT INTO ops.watermarks(source,high_watermark) VALUES (%s,%s) ON CONFLICT(source) DO UPDATE SET high_watermark=GREATEST(ops.watermarks.high_watermark,EXCLUDED.high_watermark)",(source_name,source_max))
            conn.execute("UPDATE ops.pipeline_runs SET finished_at=%s,status='success',source_counts=%s,quarantine_count=%s WHERE run_id=%s",(datetime.now(timezone.utc),json.dumps(counts),len(all_bad),run_id))
        return {"run_id":run_id,"counts":counts,"quarantined":len(all_bad)}
    except Exception as exc:
        with psycopg.connect(WAREHOUSE_DSN) as conn:
            _init_warehouse(conn)
            conn.execute("UPDATE ops.pipeline_runs SET finished_at=%s,status='failed',error=%s WHERE run_id=%s",(datetime.now(timezone.utc),str(exc)[:2000],run_id))
        LOG.exception("Pipeline run failed: %s",run_id)
        raise
