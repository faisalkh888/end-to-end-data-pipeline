import os
from datetime import date, datetime, timezone

import psycopg
import pytest

from app.pipeline import _init_warehouse, _load_dim, run_pipeline


pytestmark = pytest.mark.skipif(not os.getenv("DATABASE_URL"), reason="warehouse integration test requires DATABASE_URL")


def test_scd2_tracks_two_customer_attribute_versions():
    dsn = os.environ["DATABASE_URL"]
    customer_id = 987654321
    with psycopg.connect(dsn) as conn:
        _init_warehouse(conn)
        conn.execute("DELETE FROM warehouse.dim_customer WHERE customer_id=%s", (customer_id,))
        _load_dim(conn, "dim_customer", [{"customer_id": customer_id, "full_name": "customer_a", "email_hash": "hash_a", "country": "US", "segment": "standard"}], datetime(2025, 1, 1, tzinfo=timezone.utc))
        _load_dim(conn, "dim_customer", [{"customer_id": customer_id, "full_name": "customer_a", "email_hash": "hash_a", "country": "GB", "segment": "premium"}], datetime(2025, 2, 1, tzinfo=timezone.utc))
        _load_dim(conn, "dim_customer", [{"customer_id": customer_id, "full_name": "customer_a", "email_hash": "hash_a", "country": "CA", "segment": "vip"}], datetime(2025, 3, 1, tzinfo=timezone.utc))
        rows = conn.execute("SELECT segment,valid_from,valid_to,is_current FROM warehouse.dim_customer WHERE customer_id=%s ORDER BY valid_from", (customer_id,)).fetchall()
        assert len(rows) == 3
        assert rows[0][0] == "standard" and rows[0][2] == datetime(2025, 2, 1, tzinfo=timezone.utc) and rows[0][3] is False
        assert rows[1][0] == "premium" and rows[1][2] == datetime(2025, 3, 1, tzinfo=timezone.utc) and rows[1][3] is False
        assert rows[2][0] == "vip" and rows[2][2] is None and rows[2][3] is True
        conn.execute("DELETE FROM warehouse.dim_customer WHERE customer_id=%s", (customer_id,))


def test_same_day_run_has_identical_fact_totals():
    logical_day = date(2025, 3, 1)
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        _init_warehouse(conn)
        before = conn.execute("SELECT count(*),coalesce(sum(revenue),0) FROM warehouse.fact_orders WHERE date_key=20250301").fetchone()
    run_pipeline(logical_day)
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        after = conn.execute("SELECT count(*),coalesce(sum(revenue),0) FROM warehouse.fact_orders WHERE date_key=20250301").fetchone()
    assert after == before == (5, before[1])


def test_invalid_api_record_is_persisted_to_quarantine_not_gold():
    class Response:
        status_code = 200
        headers = {}
        def json(self): return {"data": [{"order_id": 202503029999, "customer_id": 1, "product_id": 101,
            "order_ts": "2025-03-02T10:00:00+00:00", "country": "US", "quantity": 0,
            "unit_price": 24.0, "status": "completed", "updated_at": "2025-03-02T11:00:00+00:00"}], "has_more": False}
        def raise_for_status(self): pass
    class Session:
        def get(self, *args, **kwargs): return Response()

    result = run_pipeline(date(2025, 3, 2), api_session=Session())
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        quarantine = conn.execute("SELECT entity,errors FROM ops.quarantine WHERE run_id=%s", (result["run_id"],)).fetchone()
        gold_count = conn.execute("SELECT count(*) FROM warehouse.fact_orders WHERE order_id=202503029999").fetchone()[0]
    assert result["quarantined"] == 1
    assert quarantine[0] == "order" and "quantity_invalid" in quarantine[1]
    assert gold_count == 0
