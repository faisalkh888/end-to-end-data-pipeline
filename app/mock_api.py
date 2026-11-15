"""Deterministic paginated orders API used by the local stack."""
from datetime import date, datetime, timedelta
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="Commerce Orders API", version="1.0")


def orders_for(day: date) -> list[dict]:
    rows = []
    customers = [(1, "US"), (2, "GB"), (3, "CA")]
    products = [101, 102, 103]
    for n in range(1, 6):
        customer_id, country = customers[(n + day.day) % 3]
        rows.append({
            "order_id": int(day.strftime("%Y%m%d")) * 10 + n,
            "customer_id": customer_id,
            "product_id": products[(n + day.toordinal()) % 3],
            "order_ts": f"{day.isoformat()}T{9 + n}:15:00+00:00",
            "country": country,
            "quantity": 1 + n % 3,
            "unit_price": [24.0, 18.5, 42.0][(n + day.toordinal()) % 3],
            "status": "completed" if n != 5 else "cancelled",
            "updated_at": f"{day.isoformat()}T{15 + n}:00:00+00:00",
        })
    return rows


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/orders")
def get_orders(start_date: date, end_date: date, page: int = Query(1, ge=1), page_size: int = Query(2, ge=1, le=100), updated_since: str | None = None):
    if end_date < start_date or (end_date - start_date).days > 3660:
        raise HTTPException(400, "Invalid or excessively wide date range")
    rows = []
    cursor = start_date
    while cursor <= end_date:
        rows.extend(orders_for(cursor))
        cursor += timedelta(days=1)
    if updated_since:
        cutoff = datetime.fromisoformat(updated_since.replace("Z", "+00:00"))
        rows = [row for row in rows if datetime.fromisoformat(row["updated_at"]) > cutoff]
    offset = (page - 1) * page_size
    return {"data": rows[offset:offset + page_size], "page": page, "has_more": offset + page_size < len(rows)}
