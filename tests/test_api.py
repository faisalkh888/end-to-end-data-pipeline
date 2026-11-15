from datetime import date
from app.mock_api import orders_for


def test_api_fixture_is_stable_and_has_expected_daily_volume():
    day = date(2025, 3, 1)
    assert orders_for(day) == orders_for(day)
    assert len(orders_for(day)) == 5
    assert sum(r["status"] == "completed" for r in orders_for(day)) == 4


class Response:
    status_code = 200
    headers = {}
    def __init__(self, payload): self.payload = payload
    def json(self): return self.payload
    def raise_for_status(self): pass


class PagedSession:
    def __init__(self): self.calls = 0
    def get(self, url, params, timeout):
        self.calls += 1
        if params["page"] == 1: return Response({"data": [{"order_id": 1}], "has_more": True})
        return Response({"data": [{"order_id": 2}], "has_more": False})


def test_api_extraction_follows_all_pages():
    from app.pipeline import fetch_api_orders
    session = PagedSession()
    result = fetch_api_orders(date(2025, 1, 1), date(2025, 1, 1), session)
    assert [r["order_id"] for r in result] == [1, 2]
    assert session.calls == 2


def test_api_extraction_retries_rate_limit(monkeypatch):
    from app.pipeline import fetch_api_orders
    class LimitedSession:
        calls = 0
        def get(self, url, params, timeout):
            self.calls += 1
            if self.calls == 1:
                response = Response({})
                response.status_code = 429
                response.headers = {"Retry-After": "0"}
                return response
            return Response({"data": [{"order_id": 1}], "has_more": False})
    monkeypatch.setattr("app.pipeline.time.sleep", lambda seconds: None)
    session = LimitedSession()
    assert fetch_api_orders(date(2025, 1, 1), date(2025, 1, 1), session) == [{"order_id": 1}]
    assert session.calls == 2
