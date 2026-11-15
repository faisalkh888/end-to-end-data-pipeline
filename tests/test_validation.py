from app.pipeline import validate_record, validate_rows


def test_order_valid_and_invalid_types():
    good = {"order_id": 1, "customer_id": 2, "product_id": 3, "order_ts": "2025-01-01T12:00:00+00:00", "country": "US", "quantity": 2, "unit_price": 12.5, "status": "completed", "updated_at": "2025-01-01T13:00:00+00:00"}
    assert validate_record(good, "order") == []
    bad = {**good, "quantity": "two", "unit_price": -2, "country": "USA", "status": "mystery", "order_ts": None}
    failures = validate_record(bad, "order")
    assert {"quantity_invalid", "unit_price_invalid", "country_invalid", "status_invalid", "order_ts_required"} <= set(failures)


def test_bad_record_is_quarantined_and_duplicate_key_rejected():
    row = {"order_id": 9, "customer_id": 1, "product_id": 1, "order_ts": "2025-01-01T12:00:00Z", "country": "US", "quantity": 1, "unit_price": 4, "status": "completed", "updated_at": "2025-01-01T12:00:00Z"}
    good, quarantine = validate_rows([row, row, {**row, "order_id": 10, "quantity": 0}], "order")
    assert len(good) == 1
    assert len(quarantine) == 2
    assert "duplicate_business_key" in quarantine[0]["errors"]
    assert "quantity_invalid" in quarantine[1]["errors"]


def test_customer_product_and_shipment_schema_rules():
    customer = {"customer_id": 1, "full_name": "A Person", "email": "bad", "country": "USA", "segment": "gold", "updated_at": "now"}
    product = {"product_id": 1, "product_name": "Lamp", "category": "home", "unit_price": -1, "updated_at": "now"}
    shipment = {"shipment_id": "x", "order_id": 1, "shipped_at": "bad-date", "carrier": "carrier", "status": "lost"}
    assert {"email_invalid", "country_invalid", "segment_invalid"} <= set(validate_record(customer, "customer"))
    assert "unit_price_invalid" in validate_record(product, "product")
    assert {"shipment_id_invalid", "shipped_at_invalid", "status_invalid"} <= set(validate_record(shipment, "shipment"))


def test_null_required_fields_are_reported():
    assert "order_id_required" in validate_record({}, "order")
    assert "customer_id_required" in validate_record({"customer_id": None}, "customer")
