from datetime import datetime, timezone
from src.event_generator import build_interval_event, generate_batch
from src.settings import Settings

def test_generator_is_deterministic_and_contract_valid():
    instant = datetime(2026, 7, 16, 10, 7, tzinfo=timezone.utc)
    first, second = build_interval_event(1, instant), build_interval_event(1, instant)
    assert first == second
    assert first["interval_minutes"] == 15
    assert first["interval_start_utc"].endswith("10:00:00+00:00")
    assert first["interval_end_utc"].endswith("10:15:00+00:00")
    assert first["consumption_kwh"] >= 0

def test_batch_is_unique_per_meter():
    rows = generate_batch(10, datetime(2026, 7, 16, tzinfo=timezone.utc))
    assert len({row["event_id"] for row in rows}) == 10


def test_settings_rejects_invalid_catalog_and_partitions(monkeypatch):
    monkeypatch.setenv("AIDP_CATALOG", "catalog; DROP TABLE x")
    try:
        Settings.from_env()
    except ValueError as error:
        assert "AIDP_CATALOG" in str(error)
    else:
        raise AssertionError("Invalid catalog should be rejected")

    monkeypatch.setenv("AIDP_CATALOG", "aidp_poc")
    monkeypatch.setenv("OCI_STREAM_PARTITIONS", "0,partition-a")
    try:
        Settings.from_env()
    except ValueError as error:
        assert "OCI_STREAM_PARTITIONS" in str(error)
    else:
        raise AssertionError("Invalid partitions should be rejected")
