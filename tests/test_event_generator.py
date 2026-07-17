from datetime import datetime, timezone
from src.event_generator import build_interval_event, generate_batch

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
