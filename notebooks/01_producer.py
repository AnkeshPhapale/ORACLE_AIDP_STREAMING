# AIDP notebook: generate one interval and publish it to OCI Streaming.
from datetime import datetime, timezone
import os
from src.event_generator import generate_batch
from src.oci_streaming import client, publish_events
from src.settings import Settings

settings = Settings.from_env(require_stream=True)
METER_COUNT = int(os.getenv("METER_COUNT", "1000"))
interval_value = os.getenv("INTERVAL_START_UTC")
interval_start = datetime.fromisoformat(interval_value) if interval_value else datetime.now(timezone.utc)
if interval_start.tzinfo is None:
    raise ValueError("INTERVAL_START_UTC must include a timezone, for example 2026-07-19T10:00:00+00:00")

events = generate_batch(METER_COUNT, interval_start)
receipts = publish_events(client(settings), settings.stream_ocid, events)
print(f"Published {len(receipts)} readings for {interval_start.isoformat()}; sample={receipts[0]}")
