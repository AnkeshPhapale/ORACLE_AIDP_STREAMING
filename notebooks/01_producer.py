# AIDP notebook: generate one interval and publish it to OCI Streaming.
from datetime import datetime, timezone
from src.event_generator import generate_batch
from src.oci_streaming import client, publish_events
from src.settings import Settings

settings = Settings.from_env()
METER_COUNT = 1000  # Set through an AIDP job parameter for production.
events = generate_batch(METER_COUNT, datetime.now(timezone.utc))
receipts = publish_events(client(settings), settings.stream_ocid, events)
print(f"Published {len(receipts)} readings; sample={receipts[0]}")
