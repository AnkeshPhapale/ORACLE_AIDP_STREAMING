"""OCI Streaming adapter. Keep OCI SDK calls outside Spark executors."""
from base64 import b64encode
import json, oci
from src.settings import Settings

def client(settings):
    return oci.streaming.StreamClient(oci.config.from_file(settings.config_file, settings.profile), service_endpoint=settings.stream_endpoint)

def publish_events(stream_client, stream_ocid, events):
    entries=[oci.streaming.models.PutMessagesDetailsEntry(key=b64encode(e["meter_id"].encode()).decode(), value=b64encode(json.dumps(e,separators=(",", ":")).encode()).decode()) for e in events]
    result=stream_client.put_messages(stream_ocid, oci.streaming.models.PutMessagesDetails(messages=entries))
    failures=[row.error_message for row in result.data.entries if row.error]
    if failures: raise RuntimeError(f"OCI rejected {len(failures)} events: {failures[:3]}")
    return [{"event_id":events[i]["event_id"],"partition":row.partition,"offset":row.offset} for i,row in enumerate(result.data.entries)]

def get_partition_messages(stream_client, stream_ocid, partition, offset=None, limit=1000):
    details=oci.streaming.models.CreateCursorDetails(type="AFTER_OFFSET" if offset else "TRIM_HORIZON", offset=offset) if offset else oci.streaming.models.CreateCursorDetails(type="TRIM_HORIZON")
    cursor=stream_client.create_cursor(stream_ocid, partition, details).data.value
    return stream_client.get_messages(stream_ocid, cursor, limit=limit).data
