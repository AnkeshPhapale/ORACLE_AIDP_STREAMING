# AIDP notebook: polling OCI consumer with durable checkpoints and idempotent Bronze writes.
import base64
from pyspark.sql import Row, functions as F
from src.oci_streaming import client, get_partition_messages
from src.settings import Settings

settings = Settings.from_env(require_stream=True)
bronze = settings.table("bronze", "meter_reading")
checkpoint_table = settings.table("bronze", "stream_consumer_checkpoint")
# Must list every OCI stream partition in OCI_STREAM_PARTITIONS, for example "0,1,2".
for partition in settings.stream_partitions:
    checkpoint = spark.sql(f"SELECT last_offset FROM {checkpoint_table} WHERE consumer_group='{settings.consumer_group}' AND stream_partition='{partition}' ORDER BY committed_at DESC LIMIT 1").collect()
    response = get_partition_messages(client(settings), settings.stream_ocid, partition, checkpoint[0].last_offset if checkpoint else None)
    rows = [Row(stream_partition=partition, stream_offset=str(m.offset), stream_key=base64.b64decode(m.key).decode() if m.key else None, raw_value=base64.b64decode(m.value).decode()) for m in response.messages]
    if not rows: continue
    batch = spark.createDataFrame(rows).withColumn("ingested_at", F.current_timestamp()).withColumn("payload_json", F.col("raw_value"))
    batch.createOrReplaceTempView("bronze_batch")
    spark.sql(f"MERGE INTO {bronze} t USING bronze_batch s ON t.stream_partition=s.stream_partition AND t.stream_offset=s.stream_offset WHEN NOT MATCHED THEN INSERT *")
    # OCI returns messages in cursor order; do not lexicographically sort string offsets.
    last_offset = rows[-1].stream_offset
    spark.createDataFrame([(settings.consumer_group, partition, last_offset)], "consumer_group string, stream_partition string, last_offset string").withColumn("committed_at", F.current_timestamp()).createOrReplaceTempView("checkpoint_batch")
    spark.sql(f"INSERT INTO {checkpoint_table} SELECT * FROM checkpoint_batch")
