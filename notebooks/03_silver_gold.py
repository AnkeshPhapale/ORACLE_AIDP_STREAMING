# AIDP notebook: validate Bronze records, quarantine bad events, and publish reporting aggregates.
from pyspark.sql import functions as F
from src.lakehouse import parse_bronze, valid_events, silver_projection
from src.settings import Settings
settings = Settings.from_env()
bronze, quarantine = settings.table("bronze", "meter_reading"), settings.table("bronze", "meter_reading_quarantine")
silver_table, gold_interval, gold_daily = settings.table("silver", "interval_reading"), settings.table("gold", "meter_interval_usage"), settings.table("gold", "service_point_daily_usage")
device_table, service_point_table, device_event_table = settings.table("silver", "device"), settings.table("silver", "service_point"), settings.table("silver", "device_event")
raw = spark.table(bronze).select("stream_partition", "stream_offset", "stream_key", "raw_value")
checked = valid_events(parse_bronze(raw))
checked = checked.withColumn("validation_reason", F.when(
    (F.col("validation_reason") == "") &
    (F.col("payload.interval_start_utc") < F.current_timestamp() - F.expr(f"INTERVAL {settings.late_arrival_hours} HOURS")),
    F.lit("LATE_BEYOND_ALLOWED_WINDOW")).otherwise(F.col("validation_reason")))
bad = checked.where("validation_reason <> ''").select("stream_partition", "stream_offset", "stream_key", "raw_value", "validation_reason", F.current_timestamp().alias("quarantined_at"))
bad.createOrReplaceTempView("bad_batch")
spark.sql(f"MERGE INTO {quarantine} t USING bad_batch s ON t.stream_partition=s.stream_partition AND t.stream_offset=s.stream_offset WHEN NOT MATCHED THEN INSERT *")
silver = silver_projection(checked); silver.createOrReplaceTempView("silver_batch")
spark.sql(f"MERGE INTO {silver_table} t USING silver_batch s ON t.event_id=s.event_id WHEN NOT MATCHED THEN INSERT *")
# Maintain D1_DEVICE/D1_SP-style current-state dimensions and an event fact table.
spark.sql(f"""MERGE INTO {device_table} t USING (SELECT device_id, meter_id, service_point_id, service_point_type, min(interval_start_utc) first_seen_at, max(interval_start_utc) last_seen_at, max_by(event_id, interval_start_utc) last_event_id, current_timestamp() updated_at FROM {silver_table} GROUP BY device_id, meter_id, service_point_id, service_point_type) s ON t.device_id=s.device_id WHEN MATCHED THEN UPDATE SET meter_id=s.meter_id, service_point_id=s.service_point_id, service_point_type=s.service_point_type, last_seen_at=s.last_seen_at, last_event_id=s.last_event_id, updated_at=s.updated_at WHEN NOT MATCHED THEN INSERT *""")
spark.sql(f"""MERGE INTO {service_point_table} t USING (SELECT service_point_id, service_point_type, max_by(meter_id, interval_start_utc) latest_meter_id, min(interval_start_utc) first_seen_at, max(interval_start_utc) last_seen_at, current_timestamp() updated_at FROM {silver_table} GROUP BY service_point_id, service_point_type) s ON t.service_point_id=s.service_point_id WHEN MATCHED THEN UPDATE SET service_point_type=s.service_point_type, latest_meter_id=s.latest_meter_id, last_seen_at=s.last_seen_at, updated_at=s.updated_at WHEN NOT MATCHED THEN INSERT *""")
spark.sql(f"""MERGE INTO {device_event_table} t USING (SELECT event_id, meter_id, device_id, service_point_id, interval_start_utc, explode(device_events) device_event_type, ingested_at, reading_date FROM {silver_table}) s ON t.event_id=s.event_id AND t.device_event_type=s.device_event_type WHEN NOT MATCHED THEN INSERT *""")
spark.sql(f"""CREATE OR REPLACE TEMP VIEW gold_interval AS SELECT reading_date, interval_start_utc, meter_id, service_point_id, tariff_code, sum(consumption_kwh) consumption_kwh, avg(voltage_v) avg_voltage_v, avg(power_factor) avg_power_factor, count(*) reading_count, sum(CASE WHEN is_actual THEN 1 ELSE 0 END) actual_reading_count, sum(CASE WHEN quality_code='SUSPECT' THEN 1 ELSE 0 END) suspect_reading_count, current_timestamp() refreshed_at FROM {silver_table} GROUP BY reading_date, interval_start_utc, meter_id, service_point_id, tariff_code""")
spark.sql(f"INSERT OVERWRITE {gold_interval} SELECT * FROM gold_interval")
spark.sql(f"""INSERT OVERWRITE {gold_daily} SELECT reading_date, service_point_id, sum(consumption_kwh), sum(CASE WHEN tariff_code='PEAK' THEN consumption_kwh ELSE 0 END), sum(CASE WHEN tariff_code='OFF_PEAK' THEN consumption_kwh ELSE 0 END), count(*), sum(actual_reading_count), max(consumption_kwh), current_timestamp() FROM {gold_interval} GROUP BY reading_date, service_point_id""")
