# AIDP notebook: validate Bronze records, quarantine bad events, and publish reporting aggregates.
from pyspark.sql import functions as F
from src.lakehouse import parse_bronze, valid_events, silver_projection
from src.settings import Settings
settings = Settings.from_env(); spark.sql(f"USE {settings.database}")
raw = spark.table("bronze_meter_reading").select("stream_partition", "stream_offset", "stream_key", "raw_value")
checked = valid_events(parse_bronze(raw))
checked = checked.withColumn("validation_reason", F.when(
    (F.col("validation_reason") == "") &
    (F.col("payload.interval_start_utc") < F.current_timestamp() - F.expr(f"INTERVAL {settings.late_arrival_hours} HOURS")),
    F.lit("LATE_BEYOND_ALLOWED_WINDOW")).otherwise(F.col("validation_reason")))
bad = checked.where("validation_reason <> ''").select("stream_partition", "stream_offset", "stream_key", "raw_value", "validation_reason", F.current_timestamp().alias("quarantined_at"))
bad.createOrReplaceTempView("bad_batch")
spark.sql("MERGE INTO bronze_meter_reading_quarantine t USING bad_batch s ON t.stream_partition=s.stream_partition AND t.stream_offset=s.stream_offset WHEN NOT MATCHED THEN INSERT *")
silver = silver_projection(checked); silver.createOrReplaceTempView("silver_batch")
spark.sql("MERGE INTO silver_interval_reading t USING silver_batch s ON t.event_id=s.event_id WHEN NOT MATCHED THEN INSERT *")
spark.sql("""CREATE OR REPLACE TEMP VIEW gold_interval AS SELECT reading_date, interval_start_utc, meter_id, service_point_id, tariff_code, sum(consumption_kwh) consumption_kwh, avg(voltage_v) avg_voltage_v, avg(power_factor) avg_power_factor, count(*) reading_count, sum(CASE WHEN is_actual THEN 1 ELSE 0 END) actual_reading_count, sum(CASE WHEN quality_code='SUSPECT' THEN 1 ELSE 0 END) suspect_reading_count, current_timestamp() refreshed_at FROM silver_interval_reading GROUP BY reading_date, interval_start_utc, meter_id, service_point_id, tariff_code""")
spark.sql("INSERT OVERWRITE gold_meter_interval_usage SELECT * FROM gold_interval")
spark.sql("""INSERT OVERWRITE gold_service_point_daily_usage SELECT reading_date, service_point_id, sum(consumption_kwh), sum(CASE WHEN tariff_code='PEAK' THEN consumption_kwh ELSE 0 END), sum(CASE WHEN tariff_code='OFF_PEAK' THEN consumption_kwh ELSE 0 END), count(*), sum(actual_reading_count), max(consumption_kwh), current_timestamp() FROM gold_meter_interval_usage GROUP BY reading_date, service_point_id""")
