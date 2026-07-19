"""Spark transformations and idempotent Delta writes for medallion layers."""
from pyspark.sql import functions as F
from src.schemas import METER_EVENT_SCHEMA

def parse_bronze(raw_df):
    return raw_df.withColumn("payload", F.from_json("raw_value", METER_EVENT_SCHEMA)).withColumn("ingested_at", F.current_timestamp())

def valid_events(parsed_df):
    p = F.col("payload")
    reasons = F.concat_ws("; ",
        F.when(p.isNull(), F.lit("INVALID_JSON_OR_SCHEMA")),
        F.when(
            p.event_id.isNull()
            | p.meter_id.isNull()
            | p.device_id.isNull()
            | p.service_point_id.isNull()
            | p.interval_start_utc.isNull()
            | p.interval_end_utc.isNull()
            | p.interval_minutes.isNull()
            | p.consumption_kwh.isNull()
            | p.quality_code.isNull()
            | p.tariff_code.isNull(),
            F.lit("MISSING_REQUIRED_FIELD"),
        ),
        F.when(p.event_type != "INTERVAL_READING", F.lit("UNSUPPORTED_EVENT_TYPE")),
        F.when(p.interval_minutes != 15, F.lit("INVALID_INTERVAL_MINUTES")),
        F.when((p.consumption_kwh < 0) | (p.consumption_kwh > 1000), F.lit("INVALID_CONSUMPTION")),
        F.when((p.power_factor < -1) | (p.power_factor > 1), F.lit("INVALID_POWER_FACTOR")),
        F.when(p.interval_end_utc != p.interval_start_utc + F.expr("INTERVAL 15 MINUTES"), F.lit("INVALID_INTERVAL_BOUNDARY")),
        F.when(~p.quality_code.isin("ACTUAL", "ESTIMATED", "SUBSTITUTED", "SUSPECT"), F.lit("INVALID_QUALITY_CODE")))
    return parsed_df.withColumn("validation_reason", reasons)

def silver_projection(valid_df):
    return valid_df.where(F.col("validation_reason") == "").select(
        "payload.*", "stream_partition", "stream_offset", "stream_key", "ingested_at",
        F.to_date("payload.interval_start_utc").alias("reading_date"),
        F.when(F.col("payload.quality_code") == "ACTUAL", F.lit(True)).otherwise(F.lit(False)).alias("is_actual"))

def merge_delta(spark, dataframe, target, key_condition):
    dataframe.createOrReplaceTempView("_batch_upsert")
    spark.sql(f"MERGE INTO {target} t USING _batch_upsert s ON {key_condition} WHEN NOT MATCHED THEN INSERT *")
