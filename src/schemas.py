from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

METER_EVENT_SCHEMA = StructType([
    StructField("schema_version", StringType(), False), StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False), StructField("event_ts_utc", TimestampType(), False),
    StructField("meter_id", StringType(), False), StructField("device_id", StringType(), False),
    StructField("service_point_id", StringType(), False), StructField("service_point_type", StringType()),
    StructField("interval_start_utc", TimestampType(), False), StructField("interval_end_utc", TimestampType(), False),
    StructField("interval_minutes", IntegerType(), False), StructField("consumption_kwh", DoubleType(), False),
    StructField("voltage_v", DoubleType()), StructField("current_a", DoubleType()), StructField("power_factor", DoubleType()),
    StructField("temperature_c", DoubleType()), StructField("quality_code", StringType(), False), StructField("tariff_code", StringType(), False),
    StructField("measurement_events", ArrayType(StringType())), StructField("device_events", ArrayType(StringType()))])
