CREATE DATABASE IF NOT EXISTS smart_meter_lakehouse;
USE smart_meter_lakehouse;

CREATE TABLE IF NOT EXISTS bronze_meter_reading (
  stream_partition STRING, stream_offset STRING, stream_key STRING, raw_value STRING,
  ingested_at TIMESTAMP, payload_json STRING
) USING DELTA PARTITIONED BY (stream_partition);

CREATE TABLE IF NOT EXISTS bronze_meter_reading_quarantine (
  stream_partition STRING, stream_offset STRING, stream_key STRING, raw_value STRING,
  validation_reason STRING, quarantined_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS stream_consumer_checkpoint (
  consumer_group STRING, stream_partition STRING, last_offset STRING, committed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS silver_interval_reading (
  schema_version STRING, event_id STRING, event_type STRING, event_ts_utc TIMESTAMP,
  meter_id STRING, device_id STRING, service_point_id STRING, service_point_type STRING,
  interval_start_utc TIMESTAMP, interval_end_utc TIMESTAMP, interval_minutes INT,
  consumption_kwh DOUBLE, voltage_v DOUBLE, current_a DOUBLE, power_factor DOUBLE, temperature_c DOUBLE,
  quality_code STRING, tariff_code STRING, measurement_events ARRAY<STRING>, device_events ARRAY<STRING>,
  stream_partition STRING, stream_offset STRING, stream_key STRING, ingested_at TIMESTAMP,
  reading_date DATE, is_actual BOOLEAN
) USING DELTA PARTITIONED BY (reading_date);

CREATE TABLE IF NOT EXISTS gold_meter_interval_usage (
  reading_date DATE, interval_start_utc TIMESTAMP, meter_id STRING, service_point_id STRING,
  tariff_code STRING, consumption_kwh DOUBLE, avg_voltage_v DOUBLE, avg_power_factor DOUBLE,
  reading_count BIGINT, actual_reading_count BIGINT, suspect_reading_count BIGINT, refreshed_at TIMESTAMP
) USING DELTA PARTITIONED BY (reading_date);

CREATE TABLE IF NOT EXISTS gold_service_point_daily_usage (
  reading_date DATE, service_point_id STRING, total_kwh DOUBLE, peak_kwh DOUBLE, off_peak_kwh DOUBLE,
  interval_count BIGINT, actual_interval_count BIGINT, max_interval_kwh DOUBLE, refreshed_at TIMESTAMP
) USING DELTA PARTITIONED BY (reading_date);

CREATE TABLE IF NOT EXISTS ml_meter_features (
  meter_id STRING, interval_start_utc TIMESTAMP, reading_date DATE, target_next_kwh DOUBLE,
  lag_1_kwh DOUBLE, lag_4_kwh DOUBLE, lag_96_kwh DOUBLE, rolling_4_kwh DOUBLE, hour INT,
  day_of_week INT, is_weekend INT, temperature_c DOUBLE, voltage_v DOUBLE, tariff_code STRING
) USING DELTA PARTITIONED BY (reading_date);

CREATE TABLE IF NOT EXISTS ml_meter_predictions (
  model_version STRING, meter_id STRING, interval_start_utc TIMESTAMP, prediction_kwh DOUBLE,
  scored_at TIMESTAMP
) USING DELTA PARTITIONED BY (model_version);
