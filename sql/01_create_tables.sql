-- Smart-meter streaming lakehouse, schema version 2.0.
-- Run sql/00_reset_pipeline.sql first when switching from the old event contract.
CREATE SCHEMA IF NOT EXISTS aidp_poc.bronze;
CREATE SCHEMA IF NOT EXISTS aidp_poc.silver;
CREATE SCHEMA IF NOT EXISTS aidp_poc.gold;
CREATE SCHEMA IF NOT EXISTS aidp_poc.ml;

-- Immutable landing zone: raw_value is the exact event received from OCI Streaming.
CREATE TABLE IF NOT EXISTS aidp_poc.bronze.meter_reading (
  stream_partition STRING, stream_offset STRING, stream_key STRING, raw_value STRING,
  ingested_at TIMESTAMP, payload_json STRING
) USING DELTA PARTITIONED BY (stream_partition);

CREATE TABLE IF NOT EXISTS aidp_poc.bronze.meter_reading_quarantine (
  stream_partition STRING, stream_offset STRING, stream_key STRING, raw_value STRING,
  validation_reason STRING, quarantined_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS aidp_poc.bronze.stream_consumer_checkpoint (
  consumer_group STRING, stream_partition STRING, last_offset STRING, committed_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS aidp_poc.silver.interval_reading (
  schema_version STRING, event_id STRING, event_type STRING, event_ts_utc TIMESTAMP,
  meter_id STRING, device_id STRING, service_point_id STRING, service_point_type STRING,
  interval_start_utc TIMESTAMP, interval_end_utc TIMESTAMP, interval_minutes INT,
  consumption_kwh DOUBLE, demand_kw DOUBLE, voltage_v DOUBLE, current_a DOUBLE,
  power_factor DOUBLE, frequency_hz DOUBLE, temperature_c DOUBLE, humidity_pct DOUBLE,
  quality_code STRING, tariff_code STRING, meter_status STRING, firmware_version STRING,
  region_code STRING, feeder_id STRING, transformer_id STRING, customer_segment STRING,
  outage_minutes INT, tamper_flag BOOLEAN, measurement_events ARRAY<STRING>, device_events ARRAY<STRING>,
  stream_partition STRING, stream_offset STRING, stream_key STRING, ingested_at TIMESTAMP,
  reading_date DATE, is_actual BOOLEAN
) USING DELTA PARTITIONED BY (reading_date);

CREATE TABLE IF NOT EXISTS aidp_poc.silver.device (
  device_id STRING, meter_id STRING, service_point_id STRING, service_point_type STRING,
  firmware_version STRING, meter_status STRING, first_seen_at TIMESTAMP, last_seen_at TIMESTAMP,
  last_event_id STRING, updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS aidp_poc.silver.service_point (
  service_point_id STRING, service_point_type STRING, latest_meter_id STRING, region_code STRING,
  feeder_id STRING, transformer_id STRING, customer_segment STRING, first_seen_at TIMESTAMP,
  last_seen_at TIMESTAMP, updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS aidp_poc.silver.device_event (
  event_id STRING, meter_id STRING, device_id STRING, service_point_id STRING,
  interval_start_utc TIMESTAMP, device_event_type STRING, ingested_at TIMESTAMP, reading_date DATE
) USING DELTA PARTITIONED BY (reading_date);

CREATE TABLE IF NOT EXISTS aidp_poc.gold.meter_interval_usage (
  reading_date DATE, interval_start_utc TIMESTAMP, meter_id STRING, service_point_id STRING,
  region_code STRING, feeder_id STRING, transformer_id STRING, customer_segment STRING, tariff_code STRING,
  consumption_kwh DOUBLE, demand_kw DOUBLE, avg_voltage_v DOUBLE, avg_power_factor DOUBLE,
  reading_count BIGINT, actual_reading_count BIGINT, suspect_reading_count BIGINT,
  tamper_reading_count BIGINT, outage_reading_count BIGINT, refreshed_at TIMESTAMP
) USING DELTA PARTITIONED BY (reading_date);

CREATE TABLE IF NOT EXISTS aidp_poc.gold.service_point_daily_usage (
  reading_date DATE, service_point_id STRING, region_code STRING, feeder_id STRING, transformer_id STRING,
  total_kwh DOUBLE, peak_kwh DOUBLE, off_peak_kwh DOUBLE, max_demand_kw DOUBLE, interval_count BIGINT,
  actual_interval_count BIGINT, tamper_interval_count BIGINT, outage_minutes BIGINT, refreshed_at TIMESTAMP
) USING DELTA PARTITIONED BY (reading_date);

CREATE TABLE IF NOT EXISTS aidp_poc.ml.meter_features (
  meter_id STRING, interval_start_utc TIMESTAMP, reading_date DATE, target_next_kwh DOUBLE,
  lag_1_kwh DOUBLE, lag_4_kwh DOUBLE, lag_96_kwh DOUBLE, rolling_4_kwh DOUBLE,
  hour INT, day_of_week INT, is_weekend INT, temperature_c DOUBLE, humidity_pct DOUBLE,
  voltage_v DOUBLE, demand_kw DOUBLE, tariff_code STRING, customer_segment STRING
) USING DELTA PARTITIONED BY (reading_date);

CREATE TABLE IF NOT EXISTS aidp_poc.ml.meter_predictions (
  model_version STRING, meter_id STRING, interval_start_utc TIMESTAMP, prediction_kwh DOUBLE,
  scored_at TIMESTAMP
) USING DELTA PARTITIONED BY (model_version);
