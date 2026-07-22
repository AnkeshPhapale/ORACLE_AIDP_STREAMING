-- DESTRUCTIVE: removes all existing event data, checkpoints, ML features and predictions.
-- This intentionally discards records sent under the old event contract.
DROP TABLE IF EXISTS aidp_poc.ml.meter_predictions;
DROP TABLE IF EXISTS aidp_poc.ml.meter_features;
DROP TABLE IF EXISTS aidp_poc.gold.service_point_daily_usage;
DROP TABLE IF EXISTS aidp_poc.gold.meter_interval_usage;
DROP TABLE IF EXISTS aidp_poc.silver.device_event;
DROP TABLE IF EXISTS aidp_poc.silver.service_point;
DROP TABLE IF EXISTS aidp_poc.silver.device;
DROP TABLE IF EXISTS aidp_poc.silver.interval_reading;
DROP TABLE IF EXISTS aidp_poc.bronze.stream_consumer_checkpoint;
DROP TABLE IF EXISTS aidp_poc.bronze.meter_reading_quarantine;
DROP TABLE IF EXISTS aidp_poc.bronze.meter_reading;

-- Recreate the complete v2 contract tables immediately after the reset.
-- Execute sql/01_create_tables.sql as the next statement/job.
