# Smart Meter Streaming Lakehouse (OCI Streaming + Oracle AIDP)

Production-oriented reference implementation for a 15-minute smart-meter pipeline. It generates contract-valid meter readings, publishes them to OCI Streaming, ingests them into a Delta Bronze table, produces validated Silver and reporting-ready Gold tables, and trains/scores an ML model for next-interval kWh prediction.

## Architecture

```text
Meter simulator -> OCI Streaming -> AIDP consumer -> Bronze (immutable JSON)
                                                -> Silver (validated intervals)
                                                -> Gold (15-min / daily aggregates)
                                                -> ML feature table -> model registry -> predictions
```

The event contract maps to the supplied Oracle Utilities-style entities: `D1_DEVICE`, `D1_SP`, `D1_INTERVAL_DATA`, `D1_EVENT`, and `D1_MEASUREMENT_EVENT`.

## Layout

`config/` stores the contract and runtime template; `src/` contains reusable modules; `sql/` has Delta DDL; `notebooks/` are AIDP-ready and ordered for deployment; `tests/` validates core logic.

## Setup

1. Create an OCI Streaming stream and OCI config/instance principal with publish and consume permissions.
2. Supply the variables in `config/runtime.env.example` through AIDP job settings or a secret. Never commit private keys or real OCIDs.
3. Install dependencies if needed: `pip install -r requirements.txt`.
4. Run `sql/01_create_tables.sql` in Spark SQL.
5. Run notebooks in order: producer, Bronze consumer, Silver/Gold, ML train, ML score.

## Operational model

- Bronze deduplicates by `(stream_partition, stream_offset)`; Silver by `event_id`.
- Valid readings older than 48 hours are quarantined for controlled replay; bad records are never silently dropped.
- The consumer records OCI offsets only after a successful Bronze merge.
- Schedule producer every 15 minutes; consumer every 1–5 minutes; Silver/Gold every 5–15 minutes; training daily/weekly.
- Alert on consumer failure, quarantine rate, lag, and model error/drift.

## ML use case

The baseline forecasts next 15-minute kWh from lags, hour/day, temperature, voltage, and tariff. It is a clean starting point for production monitoring; extend to outage/tamper classification once labelled event history is available.
