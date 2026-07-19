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

`config/` stores the contract and runtime template; `sql/` has Delta DDL; `notebooks/` contains the complete self-contained AIDP implementation in deployment order.

## Setup

1. Create an OCI Streaming stream and OCI config/instance principal with publish and consume permissions.
2. Supply the variables in `config/runtime.env.example` through AIDP job settings or a secret. Never commit private keys or real OCIDs.
3. Install dependencies if needed: `pip install -r requirements.txt`.
4. Run `sql/01_create_tables.sql` in Spark SQL. It creates the Bronze, Silver, Gold, and ML schemas under catalog `aidp_poc`.
5. Run notebooks in order: producer, Bronze consumer, Silver/Gold, ML train, ML score.

## First successful run

1. In AIDP, upload the repository's five `.ipynb` files. Each notebook is self-contained and has no dependency on a local `src` module.
2. Create an OCI Streaming stream, configure an OCI credential available to the AIDP runtime, and set the variables from `config/runtime.env.example` as job environment variables or secrets. Set `OCI_STREAM_PARTITIONS` to every partition ID in the stream.
3. Execute `sql/01_create_tables.sql` in the target AIDP Spark SQL catalog. If your catalog differs, set the same value in `AIDP_CATALOG` before running the notebooks.
4. Run `01_producer.ipynb`, then `02_consumer_bronze.ipynb`, then `03_silver_gold.ipynb`. Verify that rows appear in Bronze, Silver, and Gold before proceeding.
5. Schedule the first three notebooks: producer every 15 minutes, consumer every 1--5 minutes, and Silver/Gold every 5--15 minutes.
6. Before training, accumulate at least 97 valid 15-minute intervals per meter, because the feature set uses a 96-interval lag. For a controlled backfill, set `INTERVAL_START_UTC` to a different UTC 15-minute timestamp for each producer run; do not rerun the same timestamp.
7. Run `04_ml_train.ipynb`. Review the printed MAE/RMSE and promote its saved model to the approved path/version configured in `05_ml_score.ipynb` (or set its `MODEL_URI` and `MODEL_VERSION` environment variables).
8. Run `05_ml_score.ipynb`, then schedule it after each successful Silver/Gold refresh.

## Operational model

- Bronze deduplicates by `(stream_partition, stream_offset)`; Silver by `event_id`.
- Valid readings older than 48 hours are quarantined for controlled replay; bad records are never silently dropped.
- The consumer records OCI offsets only after a successful Bronze merge. Set `OCI_STREAM_PARTITIONS` to every OCI stream partition (for example, `0,1,2`) or those partitions will not be processed.
- Schedule producer every 15 minutes; consumer every 1–5 minutes; Silver/Gold every 5–15 minutes; training daily/weekly.
- Alert on consumer failure, quarantine rate, lag, and model error/drift.

## AIDP catalog namespace

All notebook table access uses `AIDP_CATALOG.layer.table_name`, for example `aidp_poc.bronze.meter_reading`. The project uses the schemas `bronze`, `silver`, `gold`, and `ml`. AIDP Master Catalog is the top-level governance container; managed tables belong in a standard catalog inside it. See Oracle's [Master Catalog documentation](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/manage-master-catalog.html) and [schema documentation](https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/schemas.html).

## ML use case

The baseline forecasts next 15-minute kWh from lags, hour/day, temperature, voltage, and tariff. It is a clean starting point for production monitoring; extend to outage/tamper classification once labelled event history is available.
