# orclstream

`orclstream` is an OCI Streaming library with a simple `Producer` / `Consumer` API. It uses the official OCI Python SDK and OCI consumer groups for scalable, at-least-once consumption.

## License

Copyright (c) 2026 Ankesh Phapale. Licensed under the MIT License: the library is free to use, copy, modify, and redistribute, provided its copyright and license notice are retained.

## Install

```bash
pip install orclstream
```

## Required OCI values

You need the stream's **OCID** and its **Messages endpoint** (not the Stream Admin endpoint). Locally, authenticate through your OCI config file; in OCI Compute, Functions, or OKE, use an instance principal.

```python
from orclstream import Consumer, Producer, StreamConfig

config = StreamConfig(
    stream_id="ocid1.stream.oc1.ap-mumbai-1.exampleuniqueID",
    endpoint="https://cell-1.streaming.ap-mumbai-1.oci.oraclecloud.com",
    config_file="~/.oci/config",  # optional; this is the default
    profile="DEFAULT",             # optional; this is the default
)
```

## Produce

```python
from orclstream import Producer

producer = Producer.from_config(config)
offset = producer.produce({"event": "order_created", "order_id": 123}, key="order-123")

# Batches are more efficient; keep the entire OCI request below 1 MiB.
producer.produce_many([
    ({"event": "order_created", "order_id": 124}, "order-124"),
    ({"event": "order_created", "order_id": 125}, "order-125"),
])
```

## Consume

```python
from orclstream import Consumer

consumer = Consumer.from_config(config, group="order-service", instance="order-worker-1")

def handle(message):
    print(message.key, message.value, message.partition, message.offset)
    # save to database, call an API, etc.

consumer.run(handle)  # blocks until consumer.stop() is called
```

The consumer commits only after every handler in its received batch succeeds. If a handler fails, the batch remains uncommitted and will be delivered again: make handlers idempotent (for example, deduplicate by `partition` + `offset`).

## Scale and deploy

Start multiple application processes with the same `group` and a unique `instance` name. OCI balances the stream's partitions across those instances. The maximum useful number of instances is the number of stream partitions.

```python
producer = Producer.from_instance_principal(config)
consumer = Consumer.from_instance_principal(config, group="order-service", instance="pod-7")
```

The library uses OCI's group cursor with `commit_on_get=False`, then explicitly commits after successful handling. This gives at-least-once processing rather than losing a message if your process exits during handler execution.

## Operational notes

- OCI retry behavior is provided by the official OCI SDK. Add retry policies appropriate to your workload at the OCI client layer if needed.
- OCI limits a `put_messages` request to 1 MiB. `produce_many` validates each individual message; callers should batch below the request limit.
- Use keyed messages where ordering matters: OCI assigns matching keys to the same partition.
