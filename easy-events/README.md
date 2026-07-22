# easy-events

`easy-events` is a small, dependency-free, in-memory event library. It is useful for connecting parts of one Python application without tying them directly together.

## Install

```bash
pip install easy-events
```

For local development:

```bash
pip install -e .
```

## Quick start

```python
from easy_events import Consumer, EventBus, Producer

bus = EventBus()
producer = Producer(bus)

def welcome(event):
    print(f"Welcome, {event.payload['name']}!")

consumer = Consumer("user.created", welcome)
bus.subscribe(consumer)

producer.publish("user.created", {"name": "Ada"})
```

`publish()` delivers events immediately to all matching consumers and returns the number of consumers that received the event.

## Topics and wildcards

Consumers can subscribe to a precise topic or a prefix wildcard.

```python
bus.subscribe(Consumer("orders.*", lambda event: print(event.topic)))
producer.publish("orders.created", {"id": 42})
```

Use `bus.unsubscribe(consumer)` when a consumer no longer needs events.

## Scope

This package deliberately keeps events in the current Python process. For delivery across servers or persistence/retries, pair your application with a broker such as RabbitMQ, Kafka, Redis Streams, or a cloud queue.

