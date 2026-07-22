"""Core producer, consumer, and event bus types."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import RLock
from typing import Any, Callable, Dict, List, Mapping, Optional
from uuid import uuid4


EventHandler = Callable[["Event"], None]


@dataclass(frozen=True)
class Event:
    """An immutable message delivered by a :class:`Producer`."""

    topic: str
    payload: Any = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid4()))
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class Consumer:
    """Receives events for one topic or a wildcard topic such as ``orders.*``."""

    def __init__(self, topic: str, handler: EventHandler, name: Optional[str] = None) -> None:
        if not topic:
            raise ValueError("topic must not be empty")
        if not callable(handler):
            raise TypeError("handler must be callable")
        self.topic = topic
        self.handler = handler
        self.name = name or getattr(handler, "__name__", "consumer")

    def matches(self, topic: str) -> bool:
        """Return whether this consumer should receive *topic*."""
        if self.topic.endswith(".*"):
            return topic.startswith(self.topic[:-1])
        return self.topic == topic

    def consume(self, event: Event) -> None:
        """Handle an event. Usually called by :class:`EventBus`."""
        self.handler(event)


class EventBus:
    """Thread-safe in-memory registry that routes events to consumers."""

    def __init__(self) -> None:
        self._consumers: List[Consumer] = []
        self._lock = RLock()

    def subscribe(self, consumer: Consumer) -> Consumer:
        """Register a consumer and return it for convenient chaining."""
        if not isinstance(consumer, Consumer):
            raise TypeError("consumer must be a Consumer instance")
        with self._lock:
            if consumer not in self._consumers:
                self._consumers.append(consumer)
        return consumer

    def unsubscribe(self, consumer: Consumer) -> bool:
        """Remove a consumer, returning ``True`` only when it was registered."""
        with self._lock:
            if consumer not in self._consumers:
                return False
            self._consumers.remove(consumer)
            return True

    def publish(self, event: Event) -> int:
        """Synchronously deliver an event and return the number of recipients.

        Consumer exceptions are intentionally allowed to propagate so callers can
        handle failures according to their application's requirements.
        """
        if not isinstance(event, Event):
            raise TypeError("event must be an Event instance")
        with self._lock:
            recipients = [consumer for consumer in self._consumers if consumer.matches(event.topic)]
        for consumer in recipients:
            consumer.consume(event)
        return len(recipients)


class Producer:
    """Creates and publishes events through an :class:`EventBus`."""

    def __init__(self, bus: EventBus) -> None:
        if not isinstance(bus, EventBus):
            raise TypeError("bus must be an EventBus instance")
        self.bus = bus

    def publish(
        self,
        topic: str,
        payload: Any = None,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> int:
        """Publish an event and return how many consumers received it."""
        if not topic:
            raise ValueError("topic must not be empty")
        event = Event(topic=topic, payload=payload, metadata=dict(metadata or {}))
        return self.bus.publish(event)

