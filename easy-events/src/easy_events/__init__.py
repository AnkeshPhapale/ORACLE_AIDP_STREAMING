"""A small, dependency-free, in-memory event producer/consumer library."""

from .core import Consumer, Event, EventBus, Producer

__all__ = ["Consumer", "Event", "EventBus", "Producer"]
__version__ = "0.1.0"

