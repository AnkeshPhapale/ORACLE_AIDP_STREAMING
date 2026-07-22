"""Simple, reliable producer and consumer primitives for OCI Streaming."""

from .client import Consumer, Message, Producer, StreamConfig
from .exceptions import ConfigurationError, HandlerError, PartialPublishError, OrclStreamError

__all__ = [
    "ConfigurationError",
    "Consumer",
    "HandlerError",
    "Message",
    "OrclStreamError",
    "PartialPublishError",
    "Producer",
    "StreamConfig",
]
__version__ = "0.1.0"

