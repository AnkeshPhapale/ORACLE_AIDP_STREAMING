"""OCI Streaming producer and consumer implementations."""

from __future__ import annotations

from base64 import b64decode, b64encode
from dataclasses import dataclass
from threading import Event as StopEvent
from time import sleep
from typing import Any, Callable, Iterable, List, Optional, Protocol, Sequence
from uuid import uuid4

from .codec import deserialize, serialize
from .exceptions import ConfigurationError, HandlerError, PartialPublishError


class _Response(Protocol):
    data: Any
    headers: dict


@dataclass(frozen=True)
class StreamConfig:
    """Connection values required by OCI's message endpoint.

    ``stream_id`` is the stream OCID. ``endpoint`` is the stream's *Messages
    endpoint*, not the Stream Admin endpoint.
    """

    stream_id: str
    endpoint: str
    config_file: str = "~/.oci/config"
    profile: str = "DEFAULT"
    timeout: tuple = (10, 60)

    def __post_init__(self) -> None:
        if not self.stream_id.startswith("ocid1."):
            raise ConfigurationError("stream_id must be an OCI stream OCID")
        if not self.endpoint.startswith(("https://", "http://")):
            raise ConfigurationError("endpoint must be the OCI Streaming Messages URL")


@dataclass(frozen=True)
class Message:
    """A decoded OCI Streaming message passed to a consumer handler."""

    value: Any
    key: Optional[str]
    partition: str
    offset: int
    raw_value: bytes


def _oci_models():
    try:
        import oci
    except ImportError as exc:  # pragma: no cover - only happens before installation
        raise ConfigurationError("Install the OCI SDK: pip install orclstream") from exc
    return oci, oci.streaming.models


def _make_client(config: StreamConfig, *, instance_principal: bool = False):
    oci, _ = _oci_models()
    if instance_principal:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        return oci.streaming.StreamClient({}, config.endpoint, signer=signer, timeout=config.timeout)
    credentials = oci.config.from_file(config.config_file, config.profile)
    return oci.streaming.StreamClient(credentials, config.endpoint, timeout=config.timeout)


class Producer:
    """Publishes JSON, strings, or bytes to one OCI stream.

    Construct with :meth:`from_config` for OCI config-file authentication or
    :meth:`from_instance_principal` for OCI Compute/Functions workloads.
    """

    def __init__(self, client: Any, stream_id: str, *, models: Any = None) -> None:
        if not stream_id.startswith("ocid1."):
            raise ConfigurationError("stream_id must be an OCI stream OCID")
        self._client = client
        self._stream_id = stream_id
        self._models = models or _oci_models()[1]

    @classmethod
    def from_config(cls, config: StreamConfig) -> "Producer":
        return cls(_make_client(config), config.stream_id)

    @classmethod
    def from_instance_principal(cls, config: StreamConfig) -> "Producer":
        return cls(_make_client(config, instance_principal=True), config.stream_id)

    def produce(self, value: Any, *, key: Optional[str] = None) -> str:
        """Publish one message and return its OCI offset.

        Keyed messages remain ordered inside the partition OCI selects for that key.
        """
        return self.produce_many([(value, key)])[0]

    def produce_many(self, messages: Iterable[tuple[Any, Optional[str]]]) -> List[str]:
        """Publish a batch and return offsets. OCI requests must stay under 1 MiB."""
        entries = []
        request_bytes = 0
        for value, key in messages:
            body = serialize(value)
            if len(body) > 1024 * 1024:
                raise ValueError("a single OCI Streaming message cannot exceed 1 MiB")
            request_bytes += len(body) + (len(key.encode("utf-8")) if key is not None else 0)
            if request_bytes > 1024 * 1024:
                raise ValueError("an OCI Streaming publish request cannot exceed 1 MiB")
            encoded_key = b64encode(key.encode("utf-8")).decode("ascii") if key is not None else None
            entries.append(self._models.PutMessagesDetailsEntry(
                key=encoded_key,
                value=b64encode(body).decode("ascii"),
            ))
        if not entries:
            return []
        response = self._client.put_messages(
            self._stream_id,
            self._models.PutMessagesDetails(messages=entries),
        )
        failures = [entry for entry in response.data.entries if getattr(entry, "error", None)]
        if failures:
            raise PartialPublishError(failures)
        return [str(entry.offset) for entry in response.data.entries]


class Consumer:
    """At-least-once OCI Streaming consumer backed by an OCI consumer group.

    Run multiple processes with the same ``group`` and unique ``instance`` values
    to let OCI rebalance partitions between them. Messages are committed only
    after the handler finishes successfully.
    """

    def __init__(
        self,
        client: Any,
        stream_id: str,
        group: str,
        instance: Optional[str] = None,
        *,
        models: Any = None,
        batch_size: int = 100,
        empty_poll_seconds: float = 1.0,
    ) -> None:
        if not stream_id.startswith("ocid1."):
            raise ConfigurationError("stream_id must be an OCI stream OCID")
        if not group:
            raise ConfigurationError("group must not be empty")
        if not 1 <= batch_size <= 10_000:
            raise ValueError("batch_size must be between 1 and 10000")
        self._client, self._stream_id, self._group = client, stream_id, group
        self._instance = instance or f"orclstream-{uuid4()}"
        self._models = models or _oci_models()[1]
        self._batch_size, self._empty_poll_seconds = batch_size, empty_poll_seconds
        self._stop = StopEvent()

    @classmethod
    def from_config(cls, config: StreamConfig, group: str, **kwargs: Any) -> "Consumer":
        return cls(_make_client(config), config.stream_id, group, **kwargs)

    @classmethod
    def from_instance_principal(cls, config: StreamConfig, group: str, **kwargs: Any) -> "Consumer":
        return cls(_make_client(config, instance_principal=True), config.stream_id, group, **kwargs)

    def stop(self) -> None:
        """Request a graceful stop after the current poll finishes."""
        self._stop.set()

    def run(self, handler: Callable[[Message], None]) -> None:
        """Poll until :meth:`stop` is called; errors from ``handler`` are re-raised."""
        self._stop.clear()
        cursor = self._create_group_cursor()
        while not self._stop.is_set():
            cursor, received = self._poll(cursor, handler)
            if not received:
                self._stop.wait(self._empty_poll_seconds)

    def poll_once(self, handler: Callable[[Message], None]) -> int:
        """Process at most one OCI batch; convenient for job-style consumers."""
        cursor = self._create_group_cursor()
        _, received = self._poll(cursor, handler)
        return received

    def _create_group_cursor(self) -> str:
        details = self._models.CreateGroupCursorDetails(
            group_name=self._group,
            instance_name=self._instance,
            type="TRIM_HORIZON",
            commit_on_get=False,
        )
        return self._client.create_group_cursor(self._stream_id, details).data.value

    def _poll(self, cursor: str, handler: Callable[[Message], None]) -> tuple[str, int]:
        response: _Response = self._client.get_messages(self._stream_id, cursor, limit=self._batch_size)
        messages: Sequence[Any] = response.data
        next_cursor = response.headers["opc-next-cursor"]
        if not messages:
            return next_cursor, 0
        for item in messages:
            raw_value = b64decode(item.value)
            key = b64decode(item.key).decode("utf-8") if item.key else None
            message = Message(
                value=deserialize(raw_value), key=key, partition=str(item.partition),
                offset=int(item.offset), raw_value=raw_value,
            )
            try:
                handler(message)
            except Exception as exc:
                raise HandlerError(
                    "handler failed; offsets were not committed and the batch will be delivered again"
                ) from exc
        self._client.consumer_commit(self._stream_id, next_cursor)
        return next_cursor, len(messages)
