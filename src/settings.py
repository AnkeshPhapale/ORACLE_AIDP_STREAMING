import os
import re
from dataclasses import dataclass


_SPARK_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_OCI_PARTITION = re.compile(r"\d+")

@dataclass(frozen=True)
class Settings:
    stream_endpoint: str | None
    stream_ocid: str | None
    config_file: str | None
    profile: str
    catalog: str
    consumer_group: str
    stream_partitions: tuple[str, ...]
    late_arrival_hours: int

    @classmethod
    def from_env(cls, require_stream: bool = False) -> "Settings":
        required = ["OCI_STREAM_ENDPOINT", "OCI_STREAM_OCID", "OCI_CONFIG_FILE"] if require_stream else []
        missing = [key for key in required if not os.getenv(key)]
        if missing: raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        catalog = os.getenv("AIDP_CATALOG", "aidp_poc")
        if not _SPARK_IDENTIFIER.fullmatch(catalog):
            raise ValueError("AIDP_CATALOG must be a simple Spark identifier")
        partitions = tuple(p.strip() for p in os.getenv("OCI_STREAM_PARTITIONS", "0").split(",") if p.strip())
        if require_stream and not partitions:
            raise ValueError("OCI_STREAM_PARTITIONS must contain at least one OCI stream partition")
        if any(not _OCI_PARTITION.fullmatch(partition) for partition in partitions):
            raise ValueError("OCI_STREAM_PARTITIONS must be comma-separated numeric partition IDs")

        consumer_group = os.getenv("CONSUMER_GROUP", "meter_bronze_consumer_v1")
        if not _SPARK_IDENTIFIER.fullmatch(consumer_group):
            raise ValueError("CONSUMER_GROUP must contain only letters, digits, and underscores")

        try:
            late_arrival_hours = int(os.getenv("LATE_ARRIVAL_HOURS", "48"))
        except ValueError as error:
            raise ValueError("LATE_ARRIVAL_HOURS must be an integer") from error
        if late_arrival_hours < 0:
            raise ValueError("LATE_ARRIVAL_HOURS must be zero or greater")

        return cls(
            os.getenv("OCI_STREAM_ENDPOINT"),
            os.getenv("OCI_STREAM_OCID"),
            os.getenv("OCI_CONFIG_FILE"),
            os.getenv("OCI_PROFILE", "DEFAULT"),
            catalog,
            consumer_group,
            partitions,
            late_arrival_hours,
        )

    def table(self, layer: str, name: str) -> str:
        if not _SPARK_IDENTIFIER.fullmatch(layer) or not _SPARK_IDENTIFIER.fullmatch(name):
            raise ValueError("Layer and table name must be simple Spark identifiers")
        return f"{self.catalog}.{layer}.{name}"
