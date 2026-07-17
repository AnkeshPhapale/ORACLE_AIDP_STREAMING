from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    stream_endpoint: str; stream_ocid: str; config_file: str; profile: str
    database: str; consumer_group: str; late_arrival_hours: int

    @classmethod
    def from_env(cls):
        required = ["OCI_STREAM_ENDPOINT", "OCI_STREAM_OCID", "OCI_CONFIG_FILE"]
        missing = [key for key in required if not os.getenv(key)]
        if missing: raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        return cls(os.environ["OCI_STREAM_ENDPOINT"], os.environ["OCI_STREAM_OCID"], os.environ["OCI_CONFIG_FILE"],
                   os.getenv("OCI_PROFILE", "DEFAULT"), os.getenv("SMART_METER_DATABASE", "smart_meter_lakehouse"),
                   os.getenv("CONSUMER_GROUP", "meter_bronze_consumer_v1"), int(os.getenv("LATE_ARRIVAL_HOURS", "48")))
