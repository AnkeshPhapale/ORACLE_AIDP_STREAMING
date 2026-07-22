"""Library-specific errors."""


class OrclStreamError(Exception):
    """Base class for all orclstream errors."""


class ConfigurationError(OrclStreamError):
    """Raised when required OCI Streaming configuration is missing or invalid."""


class PartialPublishError(OrclStreamError):
    """Raised if OCI accepted only part of a published batch."""

    def __init__(self, failures):
        self.failures = tuple(failures)
        super().__init__(f"OCI rejected {len(self.failures)} message(s) in the batch")


class HandlerError(OrclStreamError):
    """Wraps an application handler error without advancing the group offset."""

