"""External data-provider adapters."""


class ProviderError(RuntimeError):
    """Raised when an external provider cannot produce a usable snapshot."""
