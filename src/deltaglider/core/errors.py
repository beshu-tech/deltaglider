"""Core domain errors."""


class DeltaGliderError(Exception):
    """Base error for DeltaGlider."""

    pass


class NotFoundError(DeltaGliderError):
    """Object not found."""

    pass


class ReferenceCreationRaceError(DeltaGliderError):
    """Race condition during reference creation."""

    pass


class IntegrityMismatchError(DeltaGliderError):
    """SHA256 mismatch."""

    pass


class DiffEncodeError(DeltaGliderError):
    """Error encoding delta."""

    pass


class DiffDecodeError(DeltaGliderError):
    """Error decoding delta."""

    pass


class StorageIOError(DeltaGliderError):
    """Storage I/O error."""

    pass


class PolicyViolationWarning(Warning):
    """Policy violation warning."""

    pass


class CacheMissError(DeltaGliderError):
    """Cache miss - file not found in cache."""

    pass


class CacheCorruptionError(DeltaGliderError):
    """Cache corruption - SHA mismatch or tampering detected."""

    pass
