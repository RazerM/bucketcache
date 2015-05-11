from __future__ import absolute_import, division, print_function

__all__ = (
    'KeyInvalidError',
    'KeyFileNotFoundError',
    'KeyExpirationError',
    'BackendLoadError',
)


class KeyInvalidError(Exception):
    """Raised internally when a key cannot be loaded."""
    pass


class KeyFileNotFoundError(KeyInvalidError):
    """Raised when file for key doesn't exist."""
    pass


class KeyExpirationError(KeyInvalidError):
    """Raised when key has expired."""
    pass


class BackendLoadError(Exception):
    """Raised when Backend.from_file cannot load an object."""
    pass
