from __future__ import absolute_import, division, print_function

__all__ = (
    'KeyInvalidError',
    'KeyFileNotFoundError',
    'KeyExpirationError',
    'BackendLoadError',
)


class KeyInvalidError(Exception):
    """Raised internally when a key cannot be loaded."""


class KeyFileNotFoundError(KeyInvalidError):
    """Raised when file for key doesn't exist."""


class KeyExpirationError(KeyInvalidError):
    """Raised when key has expired."""


class BackendLoadError(Exception):
    """Raised when :py:meth:`bucketcache.backends.Backend.from_file` cannot
    load an object.
    """
