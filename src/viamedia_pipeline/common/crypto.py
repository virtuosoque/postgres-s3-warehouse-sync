"""Symmetric encryption for secrets stored in the metadata DB.

Connection credentials (source DSN, AWS keys) are entered in the UI and stored
in `pipeline_connections`. They are encrypted at rest with Fernet (AES-128-CBC +
HMAC) using a key supplied via the `CONFIG_ENCRYPTION_KEY` bootstrap env var.

Generate a key once with:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

Losing the key means the stored secrets can't be decrypted and must be re-entered.
"""

import os
from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken

_ENC_PREFIX = "enc:v1:"


class CryptoError(RuntimeError):
    pass


@lru_cache(maxsize=1)
def _fernet() -> Fernet:
    key = os.environ.get("CONFIG_ENCRYPTION_KEY", "").strip()
    if not key:
        raise CryptoError(
            "CONFIG_ENCRYPTION_KEY is not set. Generate one with "
            "`python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\"` and set it in the environment."
        )
    try:
        return Fernet(key.encode())
    except (ValueError, TypeError) as e:
        raise CryptoError(f"CONFIG_ENCRYPTION_KEY is not a valid Fernet key: {e}") from e


def encrypt(plaintext: str | None) -> str | None:
    """Encrypt a secret for storage. None/empty passes through unchanged."""
    if not plaintext:
        return plaintext
    token = _fernet().encrypt(plaintext.encode()).decode()
    return _ENC_PREFIX + token


def decrypt(stored: str | None) -> str | None:
    """Decrypt a value produced by `encrypt`. Values without the marker prefix
    are returned as-is (tolerates legacy/plaintext rows)."""
    if not stored or not stored.startswith(_ENC_PREFIX):
        return stored
    token = stored[len(_ENC_PREFIX):]
    try:
        return _fernet().decrypt(token.encode()).decode()
    except InvalidToken as e:
        raise CryptoError(
            "Failed to decrypt a stored secret — CONFIG_ENCRYPTION_KEY likely changed."
        ) from e


def is_encrypted(stored: str | None) -> bool:
    return bool(stored) and stored.startswith(_ENC_PREFIX)
