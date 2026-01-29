"""Password hashing utilities."""

import hashlib
import secrets
from pathlib import Path


def get_or_create_secret_key(key_file: Path = Path("data/.key")) -> str:
    """
    Get the secret key from file or create a new one if it doesn't exist.

    Args:
        key_file: Path to the secret key file

    Returns:
        The secret key as a string
    """
    # Ensure data directory exists
    key_file.parent.mkdir(parents=True, exist_ok=True)

    # Try to read existing key
    if key_file.exists():
        with key_file.open("r") as f:
            key = f.read().strip()
            if key:
                return key

    # Generate new cryptographically secure key
    key = secrets.token_urlsafe(32)

    # Save to file
    with key_file.open("w") as f:
        f.write(key)

    # Set restrictive permissions (owner read/write only)
    key_file.chmod(0o600)

    return key


def hash_password(password: str) -> str:
    """
    Hash a password using SHA-256 with a salt.

    Args:
        password: Plain text password

    Returns:
        Hashed password in format: salt$hash
    """
    salt = secrets.token_hex(16)
    pwd_hash = hashlib.sha256((salt + password).encode()).hexdigest()
    return f"{salt}${pwd_hash}"


def verify_password(password: str, hashed: str) -> bool:
    """
    Verify a password against its hash.

    Args:
        password: Plain text password to verify
        hashed: Stored password hash in format: salt$hash

    Returns:
        True if password matches, False otherwise
    """
    try:
        salt, stored_hash = hashed.split("$")
        pwd_hash = hashlib.sha256((salt + password).encode()).hexdigest()
        return pwd_hash == stored_hash
    except (ValueError, AttributeError):
        return False
