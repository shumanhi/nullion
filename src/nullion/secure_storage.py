"""Local encryption-key storage for Nullion data.

The encrypted databases use Fernet keys. On macOS, users can choose to keep the
data key in Keychain; otherwise Nullion keeps the same 0600 local key file used
by earlier releases.
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

from cryptography.fernet import Fernet

log = logging.getLogger(__name__)

DEFAULT_KEY_STORAGE = "local"
KEY_STORAGE_ENV = "NULLION_KEY_STORAGE"
KEYCHAIN_SERVICE = "Nullion Local Data Key"
KEYCHAIN_ACCOUNT = "chat_history"


class KeyStorageError(RuntimeError):
    """Raised when a configured key storage backend cannot provide a key."""


def configured_key_storage() -> str:
    raw = os.environ.get(KEY_STORAGE_ENV, DEFAULT_KEY_STORAGE)
    value = str(raw or DEFAULT_KEY_STORAGE).strip().lower()
    if value in {"keychain", "macos-keychain", "macos_keychain"}:
        return "keychain"
    if value in {"local", "file"}:
        return "local"
    log.warning("Unknown %s=%r; using local key storage.", KEY_STORAGE_ENV, raw)
    return "local"


def load_or_create_fernet_key(
    local_key_path: Path,
    *,
    storage: str | None = None,
    keychain_service: str = KEYCHAIN_SERVICE,
    keychain_account: str = KEYCHAIN_ACCOUNT,
) -> bytes:
    """Return a valid Fernet key from the selected storage backend."""
    selected = (storage or configured_key_storage()).strip().lower()
    if selected == "keychain":
        return _load_or_create_keychain_key(
            local_key_path,
            service=keychain_service,
            account=keychain_account,
        )
    return _load_or_create_local_key(local_key_path)


def initialize_key_storage(
    *,
    storage: str | None = None,
    local_key_path: Path | None = None,
) -> str:
    """Create or migrate the configured key and return the backend actually used."""
    path = local_key_path or (Path.home() / ".nullion" / "chat_history.key")
    selected = (storage or configured_key_storage()).strip().lower()
    load_or_create_fernet_key(path, storage=selected)
    return selected


def _validate_key(key: bytes) -> bytes:
    key = key.strip()
    Fernet(key)
    return key


def _load_or_create_local_key(path: Path) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        os.chmod(path, 0o600)
        return _validate_key(path.read_bytes())
    key = Fernet.generate_key()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as fh:
        fh.write(key + b"\n")
    return key


def _load_or_create_keychain_key(path: Path, *, service: str, account: str) -> bytes:
    if sys.platform != "darwin":
        log.warning("Keychain storage requested on a non-macOS platform; using local key storage.")
        return _load_or_create_local_key(path)
    if not shutil.which("security"):
        log.warning("macOS security command not found; using local key storage.")
        return _load_or_create_local_key(path)

    existing = _keychain_get(service=service, account=account)
    if existing:
        return _validate_key(existing.encode("ascii"))

    if path.exists():
        key = _load_or_create_local_key(path)
        _keychain_set(service=service, account=account, value=key.decode("ascii"))
        try:
            path.unlink()
            log.info("Migrated local chat-history key into macOS Keychain.")
        except OSError:
            log.warning("Migrated key into Keychain but could not remove local key file: %s", path)
        return key

    key = Fernet.generate_key()
    _keychain_set(service=service, account=account, value=key.decode("ascii"))
    return key


def _keychain_get(*, service: str, account: str) -> str | None:
    result = subprocess.run(
        [
            "security",
            "find-generic-password",
            "-s",
            service,
            "-a",
            account,
            "-w",
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        return result.stdout.strip()
    return None


def _keychain_set(*, service: str, account: str, value: str) -> None:
    result = subprocess.run(
        [
            "security",
            "add-generic-password",
            "-U",
            "-s",
            service,
            "-a",
            account,
            "-w",
            value,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "unknown Keychain error").strip()
        raise KeyStorageError(f"Could not store Nullion data key in macOS Keychain: {detail}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m nullion.secure_storage")
    parser.add_argument("--init", action="store_true", help="initialize configured key storage")
    parser.add_argument("--storage", choices=("local", "keychain"), default=None)
    args = parser.parse_args(argv)
    if args.init:
        used = initialize_key_storage(storage=args.storage)
        print(used)
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
