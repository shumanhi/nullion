from __future__ import annotations

from cryptography.fernet import Fernet

from nullion import secure_storage


def test_local_key_storage_creates_0600_key(tmp_path):
    key_path = tmp_path / "chat_history.key"

    key = secure_storage.load_or_create_fernet_key(key_path, storage="local")

    Fernet(key)
    assert key_path.exists()
    assert key_path.stat().st_mode & 0o777 == 0o600
    assert secure_storage.load_or_create_fernet_key(key_path, storage="local") == key


def test_keychain_storage_migrates_local_key_and_removes_file(tmp_path, monkeypatch):
    key_path = tmp_path / "chat_history.key"
    existing_key = Fernet.generate_key()
    key_path.write_bytes(existing_key + b"\n")
    stored: dict[tuple[str, str], str] = {}

    monkeypatch.setattr(secure_storage.sys, "platform", "darwin")
    monkeypatch.setattr(secure_storage.shutil, "which", lambda name: "/usr/bin/security")
    monkeypatch.setattr(
        secure_storage,
        "_keychain_get",
        lambda *, service, account: stored.get((service, account)),
    )

    def fake_set(*, service: str, account: str, value: str) -> None:
        stored[(service, account)] = value

    monkeypatch.setattr(secure_storage, "_keychain_set", fake_set)

    key = secure_storage.load_or_create_fernet_key(key_path, storage="keychain")

    assert key == existing_key
    assert stored[(secure_storage.KEYCHAIN_SERVICE, secure_storage.KEYCHAIN_ACCOUNT)] == existing_key.decode("ascii")
    assert not key_path.exists()


def test_keychain_storage_reads_existing_keychain_value(tmp_path, monkeypatch):
    key_path = tmp_path / "chat_history.key"
    keychain_key = Fernet.generate_key().decode("ascii")

    monkeypatch.setattr(secure_storage.sys, "platform", "darwin")
    monkeypatch.setattr(secure_storage.shutil, "which", lambda name: "/usr/bin/security")
    monkeypatch.setattr(secure_storage, "_keychain_get", lambda *, service, account: keychain_key)

    key = secure_storage.load_or_create_fernet_key(key_path, storage="keychain")

    assert key == keychain_key.encode("ascii")
    assert not key_path.exists()
