"""The old updater must accept the new release within its original deadline."""
import importlib.util
import os
from pathlib import Path
import sqlite3
import time

import pytest

pytestmark = pytest.mark.skipif(os.environ.get('NULLION_RUN_E2E') != '1', reason='Docker E2E')


@pytest.fixture
def telegram_env(tmp_path):
    env_file = tmp_path / '.env'
    env_file.write_text('NULLION_TELEGRAM_BOT_TOKEN=123456:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijk\n')
    env_file.chmod(0o600)
    return env_file


def test_release_health_accepts_two_slow_required_bootstraps(tmp_path, monkeypatch, telegram_env):
    from nullion import updater
    source = Path(updater.__file__).parent
    package = tmp_path / 'nullion'
    package.mkdir()
    (package / '__init__.py').write_text(f'__path__.append({str(source)!r})\n')
    # Only external application construction is faked. Both real subprocess
    # boundaries, the installed health implementation and the old parent run.
    marker = tmp_path / 'web'
    (package / 'web_app.py').write_text(
        'import time\nfrom pathlib import Path\nfrom types import SimpleNamespace\n'
        '_STARTUP_WARNINGS = []\ndef _build_runtime(**kwargs):\n'
        f'    time.sleep(24)\n    Path({str(marker)!r}).write_text("checked")\n'
        '    return SimpleNamespace(model_client=True), None, None\n')
    marker2 = tmp_path / 'telegram'
    (package / 'telegram_entrypoint.py').write_text(
        'import time\nfrom pathlib import Path\n'
        'def build_runtime_service_from_settings(**kwargs):\n'
        f'    time.sleep(24)\n    Path({str(marker2)!r}).write_text("checked")\n')
    monkeypatch.setenv('PYTHONPATH', str(tmp_path))
    monkeypatch.setenv('NULLION_SRC_DIR', str(tmp_path))
    fixture = Path(__file__).parent / 'fixtures/updater_v0110_fresh_health.py'
    spec = importlib.util.spec_from_file_location('old_release_probe', fixture)
    parent = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parent)
    start = time.monotonic()
    assert parent.fresh_health_check(require_web=False, env_path=telegram_env, checkpoint_path=tmp_path / 'runtime.db')
    elapsed = time.monotonic() - start
    assert marker.exists() and marker2.exists()
    assert 24 <= elapsed < 42, elapsed


def test_release_health_checks_database_without_loading_saved_history(tmp_path, monkeypatch, telegram_env):
    from nullion import updater
    from nullion.runtime_persistence import save_runtime_store
    from nullion.runtime_store import RuntimeStore

    checkpoint = tmp_path / 'runtime.db'
    save_runtime_store(RuntimeStore(), checkpoint)
    monkeypatch.setenv('NULLION_HOME', str(tmp_path))
    monkeypatch.setenv('NULLION_CHECKPOINT_PATH', str(checkpoint))
    monkeypatch.setenv('NULLION_ENV_FILE', str(telegram_env))
    monkeypatch.setenv('NULLION_SRC_DIR', str(Path(updater.__file__).parents[2]))
    monkeypatch.setenv('PYTHONPATH', os.pathsep.join((str(tmp_path), str(Path(updater.__file__).parents[1]))))
    # Keep the actual builders running. Catch any attempt to hydrate the
    # installed checkpoint inside Web, Telegram or post-update migration.
    (tmp_path / 'sitecustomize.py').write_text(
        'from pathlib import Path\nfrom nullion import runtime\n'
        'bootstrap = runtime.bootstrap_persistent_runtime\n'
        'def guard(path):\n'
        f'    if Path(path) == Path({str(checkpoint)!r}):\n'
        '        raise SystemExit("loaded the installed checkpoint")\n'
        '    return bootstrap(path)\n'
        'runtime.bootstrap_persistent_runtime = guard\n')
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(
            "INSERT INTO runtime_events (collection, item_key, payload, updated_at) VALUES (?, ?, ?, ?)",
            ('events', 'historical-record', '{"data": "' + 'a' * (8 * 1024 * 1024) + '"}', '2026-01-01'),
        )
    assert updater.fresh_post_update_migrations(env_path=telegram_env, checkpoint_path=checkpoint)['warnings'] == []
    assert updater.fresh_health_check(require_web=False, env_path=telegram_env, checkpoint_path=checkpoint)
    index = "idx_conversation_events_type_created"
    with sqlite3.connect(checkpoint) as conn:
        conn.execute(f"DROP INDEX {index}")
        conn.execute("DROP INDEX idx_conversation_events_conversation_created")
        conn.execute("INSERT INTO conversation_events VALUES (?, ?, ?, ?)",
                     ("conversation_events", "invalid-history", "{malformed", "2026-01-01"))
    assert not updater.fresh_health_check(require_web=False, env_path=telegram_env, checkpoint_path=checkpoint)
    with sqlite3.connect(checkpoint) as conn:
        assert conn.execute("SELECT payload FROM conversation_events WHERE item_key='invalid-history'").fetchone() == ("{malformed",)
        assert conn.execute("SELECT 1 FROM sqlite_schema WHERE name=?", (index,)).fetchone() is None
        conn.execute("DELETE FROM conversation_events WHERE item_key='invalid-history'")
    assert updater.fresh_health_check(require_web=False, env_path=telegram_env, checkpoint_path=checkpoint)
    with sqlite3.connect(checkpoint) as conn:
        assert conn.execute("SELECT 1 FROM sqlite_schema WHERE name=?", (index,)).fetchone() is None
        assert conn.execute("SELECT length(payload) FROM runtime_events WHERE item_key='historical-record'").fetchone()[0] > 8 * 1024 * 1024
        (version,) = conn.execute("SELECT value FROM runtime_meta WHERE key='format_version'").fetchone()
        conn.execute("UPDATE runtime_meta SET value='999999' WHERE key='format_version'")
    assert not updater.fresh_health_check(require_web=False, env_path=telegram_env, checkpoint_path=checkpoint)
    with sqlite3.connect(checkpoint) as conn:
        conn.execute("UPDATE runtime_meta SET value=? WHERE key='format_version'", (version,))
        conn.execute("ALTER TABLE runtime_events RENAME COLUMN payload TO incompatible_payload")
    assert not updater.fresh_health_check(require_web=False, env_path=telegram_env, checkpoint_path=checkpoint)
