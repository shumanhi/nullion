from __future__ import annotations

import os
from pathlib import Path

import pytest

from runtime_sqlite_compat_fixture import (
    LEGACY_APPROVAL_ID,
    LEGACY_CONVERSATION_ID,
    LEGACY_FRAME_ID,
    LEGACY_MISSION_ID,
    create_legacy_runtime_sqlite_db,
)


pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_RUN_E2E") != "1",
    reason="set NULLION_RUN_E2E=1 to run runtime sqlite update compatibility e2e tests",
)


def test_seeded_legacy_runtime_sqlite_survives_fresh_update_migration_and_web_boot(tmp_path: Path, monkeypatch) -> None:
    from fastapi.testclient import TestClient
    from nullion import updater
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.runtime_persistence import load_runtime_store
    from nullion.tools import ToolRegistry
    from nullion.web_app import create_app

    install = tmp_path / "install"
    install.mkdir()
    env_path = install / ".env"
    env_path.write_text('NULLION_KEY_STORAGE="local"\n', encoding="utf-8")
    checkpoint = create_legacy_runtime_sqlite_db(install / "runtime.db")
    monkeypatch.setenv("NULLION_INSTALL_DIR", str(install))
    monkeypatch.setenv("NULLION_ENV_FILE", str(env_path))
    monkeypatch.setenv("NULLION_SRC_DIR", os.environ.get("NULLION_APP_REPO", str(install / "src")))

    result = updater.fresh_post_update_migrations(env_path=env_path, checkpoint_path=checkpoint, install_dir=install)

    assert result["warnings"] == []
    assert result["memory_migrated"] is True
    assert result["memory_rows_encrypted"] == 3
    loaded = load_runtime_store(checkpoint)
    assert loaded.get_approval_request(LEGACY_APPROVAL_ID).resource == "https://api.example.test/*"
    assert loaded.get_task_frame(LEGACY_FRAME_ID).summary == "compat summary"
    assert loaded.get_mission(LEGACY_MISSION_ID).goal == "Keep data intact"
    assert loaded.user_facts["mem-fact"].value == "beta"

    runtime = bootstrap_persistent_runtime(checkpoint)
    runtime.store.add_conversation_event({"conversation_id": LEGACY_CONVERSATION_ID, "event_type": "compat.web_boot"})
    runtime.checkpoint()

    app = create_app(runtime, orchestrator=None, registry=ToolRegistry(filesystem_allowed_roots=[tmp_path]))
    with TestClient(app) as client:
        status = client.get("/api/status")

    assert status.status_code == 200
    assert any(event["event_type"] == "compat.web_boot" for event in load_runtime_store(checkpoint).list_conversation_events())
