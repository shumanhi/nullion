from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_e2e_tests_require_container_marker() -> None:
    guard = (ROOT / "tests" / "e2e" / "conftest.py").read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.e2e.yml").read_text(encoding="utf-8")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "NULLION_RUN_E2E" in guard
    assert "NULLION_E2E_CONTAINER" in guard
    assert "Path(\"/.dockerenv\").exists()" in guard
    assert "Refusing to run e2e tests on the host" in guard
    assert 'NULLION_E2E_CONTAINER: "1"' in compose
    assert "E2E tests refuse to run on the host" in readme
