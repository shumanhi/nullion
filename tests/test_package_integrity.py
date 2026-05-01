from __future__ import annotations

import importlib
import pathlib
import tomllib


ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_auth_runtime_dependencies_are_declared() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = "\n".join(pyproject["project"]["dependencies"])

    assert "httpx" in dependencies


def test_installed_entrypoint_modules_import() -> None:
    for module_name in ("nullion.auth", "nullion.secure_storage"):
        importlib.import_module(module_name)
