from __future__ import annotations

import importlib
import os
import pathlib
import tomllib


ROOT = pathlib.Path(
    os.environ.get(
        "NULLION_APP_REPO",
        pathlib.Path(__file__).resolve().parents[1],
    )
).resolve()


def test_auth_runtime_dependencies_are_declared() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = "\n".join(pyproject["project"]["dependencies"])

    assert "httpx" in dependencies


def test_pdf_runtime_dependencies_are_declared() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = "\n".join(pyproject["project"]["dependencies"])

    assert "pillow" in dependencies
    assert "pypdf" in dependencies


def test_capability_dependency_catalog_is_installed_with_runtime() -> None:
    from nullion.capability_dependencies import list_capability_dependencies

    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = "\n".join(item.lower() for item in pyproject["project"]["dependencies"])

    for dependency in list_capability_dependencies():
        package = dependency.package.lower()
        assert package in dependencies or package.replace("_", "-") in dependencies


def test_installed_entrypoint_modules_import() -> None:
    for module_name in ("nullion.auth", "nullion.secure_storage"):
        importlib.import_module(module_name)
