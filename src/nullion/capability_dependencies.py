"""Cataloged optional dependencies for Builder-managed capabilities."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import metadata
import re
import subprocess
import sys
from urllib.parse import urlparse, urlunparse
from typing import Iterable


@dataclass(frozen=True, slots=True)
class CapabilityDependency:
    dependency_id: str
    package: str
    import_name: str
    requirement: str
    license: str
    source_url: str
    docs_url: str
    summary: str
    enables: tuple[str, ...]

    @property
    def install_command(self) -> str:
        return f"python -m pip install '{self.requirement}'"


@dataclass(frozen=True, slots=True)
class CapabilityDependencyStatus:
    dependency: CapabilityDependency
    installed: bool
    installed_version: str | None = None


_DEPENDENCY_CATALOG: tuple[CapabilityDependency, ...] = (
    CapabilityDependency(
        dependency_id="pandas",
        package="pandas",
        import_name="pandas",
        requirement="pandas>=2.2,<3",
        license="BSD-3-Clause",
        source_url="https://github.com/pandas-dev/pandas",
        docs_url="https://pandas.pydata.org/docs/",
        summary="DataFrame analysis, CSV/Excel transforms, joins, filters, and structured tabular workflows.",
        enables=("dataframe_analysis", "csv_processing", "tabular_cleanup"),
    ),
    CapabilityDependency(
        dependency_id="openpyxl",
        package="openpyxl",
        import_name="openpyxl",
        requirement="openpyxl>=3.1,<4",
        license="MIT",
        source_url="https://foss.heptapod.net/openpyxl/openpyxl",
        docs_url="https://openpyxl.readthedocs.io/en/stable/",
        summary="Create and edit .xlsx workbooks, including sheets with links and embedded images.",
        enables=("spreadsheet_create", "xlsx_artifacts", "image_embedded_spreadsheets"),
    ),
    CapabilityDependency(
        dependency_id="pillow",
        package="Pillow",
        import_name="PIL",
        requirement="pillow>=10,<12",
        license="HPND",
        source_url="https://github.com/python-pillow/Pillow",
        docs_url="https://pillow.readthedocs.io/",
        summary="Load, inspect, resize, convert, and compose local images for visual deliverables.",
        enables=("image_processing", "thumbnail_generation", "spreadsheet_image_embedding"),
    ),
    CapabilityDependency(
        dependency_id="pypdf",
        package="pypdf",
        import_name="pypdf",
        requirement="pypdf>=6,<7",
        license="BSD-3-Clause",
        source_url="https://github.com/py-pdf/pypdf",
        docs_url="https://pypdf.readthedocs.io/",
        summary="Read, split, merge, and edit PDF files for local document workflows.",
        enables=("pdf_edit", "pdf_artifacts", "document_processing"),
    ),
    CapabilityDependency(
        dependency_id="python-docx",
        package="python-docx",
        import_name="docx",
        requirement="python-docx>=1.1,<2",
        license="MIT",
        source_url="https://github.com/python-openxml/python-docx",
        docs_url="https://python-docx.readthedocs.io/en/latest/",
        summary="Create and edit .docx documents when a document artifact workflow needs local generation.",
        enables=("docx_artifacts", "document_generation"),
    ),
    CapabilityDependency(
        dependency_id="python-pptx",
        package="python-pptx",
        import_name="pptx",
        requirement="python-pptx>=1.0,<2",
        license="MIT",
        source_url="https://github.com/scanny/python-pptx",
        docs_url="https://python-pptx.readthedocs.io/en/latest/",
        summary="Create and edit .pptx slide decks when a presentation artifact workflow needs local generation.",
        enables=("pptx_artifacts", "presentation_generation"),
    ),
    CapabilityDependency(
        dependency_id="soundfile",
        package="soundfile",
        import_name="soundfile",
        requirement="soundfile>=0.12,<1",
        license="BSD-3-Clause",
        source_url="https://github.com/bastibe/python-soundfile",
        docs_url="https://python-soundfile.readthedocs.io/en/latest/",
        summary="Read and write common audio files for local audio deliverable workflows.",
        enables=("audio_artifacts", "audio_file_processing"),
    ),
)

_PYPI_PACKAGE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_REQUIREMENT_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]*(?:\[[A-Za-z0-9_,.-]+\])?(?:[<>=!~]=?[A-Za-z0-9*_.!+,-]+)?(?:,[<>=!~]=?[A-Za-z0-9*_.!+,-]+)*$"
)
_GIT_REF_RE = re.compile(r"^[A-Za-z0-9._/-]{1,120}$")
_GIT_SUBDIRECTORY_RE = re.compile(r"^[A-Za-z0-9._/-]{1,180}$")


def list_capability_dependencies() -> tuple[CapabilityDependency, ...]:
    return _DEPENDENCY_CATALOG


def get_capability_dependency(dependency_id: str) -> CapabilityDependency | None:
    normalized = str(dependency_id or "").strip().lower()
    for dependency in _DEPENDENCY_CATALOG:
        if dependency.dependency_id == normalized or dependency.package.lower() == normalized:
            return dependency
    return None


def dependency_for_package(package: str) -> CapabilityDependency | None:
    normalized = str(package or "").strip().lower()
    if not normalized:
        return None
    for dependency in _DEPENDENCY_CATALOG:
        if dependency.package.lower() == normalized:
            return dependency
    return None


def dependency_for_enabled_capability(capability: str) -> CapabilityDependency | None:
    normalized = str(capability or "").strip()
    if not normalized:
        return None
    for dependency in _DEPENDENCY_CATALOG:
        if normalized in dependency.enables:
            return dependency
    return None


def capability_dependency_status(dependency: CapabilityDependency) -> CapabilityDependencyStatus:
    version: str | None = None
    try:
        version = metadata.version(dependency.package)
    except metadata.PackageNotFoundError:
        return CapabilityDependencyStatus(dependency=dependency, installed=False)
    except Exception:
        return CapabilityDependencyStatus(dependency=dependency, installed=True)
    return CapabilityDependencyStatus(dependency=dependency, installed=True, installed_version=version)


def list_capability_dependency_statuses() -> tuple[CapabilityDependencyStatus, ...]:
    return tuple(capability_dependency_status(dependency) for dependency in _DEPENDENCY_CATALOG)


def dependency_status_payload(status: CapabilityDependencyStatus) -> dict[str, object]:
    dependency = status.dependency
    return {
        "dependency_id": dependency.dependency_id,
        "package": dependency.package,
        "requirement": dependency.requirement,
        "import_name": dependency.import_name,
        "license": dependency.license,
        "source_url": dependency.source_url,
        "docs_url": dependency.docs_url,
        "summary": dependency.summary,
        "enables": list(dependency.enables),
        "install_command": dependency.install_command,
        "installed": status.installed,
        "installed_version": status.installed_version,
    }


def dependency_status_payloads(
    statuses: Iterable[CapabilityDependencyStatus] | None = None,
) -> list[dict[str, object]]:
    return [
        dependency_status_payload(status)
        for status in (statuses if statuses is not None else list_capability_dependency_statuses())
    ]


def normalize_custom_dependency(
    *,
    package: str,
    requirement: str | None = None,
    import_name: str | None = None,
    license: str | None = None,
    source_url: str | None = None,
    docs_url: str | None = None,
    summary: str | None = None,
    github_url: str | None = None,
    git_ref: str | None = None,
    subdirectory: str | None = None,
) -> CapabilityDependency:
    normalized_package = str(package or "").strip()
    normalized_requirement = str(requirement or normalized_package).strip()
    if not _PYPI_PACKAGE_RE.fullmatch(normalized_package):
        raise ValueError("Package must be a PyPI package name, not a path, URL, or shell command.")
    normalized_github_url = str(github_url or "").strip()
    normalized_ref = str(git_ref or "").strip()
    normalized_subdirectory = str(subdirectory or "").strip().strip("/")
    if normalized_github_url:
        normalized_requirement = _github_requirement(
            package=normalized_package,
            github_url=normalized_github_url,
            ref=normalized_ref,
            subdirectory=normalized_subdirectory,
        )
    else:
        if not _REQUIREMENT_RE.fullmatch(normalized_requirement):
            raise ValueError("Requirement must be a PyPI requirement such as package, package>=1, or package>=1,<2.")
        requirement_package = re.split(r"[\[<>=!~,]", normalized_requirement, maxsplit=1)[0].strip()
        if requirement_package.lower().replace("_", "-") != normalized_package.lower().replace("_", "-"):
            raise ValueError("Requirement package must match the package name.")
    normalized_import = str(import_name or normalized_package.replace("-", "_")).strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*", normalized_import):
        raise ValueError("Import name must be a Python module path such as package_name.")
    return CapabilityDependency(
        dependency_id=f"custom-{normalized_package.lower().replace('_', '-')}",
        package=normalized_package,
        import_name=normalized_import,
        requirement=normalized_requirement,
        license=str(license or "operator-reviewed").strip() or "operator-reviewed",
        source_url=normalized_github_url or str(source_url or "").strip(),
        docs_url=str(docs_url or "").strip(),
        summary=str(summary or f"Operator-requested Python package {normalized_package}.").strip(),
        enables=("custom_python_package",),
    )


def _github_requirement(*, package: str, github_url: str, ref: str, subdirectory: str) -> str:
    parsed = urlparse(github_url)
    if parsed.scheme not in {"https", "http", "git+https"}:
        raise ValueError("GitHub installs must use a public https://github.com/... repository URL.")
    if parsed.username or parsed.password or "@" in parsed.netloc:
        raise ValueError("GitHub install URL must not include credentials.")
    host = parsed.netloc.lower()
    if host != "github.com":
        raise ValueError("Only public github.com repository URLs are supported from this UI.")
    path = parsed.path.strip("/")
    parts = path.split("/")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError("GitHub URL must include owner and repository.")
    repo_path = f"/{parts[0]}/{parts[1]}"
    if not repo_path.endswith(".git"):
        repo_path += ".git"
    repo_url = urlunparse(("https", "github.com", repo_path, "", "", ""))
    ref_part = ""
    if ref:
        if not _GIT_REF_RE.fullmatch(ref) or ".." in ref or ref.startswith(("/", "-")):
            raise ValueError("Git ref must be a branch, tag, or commit-like value.")
        ref_part = f"@{ref}"
    fragment = ""
    if subdirectory:
        if not _GIT_SUBDIRECTORY_RE.fullmatch(subdirectory) or ".." in subdirectory or subdirectory.startswith(("/", "-")):
            raise ValueError("Subdirectory must be a relative package path inside the repository.")
        fragment = f"#subdirectory={subdirectory}"
    return f"{package} @ git+{repo_url}{ref_part}{fragment}"


def install_dependency(
    dependency: CapabilityDependency,
    *,
    python_executable: str | None = None,
    timeout_seconds: int = 180,
) -> dict[str, object]:
    executable = python_executable or sys.executable
    command = [executable, "-m", "pip", "install", dependency.requirement]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    status = capability_dependency_status(dependency)
    return {
        "dependency_id": dependency.dependency_id,
        "package": dependency.package,
        "requirement": dependency.requirement,
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout[-4000:],
        "stderr": completed.stderr[-4000:],
        "installed": status.installed,
        "installed_version": status.installed_version,
    }


def uninstall_dependency(
    dependency: CapabilityDependency,
    *,
    python_executable: str | None = None,
    timeout_seconds: int = 180,
) -> dict[str, object]:
    executable = python_executable or sys.executable
    command = [executable, "-m", "pip", "uninstall", "-y", dependency.package]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    status = capability_dependency_status(dependency)
    return {
        "dependency_id": dependency.dependency_id,
        "package": dependency.package,
        "requirement": dependency.requirement,
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout[-4000:],
        "stderr": completed.stderr[-4000:],
        "installed": status.installed,
        "installed_version": status.installed_version,
    }


def install_capability_dependency(
    dependency_id: str,
    *,
    python_executable: str | None = None,
    timeout_seconds: int = 180,
) -> dict[str, object]:
    dependency = get_capability_dependency(dependency_id)
    if dependency is None:
        raise KeyError(dependency_id)
    return install_dependency(
        dependency,
        python_executable=python_executable,
        timeout_seconds=timeout_seconds,
    )


def uninstall_capability_dependency(
    dependency_id: str,
    *,
    python_executable: str | None = None,
    timeout_seconds: int = 180,
) -> dict[str, object]:
    dependency = get_capability_dependency(dependency_id)
    if dependency is None:
        raise KeyError(dependency_id)
    return uninstall_dependency(
        dependency,
        python_executable=python_executable,
        timeout_seconds=timeout_seconds,
    )
