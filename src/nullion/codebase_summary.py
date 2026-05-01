from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


_REPO_ARTIFACT_MARKERS = (
    "readme.md",
    "readme.rst",
    "docs",
    "doc",
    "plans",
    "plan",
    "architecture",
    "design",
)


@dataclass(frozen=True)
class CodebaseSummary:
    package_root: str
    top_level_modules: tuple[str, ...]
    top_level_packages: tuple[str, ...]
    source_file_count: int
    repo_artifacts: tuple[str, ...]


def build_codebase_summary(
    repo_root: str | Path,
    package_root: str = "src/nullion",
) -> CodebaseSummary:
    root = Path(repo_root)
    package_dir = root / package_root

    if package_dir.exists():
        top_level_modules = tuple(
            sorted(
                entry.stem
                for entry in package_dir.glob("*.py")
                if entry.name != "__init__.py"
            )
        )

        top_level_packages = tuple(
            sorted(
                entry.name
                for entry in package_dir.iterdir()
                if entry.is_dir() and (entry / "__init__.py").exists()
            )
        )

        source_file_count = sum(
            1
            for entry in package_dir.rglob("*.py")
            if "__pycache__" not in entry.parts
        )
    else:
        top_level_modules = ()
        top_level_packages = ()
        source_file_count = 0

    repo_artifacts = _collect_repo_artifacts(root)

    return CodebaseSummary(
        package_root=package_root,
        top_level_modules=top_level_modules,
        top_level_packages=top_level_packages,
        source_file_count=source_file_count,
        repo_artifacts=repo_artifacts,
    )


def format_codebase_summary(summary: CodebaseSummary) -> str:
    module_list = ", ".join(summary.top_level_modules) if summary.top_level_modules else "none"
    package_list = ", ".join(summary.top_level_packages) if summary.top_level_packages else "none"
    artifacts = ", ".join(summary.repo_artifacts) if summary.repo_artifacts else "none"

    return (
        "Codebase summary\n"
        f"Package root: {summary.package_root}\n"
        f"Top-level modules ({len(summary.top_level_modules)}): {module_list}\n"
        f"Top-level packages ({len(summary.top_level_packages)}): {package_list}\n"
        f"Source files (.py): {summary.source_file_count}\n"
        f"Repo docs/plans: {artifacts}"
    )


def _collect_repo_artifacts(root: Path) -> tuple[str, ...]:
    if not root.exists():
        return ()

    existing_by_lower = {entry.name.lower(): entry for entry in root.iterdir()}
    artifacts: list[str] = []

    for name in _REPO_ARTIFACT_MARKERS:
        entry = existing_by_lower.get(name)
        if entry is None:
            continue

        artifact_name = f"{entry.name}/" if entry.is_dir() else entry.name
        if artifact_name not in artifacts:
            artifacts.append(artifact_name)

    return tuple(artifacts)
