"""Dependency-card helpers for Builder capability installs.

This module keeps the install/card logic separate from the main builder decision
code so the web app and runtime can reuse it without duplicating package checks
or proposal formatting.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from importlib import metadata, util
from pathlib import Path
from subprocess import CompletedProcess, run
from typing import Any, Iterable, Mapping
from sys import executable

from nullion.audit import make_audit_record
from nullion.builder import BuilderDecisionType, BuilderProposal, BuilderProposalRecord
from nullion.capability_dependencies import (
    CapabilityDependency,
    capability_dependency_status as _catalog_capability_dependency_status,
    dependency_for_package as _catalog_dependency_for_package,
    get_capability_dependency,
    install_capability_dependency as _catalog_install_capability_dependency,
    install_dependency as _catalog_install_dependency,
    list_capability_dependencies,
    normalize_custom_dependency,
    uninstall_capability_dependency as _catalog_uninstall_capability_dependency,
    uninstall_dependency as _catalog_uninstall_dependency,
)
from nullion.events import make_event
from nullion.tools import ToolResult


_DEPENDENCY_TAG_PREFIX = "dependency:"
_CUSTOM_DEPENDENCY_TAG = "custom_dependency"


@dataclass(frozen=True, slots=True)
class DependencySpec:
    dependency_id: str
    package: str
    import_name: str
    summary: str
    usage_note: str
    docs_url: str | None = None
    github_url: str | None = None
    license: str | None = None
    git_ref: str | None = None
    requirement: str | None = None

    @property
    def install_requirement(self) -> str:
        if self.requirement:
            return self.requirement
        if self.github_url:
            if self.git_ref:
                return f"git+{self.github_url}@{self.git_ref}"
            return f"git+{self.github_url}"
        return self.package

    @property
    def install_command(self) -> tuple[str, ...]:
        return (executable, "-m", "pip", "install", self.install_requirement)


@dataclass(frozen=True, slots=True)
class DependencyStatus:
    dependency_id: str
    package: str
    import_name: str
    installed: bool
    installed_version: str | None
    summary: str
    usage_note: str
    docs_url: str | None = None
    github_url: str | None = None
    license: str | None = None
    install_requirement: str | None = None


_CAPABILITY_DEPENDENCIES: dict[str, DependencySpec] = {
    "pandas": DependencySpec(
        dependency_id="pandas",
        package="pandas",
        import_name="pandas",
        summary="DataFrame analysis, CSV/Excel transforms, and structured tabular workflows.",
        usage_note="Use this for local spreadsheet-style processing, joins, filters, and data cleanup.",
        docs_url="https://pandas.pydata.org/docs/",
        github_url="https://github.com/pandas-dev/pandas",
        license="BSD-3-Clause",
    ),
    "openpyxl": DependencySpec(
        dependency_id="openpyxl",
        package="openpyxl",
        import_name="openpyxl",
        summary="Create and edit Excel workbooks locally.",
        usage_note="Use this when the task needs XLSX sheet updates, formatting, or formulas without a remote service.",
        docs_url="https://openpyxl.readthedocs.io/",
        github_url="https://foss.heptapod.net/openpyxl/openpyxl",
        license="MIT",
    ),
    "soundfile": DependencySpec(
        dependency_id="soundfile",
        package="soundfile",
        import_name="soundfile",
        summary="Read and write audio files locally for media and transcription workflows.",
        usage_note="Use this when the task needs waveform inspection, audio conversion, or sample export on the shell.",
        docs_url="https://pysoundfile.readthedocs.io/",
        github_url="https://github.com/bastibe/python-soundfile",
        license="BSD-3-Clause",
    ),
    "pillow": DependencySpec(
        dependency_id="pillow",
        package="Pillow",
        import_name="PIL",
        summary="Image loading, resizing, and local pixel transforms.",
        usage_note="Use this when the task needs screenshots, thumbnails, or simple image edits without an external tool.",
        docs_url="https://pillow.readthedocs.io/",
        github_url="https://github.com/python-pillow/Pillow",
        license="HPND",
    ),
    "python-docx": DependencySpec(
        dependency_id="python-docx",
        package="python-docx",
        import_name="docx",
        summary="Create and edit Word documents locally.",
        usage_note="Use this when the task needs DOCX generation, template filling, or document round-tripping on the shell.",
        docs_url="https://python-docx.readthedocs.io/",
        github_url="https://github.com/python-openxml/python-docx",
        license="MIT",
    ),
}


def dependency_catalog() -> dict[str, DependencySpec]:
    return dict(_CAPABILITY_DEPENDENCIES)


def resolve_dependency_spec(
    dependency_id: str,
    *,
    package: str | None = None,
    import_name: str | None = None,
    summary: str | None = None,
    usage_note: str | None = None,
    docs_url: str | None = None,
    github_url: str | None = None,
    license: str | None = None,
    git_ref: str | None = None,
    requirement: str | None = None,
) -> DependencySpec:
    normalized_id = str(dependency_id).strip().lower()
    if not normalized_id:
        raise ValueError("dependency_id is required")
    if normalized_id in _CAPABILITY_DEPENDENCIES:
        base = _CAPABILITY_DEPENDENCIES[normalized_id]
        return DependencySpec(
            dependency_id=base.dependency_id,
            package=package or base.package,
            import_name=import_name or base.import_name,
            summary=summary or base.summary,
            usage_note=usage_note or base.usage_note,
            docs_url=docs_url or base.docs_url,
            github_url=github_url or base.github_url,
            license=license or base.license,
            git_ref=git_ref or base.git_ref,
            requirement=requirement or base.requirement,
        )
    if not package:
        package = normalized_id
    if not import_name:
        import_name = package.replace("-", "_")
    return DependencySpec(
        dependency_id=normalized_id,
        package=package,
        import_name=import_name,
        summary=summary or f"Install {package} so Builder can use it for the requested local task.",
        usage_note=usage_note or "Use this dependency when the shell can complete the task but the Python library is missing.",
        docs_url=docs_url,
        github_url=github_url,
        license=license,
        git_ref=git_ref,
        requirement=requirement,
    )


def _installed_version_for_import(import_name: str, package: str) -> str | None:
    try:
        spec = util.find_spec(import_name)
    except Exception:
        spec = None
    if spec is None:
        return None
    try:
        return metadata.version(package)
    except Exception:
        try:
            return metadata.version(import_name)
        except Exception:
            return None


def _spec_from_capability_dependency(dependency: CapabilityDependency) -> DependencySpec:
    return DependencySpec(
        dependency_id=dependency.dependency_id,
        package=dependency.package,
        import_name=dependency.import_name,
        summary=dependency.summary,
        usage_note=dependency.summary,
        docs_url=dependency.docs_url,
        github_url=dependency.source_url,
        license=dependency.license,
        requirement=dependency.requirement,
    )


def capability_dependency_status(dependency: str | DependencySpec | CapabilityDependency, /) -> DependencyStatus:
    if isinstance(dependency, CapabilityDependency):
        status = _catalog_capability_dependency_status(dependency)
        return DependencyStatus(
            dependency_id=dependency.dependency_id,
            package=dependency.package,
            import_name=dependency.import_name,
            installed=status.installed,
            installed_version=status.installed_version,
            summary=dependency.summary,
            usage_note=dependency.summary,
            docs_url=dependency.docs_url,
            github_url=dependency.source_url,
            license=dependency.license,
            install_requirement=dependency.requirement,
        )
    catalog_dependency = get_capability_dependency(dependency) if isinstance(dependency, str) else None
    if catalog_dependency is not None:
        return capability_dependency_status(catalog_dependency)
    spec = resolve_dependency_spec(dependency) if isinstance(dependency, str) else dependency
    installed_version = _installed_version_for_import(spec.import_name, spec.package)
    return DependencyStatus(
        dependency_id=spec.dependency_id,
        package=spec.package,
        import_name=spec.import_name,
        installed=installed_version is not None,
        installed_version=installed_version,
        summary=spec.summary,
        usage_note=spec.usage_note,
        docs_url=spec.docs_url,
        github_url=spec.github_url,
        license=spec.license,
        install_requirement=spec.install_requirement,
    )


def install_dependency(
    dependency: DependencySpec | CapabilityDependency | Mapping[str, Any] | str,
    /,
    *,
    upgrade: bool = False,
) -> dict[str, Any]:
    if isinstance(dependency, CapabilityDependency):
        result = _catalog_install_dependency(dependency)
        result["ok"] = bool(result.get("returncode") == 0)
        return dict(result)
    if isinstance(dependency, DependencySpec):
        spec = dependency
    elif isinstance(dependency, str):
        spec = resolve_dependency_spec(dependency)
    else:
        payload = dict(dependency)
        dependency_id = str(payload.get("dependency_id") or payload.get("package") or "").strip()
        spec = resolve_dependency_spec(
            dependency_id,
            package=payload.get("package"),
            import_name=payload.get("import_name"),
            summary=payload.get("summary"),
            usage_note=payload.get("usage_note"),
            docs_url=payload.get("docs_url"),
            github_url=payload.get("github_url"),
            license=payload.get("license"),
            git_ref=payload.get("git_ref"),
            requirement=payload.get("requirement"),
        )
    command = list(spec.install_command)
    if upgrade:
        command.append("--upgrade")
    completed = run(command, capture_output=True, text=True)
    status = capability_dependency_status(spec)
    return {
        "ok": completed.returncode == 0,
        "dependency_id": spec.dependency_id,
        "package": spec.package,
        "import_name": spec.import_name,
        "installed": status.installed,
        "installed_version": status.installed_version,
        "requirement": spec.install_requirement,
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def install_capability_dependency(dependency_id: str, /) -> dict[str, object]:
    result = _catalog_install_capability_dependency(dependency_id)
    result["ok"] = bool(result.get("returncode") == 0)
    return dict(result)


def disabled_capability_dependency_ids() -> set[str]:
    ids: set[str] = set()
    for raw in os.environ.get("NULLION_DISABLED_CAPABILITY_DEPENDENCIES", "").split(","):
        value = raw.strip().lower()
        if not value:
            continue
        dependency = get_capability_dependency(value)
        if dependency is not None:
            ids.add(_normalize_dependency_key(dependency.dependency_id))
            ids.add(_normalize_dependency_key(dependency.package))
        else:
            ids.add(_normalize_dependency_key(value))
    return ids


def capability_dependency_disabled(dependency: str | DependencySpec | CapabilityDependency, /) -> bool:
    disabled = disabled_capability_dependency_ids()
    if not disabled:
        return False
    if isinstance(dependency, CapabilityDependency):
        return (
            _normalize_dependency_key(dependency.dependency_id) in disabled
            or _normalize_dependency_key(dependency.package) in disabled
        )
    spec = resolve_dependency_spec(dependency) if isinstance(dependency, str) else dependency
    return (
        _normalize_dependency_key(spec.dependency_id) in disabled
        or _normalize_dependency_key(spec.package) in disabled
    )


def uninstall_dependency(
    dependency: DependencySpec | CapabilityDependency | Mapping[str, Any] | str,
    /,
) -> dict[str, Any]:
    if isinstance(dependency, CapabilityDependency):
        result = _catalog_uninstall_dependency(dependency)
        result["ok"] = bool(result.get("returncode") == 0 and not result.get("installed"))
        return dict(result)
    if isinstance(dependency, DependencySpec):
        spec = dependency
    elif isinstance(dependency, str):
        catalog_dependency = get_capability_dependency(dependency)
        if catalog_dependency is not None:
            return uninstall_dependency(catalog_dependency)
        spec = resolve_dependency_spec(dependency)
    else:
        payload = dict(dependency)
        dependency_id = str(payload.get("dependency_id") or payload.get("package") or "").strip()
        spec = resolve_dependency_spec(
            dependency_id,
            package=payload.get("package"),
            import_name=payload.get("import_name"),
            summary=payload.get("summary"),
            usage_note=payload.get("usage_note"),
            docs_url=payload.get("docs_url"),
            github_url=payload.get("github_url"),
            license=payload.get("license"),
            git_ref=payload.get("git_ref"),
            requirement=payload.get("requirement"),
        )
    command = [executable, "-m", "pip", "uninstall", "-y", spec.package]
    completed = run(command, capture_output=True, text=True)
    status = capability_dependency_status(spec)
    return {
        "ok": completed.returncode == 0 and not status.installed,
        "dependency_id": spec.dependency_id,
        "package": spec.package,
        "import_name": spec.import_name,
        "installed": status.installed,
        "installed_version": status.installed_version,
        "requirement": spec.install_requirement,
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def uninstall_capability_dependency(dependency_id: str, /) -> dict[str, object]:
    result = _catalog_uninstall_capability_dependency(dependency_id)
    result["ok"] = bool(result.get("returncode") == 0 and not result.get("installed"))
    return dict(result)


def build_dependency_proposal(
    dependency: DependencySpec | CapabilityDependency | str,
    *,
    title: str | None = None,
    summary: str | None = None,
    confidence: float = 0.8,
) -> BuilderProposal:
    if isinstance(dependency, CapabilityDependency):
        spec = _spec_from_capability_dependency(dependency)
    elif isinstance(dependency, str):
        catalog_dependency = get_capability_dependency(dependency)
        spec = _spec_from_capability_dependency(catalog_dependency) if catalog_dependency is not None else resolve_dependency_spec(dependency)
    else:
        spec = dependency
    status = capability_dependency_status(spec)
    install_line = f"Install {spec.install_requirement} so Builder can use it locally."
    title = title or (f"Review & install {spec.package}" if not status.installed else f"Use installed {spec.package}")
    summary = summary or (
        f"{spec.summary} {install_line} Nothing is installed until you review and approve it."
        if not status.installed
        else f"{spec.summary} Builder can use the installed package immediately after you approve the card."
    )
    tags = tuple(
        dict.fromkeys(
            (
                "dependency",
                f"{_DEPENDENCY_TAG_PREFIX}{spec.dependency_id}",
                f"package:{spec.package}",
                f"import:{spec.import_name}",
            )
        )
    )
    steps = (
        "Review the package details and the usage note.",
        "Install the library with pip if the task really needs it.",
        "Re-run the task locally and verify the artifact.",
    )
    return BuilderProposal(
        decision_type=BuilderDecisionType.DEPENDENCY_PROPOSAL,
        title=title,
        summary=summary,
        confidence=confidence,
        approval_mode="dependency",
        suggested_steps=steps,
        suggested_tags=tags,
        dependency_id=spec.dependency_id,
        dependency_package=spec.package,
        dependency_import_name=spec.import_name,
        dependency_requirement=spec.install_requirement,
        dependency_install_command=spec.install_command,
        dependency_docs_url=spec.docs_url,
        dependency_github_url=spec.github_url,
        dependency_license=spec.license,
        dependency_usage_note=spec.usage_note,
    )


def _normalize_dependency_key(value: object) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _dependency_for_package(package: str) -> DependencySpec | CapabilityDependency | None:
    catalog_dependency = _catalog_dependency_for_package(package)
    if catalog_dependency is not None:
        return catalog_dependency
    normalized = _normalize_dependency_key(package)
    if not normalized:
        return None
    for dependency in _CAPABILITY_DEPENDENCIES.values():
        keys = {
            _normalize_dependency_key(dependency.dependency_id),
            _normalize_dependency_key(dependency.package),
            _normalize_dependency_key(dependency.import_name),
        }
        if normalized in keys:
            return dependency
    return None


def _missing_dependency_payload(result: ToolResult) -> dict[str, object] | None:
    if result.status == "completed":
        return None
    output = result.output if isinstance(result.output, dict) else {}
    if output.get("reason") != "missing_dependency":
        return None
    package = str(output.get("package") or output.get("dependency") or "").strip()
    if not package:
        return None
    return output


def _proposal_for_dependency(dependency: DependencySpec | CapabilityDependency, *, tool_name: str) -> BuilderProposal:
    spec = _spec_from_capability_dependency(dependency) if isinstance(dependency, CapabilityDependency) else dependency
    return build_dependency_proposal(
        spec,
        title=f"Install {spec.package} for local deliverables",
        summary=(
            f"{spec.summary} It supports tool `{tool_name}` when local Python execution needs "
            f"`import {spec.import_name}`. Nothing is installed until you review and approve it."
        ),
        confidence=1.0,
    )


def propose_missing_dependency(
    runtime: Any,
    result: ToolResult,
    *,
    actor: str = "builder_reflector",
) -> BuilderProposalRecord | None:
    payload = _missing_dependency_payload(result)
    if payload is None:
        return None
    dependency = _dependency_for_package(str(payload.get("package") or payload.get("dependency") or ""))
    if dependency is None:
        return None
    if capability_dependency_status(dependency).installed:
        return None
    proposal = _proposal_for_dependency(dependency, tool_name=result.tool_name)
    proposal_id = f"dependency-{dependency.dependency_id}"
    existing = runtime.get_builder_proposal(proposal_id)
    if existing is not None and existing.status == "pending":
        return existing
    return runtime.store_builder_proposal(
        proposal,
        proposal_id=proposal_id,
        context_key=proposal_id,
        actor=actor,
    )


def propose_missing_dependencies_from_tool_results(
    runtime: Any,
    tool_results: Iterable[ToolResult] | None,
    *,
    actor: str = "builder_reflector",
) -> list[BuilderProposalRecord]:
    records: list[BuilderProposalRecord] = []
    for result in tool_results or ():
        record = propose_missing_dependency(runtime, result, actor=actor)
        if record is not None:
            records.append(record)
    return records


def dependency_id_from_proposal(proposal: BuilderProposal) -> str | None:
    if proposal.dependency_id:
        return proposal.dependency_id
    for value in (*proposal.suggested_tags, *proposal.suggested_steps):
        if isinstance(value, str) and value.startswith(_DEPENDENCY_TAG_PREFIX):
            dependency_id = value[len(_DEPENDENCY_TAG_PREFIX):].strip()
            if dependency_id:
                return dependency_id
    return None


def _proposal_value(proposal: BuilderProposal, prefix: str) -> str | None:
    for value in (*proposal.suggested_tags, *proposal.suggested_steps):
        if isinstance(value, str) and value.startswith(prefix):
            found = value[len(prefix):].strip()
            if found:
                return found
    return None


def _custom_dependency_from_proposal(proposal: BuilderProposal) -> DependencySpec | None:
    values = (*proposal.suggested_tags, *proposal.suggested_steps)
    is_custom = _CUSTOM_DEPENDENCY_TAG in values
    package = proposal.dependency_package or _proposal_value(proposal, "package:")
    if not is_custom and not package:
        return None
    if not package:
        return None
    return resolve_dependency_spec(
        proposal.dependency_id or package,
        package=package,
        import_name=proposal.dependency_import_name or _proposal_value(proposal, "import:"),
        summary=proposal.summary,
        usage_note=proposal.dependency_usage_note or proposal.summary,
        docs_url=proposal.dependency_docs_url or _proposal_value(proposal, "docs:"),
        github_url=proposal.dependency_github_url or _proposal_value(proposal, "github:") or _proposal_value(proposal, "source:"),
        license=proposal.dependency_license or _proposal_value(proposal, "license:"),
        git_ref=_proposal_value(proposal, "git_ref:"),
        requirement=proposal.dependency_requirement or _proposal_value(proposal, "requirement:"),
    )


def accept_dependency_builder_proposal(
    runtime: Any,
    proposal_id: str,
    *,
    actor: str = "operator",
) -> dict[str, object]:
    record = runtime.get_builder_proposal(proposal_id)
    if record is None:
        raise KeyError(proposal_id)
    if record.status != "pending":
        raise ValueError(f"proposal {proposal_id} is {record.status}; only pending proposals can be accepted")
    if record.proposal.approval_mode != "dependency":
        raise ValueError(f"proposal {proposal_id} is a {record.proposal.approval_mode} proposal")
    dependency_id = dependency_id_from_proposal(record.proposal)
    catalog_dependency = get_capability_dependency(dependency_id or "")
    if catalog_dependency is not None:
        dependency: DependencySpec | CapabilityDependency = catalog_dependency
        result = install_capability_dependency(catalog_dependency.dependency_id)
    else:
        dependency = _custom_dependency_from_proposal(record.proposal)
        if dependency is None:
            dependency = resolve_dependency_spec(dependency_id or record.proposal.dependency_package or record.proposal.title)
        result = install_dependency(dependency)
    if not result.get("installed"):
        raise RuntimeError(str(result.get("stderr") or result.get("stdout") or "dependency install failed"))
    updated = replace(
        record,
        status="accepted",
        accepted_skill_id=None,
        resolved_at=datetime.now(UTC),
        result=result,
    )
    runtime.store.builder_proposals[proposal_id] = updated
    checkpoint = getattr(runtime, "checkpoint", None)
    if callable(checkpoint):
        checkpoint()
    payload = {
        "proposal_id": proposal_id,
        "decision_type": record.proposal.decision_type.value,
        "status": "accepted",
        "dependency_id": dependency.dependency_id,
        "package": dependency.package,
        "installed_version": result.get("installed_version"),
    }
    runtime.store.add_event(make_event(event_type="builder.proposal_accepted", actor=actor, payload=payload))
    runtime.store.add_audit_record(make_audit_record(action="builder.proposal_accepted", actor=actor, details=payload))
    return result


def build_custom_dependency_spec(payload: Mapping[str, Any]) -> DependencySpec:
    dependency_id = str(payload.get("dependency_id") or payload.get("package") or "").strip().lower()
    if not dependency_id:
        raise ValueError("dependency_id or package is required")
    package = str(payload.get("package") or dependency_id).strip()
    import_name = str(payload.get("import_name") or package.replace("-", "_")).strip()
    summary = str(payload.get("summary") or "").strip() or None
    usage_note = str(payload.get("usage_note") or "").strip() or None
    return resolve_dependency_spec(
        dependency_id,
        package=package,
        import_name=import_name,
        summary=summary,
        usage_note=usage_note,
        docs_url=payload.get("docs_url"),
        github_url=payload.get("github_url"),
        license=payload.get("license"),
        git_ref=payload.get("git_ref"),
        requirement=payload.get("requirement"),
    )


def build_dependency_context_line(spec: DependencySpec, status: DependencyStatus) -> str:
    state = "installed" if status.installed else "not installed"
    version = f" v{status.installed_version}" if status.installed_version else ""
    return f"- {spec.package} (`import {spec.import_name}`) — {spec.summary} [{state}{version}]"


def installed_dependency_context(runtime: Any) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    seen: set[str] = set()

    def append_record(
        *,
        package: str,
        import_name: str,
        requirement: str | None,
        installed_version: object,
        summary: str,
        docs_url: str | None,
        github_url: str | None,
        license: str | None,
    ) -> None:
        records.append(
            {
                "package": package,
                "import_name": import_name,
                "requirement": requirement,
                "installed_version": installed_version,
                "summary": summary,
                "docs_url": docs_url,
                "github_url": github_url,
                "license": license,
            }
        )
        seen.add(_normalize_dependency_key(package))

    store = getattr(runtime, "store", None)
    proposals = getattr(store, "builder_proposals", {}) if store is not None else {}
    if isinstance(proposals, dict):
        for record in proposals.values():
            proposal = getattr(record, "proposal", None)
            status = str(getattr(record, "status", "")).lower()
            if proposal is None or status != "accepted":
                continue
            if str(getattr(proposal, "approval_mode", "")) != "dependency":
                continue
            dependency = _custom_dependency_from_proposal(proposal)
            if dependency is None:
                continue
            key = _normalize_dependency_key(dependency.package)
            if key in seen:
                continue
            result = getattr(record, "result", None)
            dependency_status = capability_dependency_status(dependency)
            if capability_dependency_disabled(dependency) or not dependency_status.installed:
                continue
            append_record(
                package=dependency.package,
                import_name=dependency.import_name,
                requirement=dependency.install_requirement,
                installed_version=(
                    dependency_status.installed_version
                    or (result.get("installed_version") if isinstance(result, dict) else None)
                ),
                summary=dependency.summary,
                docs_url=dependency.docs_url,
                github_url=dependency.github_url,
                license=dependency.license,
            )

    for dependency in (*list_capability_dependencies(), *_CAPABILITY_DEPENDENCIES.values()):
        key = _normalize_dependency_key(dependency.package)
        if key in seen:
            continue
        if capability_dependency_disabled(dependency):
            continue
        status = capability_dependency_status(dependency)
        if not status.installed:
            continue
        requirement = (
            dependency.requirement
            if isinstance(dependency, CapabilityDependency)
            else dependency.install_requirement
        )
        github_url = (
            dependency.source_url
            if isinstance(dependency, CapabilityDependency)
            else dependency.github_url
        )
        append_record(
            package=dependency.package,
            import_name=dependency.import_name,
            requirement=requirement,
            installed_version=status.installed_version,
            summary=dependency.summary,
            docs_url=dependency.docs_url,
            github_url=github_url,
            license=dependency.license,
        )
    return records


def format_installed_dependency_context(runtime: Any) -> str:
    """Return a prompt block describing installed dependency cards."""

    records = installed_dependency_context(runtime)
    if not records:
        return ""
    lines: list[str] = []
    for record in records[:20]:
        package = str(record.get("package") or "dependency")
        import_name = str(record.get("import_name") or package)
        summary = str(record.get("summary") or "")
        installed_version = record.get("installed_version")
        version_text = f" v{installed_version}" if installed_version else ""
        lines.append(f"- {package} (import `{import_name}`){version_text} — {summary}")
    header = [
        "Installed dependency cards:",
        "Use these local Python libraries when the shell can complete the task faster or more reliably than a remote tool.",
    ]
    return "\n".join([*header, *lines])


def create_custom_dependency_builder_proposal(
    runtime: Any,
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
    actor: str = "operator",
) -> BuilderProposalRecord:
    normalized = normalize_custom_dependency(
        package=package,
        requirement=requirement,
        import_name=import_name,
        license=license,
        source_url=source_url,
        docs_url=docs_url,
        summary=summary,
        github_url=github_url,
        git_ref=git_ref,
        subdirectory=subdirectory,
    )
    dependency = resolve_dependency_spec(
        normalized.dependency_id,
        package=normalized.package,
        import_name=normalized.import_name,
        summary=normalized.summary,
        usage_note=normalized.summary,
        docs_url=normalized.docs_url,
        github_url=github_url or normalized.source_url,
        license=normalized.license,
        requirement=normalized.requirement,
    )
    proposal = build_dependency_proposal(
        dependency,
        title=f"Install {dependency.package} for local Python workflows",
        summary=dependency.summary,
        confidence=1.0,
    )
    proposal = replace(
        proposal,
        suggested_steps=tuple(
            dict.fromkeys(
                (
                    *proposal.suggested_steps,
                    _CUSTOM_DEPENDENCY_TAG,
                    f"requirement:{dependency.install_requirement}",
                    f"import:{dependency.import_name}",
                    f"license:{dependency.license or ''}",
                    f"source:{source_url or ''}",
                    f"docs:{dependency.docs_url or ''}",
                    f"github:{github_url or ''}",
                    f"git_ref:{git_ref or ''}",
                    f"subdirectory:{subdirectory or ''}",
                )
            )
        ),
        suggested_tags=tuple(
            dict.fromkeys(
                (
                    *proposal.suggested_tags,
                    _CUSTOM_DEPENDENCY_TAG,
                    f"{_DEPENDENCY_TAG_PREFIX}{dependency.dependency_id}",
                    f"requirement:{dependency.install_requirement}",
                    "custom_python_package",
                )
            )
        ),
    )
    proposal_id = f"dependency-{dependency.dependency_id}"
    return runtime.store_builder_proposal(
        proposal,
        proposal_id=proposal_id,
        context_key=proposal_id,
        actor=actor,
    )


def create_dependency_builder_proposal(
    runtime: Any,
    dependency_id: str,
    *,
    actor: str = "operator",
) -> BuilderProposalRecord:
    normalized_id = str(dependency_id).strip().lower()
    catalog_dependency = get_capability_dependency(normalized_id)
    if catalog_dependency is not None:
        dependency: DependencySpec | CapabilityDependency = catalog_dependency
    elif normalized_id in _CAPABILITY_DEPENDENCIES:
        dependency = resolve_dependency_spec(normalized_id)
    else:
        raise KeyError(dependency_id)
    proposal = _proposal_for_dependency(dependency, tool_name="capability_dependency")
    proposal_id = f"dependency-{dependency.dependency_id}"
    return runtime.store_builder_proposal(
        proposal,
        proposal_id=proposal_id,
        context_key=proposal_id,
        actor=actor,
    )


def dependency_proposal_payload(proposal: BuilderProposal, record_result: Mapping[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "decision_type": proposal.decision_type.value,
        "title": proposal.title,
        "summary": proposal.summary,
        "confidence": proposal.confidence,
        "approval_mode": proposal.approval_mode,
        "suggested_skill_title": proposal.suggested_skill_title,
        "suggested_trigger": proposal.suggested_trigger,
        "suggested_steps": list(proposal.suggested_steps),
        "suggested_tags": list(proposal.suggested_tags),
        "dependency_id": proposal.dependency_id,
        "dependency_package": proposal.dependency_package,
        "dependency_import_name": proposal.dependency_import_name,
        "dependency_requirement": proposal.dependency_requirement,
        "dependency_install_command": list(proposal.dependency_install_command),
        "dependency_docs_url": proposal.dependency_docs_url,
        "dependency_github_url": proposal.dependency_github_url,
        "dependency_license": proposal.dependency_license,
        "dependency_usage_note": proposal.dependency_usage_note,
    }
    if record_result is not None:
        payload["result"] = dict(record_result)
    return payload


__all__ = [
    "DependencySpec",
    "DependencyStatus",
    "accept_dependency_builder_proposal",
    "build_custom_dependency_spec",
    "build_dependency_context_line",
    "build_dependency_proposal",
    "capability_dependency_disabled",
    "capability_dependency_status",
    "create_custom_dependency_builder_proposal",
    "create_dependency_builder_proposal",
    "disabled_capability_dependency_ids",
    "dependency_catalog",
    "dependency_id_from_proposal",
    "dependency_proposal_payload",
    "format_installed_dependency_context",
    "install_capability_dependency",
    "install_dependency",
    "installed_dependency_context",
    "propose_missing_dependencies_from_tool_results",
    "propose_missing_dependency",
    "resolve_dependency_spec",
    "uninstall_capability_dependency",
    "uninstall_dependency",
]
