"""Install and inspect local skill packs.

Skill packs are reference instructions, not executable packages.  The installer
copies or clones skill folders and indexes `SKILL.md` files; it never runs code
from the pack.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen


logger = logging.getLogger(__name__)
_PACK_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._/-]*$")
_SKILL_PACK_INDEX_CACHE_NAMESPACE = "skill_pack.compact_index"
_SKILL_PACK_INDEX_CACHE_VERSION = "v1"
_SKILL_PACK_INDEX_CACHE_TTL_SECONDS = 24 * 60 * 60
_SUSPICIOUS_PATTERNS = (
    "curl ",
    "wget ",
    "bash ",
    "sh ",
    "powershell",
    "base64",
    "token",
    "secret",
    "private key",
    "credential",
)
_BUILTIN_REFERENCE_ALIASES = {"SKILL.md", "README.md", "README.txt"}

BUILTIN_SKILL_PACK_PROMPTS: dict[str, str] = {
    "nullion/web-research": (
        "Skill pack: nullion/web-research\n"
        "Use for research, fact checking, summarization, and source-backed answers. "
        "Prefer search and fetch tools when current or sourced information matters. "
        "Summarize findings, cite sources when available, and call out uncertainty instead of guessing. "
        "Do not treat web access as account access."
    ),
    "nullion/browser-automation": (
        "Skill pack: nullion/browser-automation\n"
        "Use when the user asks to inspect, navigate, fill forms, test UI, capture screenshots, or verify a web app. "
        "Prefer visible browser attachment when configured. Ask for approval before sensitive form submissions, purchases, or account changes. "
        "Keep browser actions narrow and report what was observed."
    ),
    "nullion/files-and-docs": (
        "Skill pack: nullion/files-and-docs\n"
        "Use for local files, documents, spreadsheets, slide decks, notes, and reports. "
        "Read existing files before editing, preserve unrelated user changes, and keep generated artifacts organized. "
        "For office files, verify rendered output when possible."
    ),
    "nullion/pdf-documents": (
        "Skill pack: nullion/pdf-documents\n"
        "Use when the user asks to create, convert, edit, summarize, or deliver a PDF. "
        "A requested PDF deliverable must be an actual .pdf artifact, not HTML, Markdown, plain text, or a path-only reply. "
        "If HTML or another source format is useful for layout, render or convert it to PDF before claiming completion. "
        "When images are requested, preserve the images in the PDF output when the available tools support it. "
        "Write the final PDF under the configured artifact directory, verify the file exists with a .pdf suffix and nonzero size, "
        "and make sure the final response includes a deliverable attachment path."
    ),
    "nullion/email-calendar": (
        "Skill pack: nullion/email-calendar\n"
        "Use for inbox triage, drafting replies, scheduling, meeting prep, reminders, and calendar summaries. "
        "This pack is guidance only; require the relevant email/calendar plugin and provider connection before accessing account data. "
        "Never send messages, delete mail, or create events without clear user intent and required approvals."
    ),
    "nullion/github-code": (
        "Skill pack: nullion/github-code\n"
        "Use for repository work, GitHub issues, pull requests, code review, release notes, and CI triage. "
        "Inspect local git state before edits, do not revert unrelated changes, and prefer small focused patches. "
        "Treat remote GitHub actions as external side effects that need explicit user intent."
    ),
    "nullion/media-local": (
        "Skill pack: nullion/media-local\n"
        "Use for audio transcription, image text extraction, image understanding, and local image generation workflows. "
        "Prefer local media providers when configured. Explain missing binaries or models plainly, and avoid uploading private media to cloud models unless the user chose that path."
    ),
    "nullion/productivity-memory": (
        "Skill pack: nullion/productivity-memory\n"
        "Use for task planning, daily summaries, recurring workflows, durable preferences, and follow-up organization. "
        "Distinguish reminders, memory, and scheduled tasks. Only claim something was saved or scheduled after the tool or config confirms it."
    ),
    "nullion/connector-skills": (
        "Skill pack: nullion/connector-skills\n"
        "Use for workflows that call external SaaS/API connector gateways, MCP servers, or custom HTTP bridges. "
        "These skills are workflow guidance, not account access by themselves. Check which credentials and tool adapters are configured before claiming access. "
        "Prefer the credential_ref on the active provider connection, then gateway-specific env vars derived from that provider id. "
        "Use the enabled skill pack's instructions or reference files for service-specific endpoint paths and parameters. "
        "For custom email bridges, Nullion's native custom_api_provider expects /email/search and /email/read/{id}. "
        "Always ask before writes, sends, deletes, payments, or account changes, and mention when a requested app still needs a connector or skill pack installed."
    ),
}


@dataclass(frozen=True, slots=True)
class InstalledSkillPack:
    pack_id: str
    source: str
    installed_at: str
    skills_count: int
    warnings: tuple[str, ...] = ()
    path: str | None = None


def default_skill_pack_root() -> Path:
    configured = os.environ.get("NULLION_SKILL_PACK_DIR", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".nullion" / "skill-packs"


def normalize_pack_id(pack_id: str) -> str:
    normalized = pack_id.strip().lower().strip("/")
    normalized = re.sub(r"/+", "/", normalized)
    if not _PACK_ID_RE.fullmatch(normalized):
        raise ValueError("pack_id must look like owner/pack")
    if ".." in normalized.split("/"):
        raise ValueError("pack_id cannot contain '..'")
    return normalized


def derive_pack_id_from_source(source: str) -> str:
    raw = source.strip()
    if not raw:
        raise ValueError("source is required")
    github_tree = _parse_github_tree_url(raw)
    if github_tree is not None:
        owner, _repo, _ref, subpath = github_tree
        pack_name = Path(subpath).name if subpath else _repo
        return normalize_pack_id(f"{owner}/{pack_name}")
    parsed = urlparse(raw)
    if parsed.scheme in {"http", "https", "ssh", "git"} and parsed.path:
        parts = [part for part in parsed.path.rstrip("/").split("/") if part]
        if len(parts) >= 2:
            owner = parts[-2].lower()
            repo = parts[-1].removesuffix(".git").lower()
            return normalize_pack_id(f"{owner}/{repo}")
    path = Path(raw).expanduser()
    name = path.name or "skills"
    parent = path.parent.name or "local"
    return normalize_pack_id(f"{parent}/{name}")


def skill_pack_path(pack_id: str, *, root: Path | None = None) -> Path:
    normalized = normalize_pack_id(pack_id)
    base = root or default_skill_pack_root()
    return base.joinpath(*normalized.split("/"))


def list_installed_skill_packs(*, root: Path | None = None) -> tuple[InstalledSkillPack, ...]:
    base = root or default_skill_pack_root()
    if not base.exists():
        return ()
    packs: list[InstalledSkillPack] = []
    for manifest_path in sorted(base.glob("*/*/nullion-skill-pack.json")):
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
            packs.append(
                InstalledSkillPack(
                    pack_id=str(data.get("pack_id") or ""),
                    source=str(data.get("source") or ""),
                    installed_at=str(data.get("installed_at") or ""),
                    skills_count=int(data.get("skills_count") or 0),
                    warnings=tuple(str(item) for item in data.get("warnings") or ()),
                    path=str(manifest_path.parent),
                )
            )
        except Exception:
            continue
    return tuple(pack for pack in packs if pack.pack_id)


def _file_signature(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return (int(stat.st_mtime_ns), int(stat.st_size))


def enabled_skill_pack_signature(
    enabled_pack_ids: tuple[str, ...] | list[str],
    *,
    root: Path | None = None,
    max_paths: int = 200,
) -> tuple[object, ...]:
    """Return a conservative fingerprint for enabled skill-pack prompt content."""

    installed = {pack.pack_id: pack for pack in list_installed_skill_packs(root=root)}
    signatures: list[object] = []
    for raw_pack_id in enabled_pack_ids:
        try:
            pack_id = normalize_pack_id(str(raw_pack_id))
        except ValueError:
            continue
        builtin_prompt = BUILTIN_SKILL_PACK_PROMPTS.get(pack_id)
        if builtin_prompt is not None:
            signatures.append((pack_id, "builtin", sha256(builtin_prompt.encode("utf-8")).hexdigest()))
            continue
        pack = installed.get(pack_id)
        if pack is None or not pack.path:
            signatures.append((pack_id, "missing"))
            continue
        pack_path = Path(pack.path).resolve()
        file_signatures: list[object] = []
        for relative_path in list_skill_pack_reference_paths(pack_id, root=root, max_paths=max_paths):
            file_path = pack_path / relative_path
            file_signatures.append((relative_path, _file_signature(file_path)))
        signatures.append(
            (
                pack_id,
                str(pack_path),
                _file_signature(pack_path / "nullion-skill-pack.json"),
                tuple(file_signatures),
            )
        )
    return tuple(signatures)


def get_installed_skill_pack(pack_id: str, *, root: Path | None = None) -> InstalledSkillPack | None:
    normalized = normalize_pack_id(pack_id)
    for pack in list_installed_skill_packs(root=root):
        if pack.pack_id == normalized:
            return pack
    return None


def list_skill_pack_reference_paths(
    pack_id: str,
    *,
    root: Path | None = None,
    max_paths: int = 160,
) -> tuple[str, ...]:
    pack = get_installed_skill_pack(pack_id, root=root)
    if pack is None or not pack.path:
        return ()
    pack_path = Path(pack.path).resolve()
    paths: list[str] = []
    for file_path in sorted(pack_path.rglob("*")):
        if len(paths) >= max_paths:
            break
        if not file_path.is_file():
            continue
        if file_path.name == "nullion-skill-pack.json":
            continue
        if file_path.suffix.lower() not in {".md", ".txt", ".json", ".yaml", ".yml"}:
            continue
        try:
            relative_path = file_path.resolve().relative_to(pack_path)
        except ValueError:
            continue
        paths.append(relative_path.as_posix())
    return tuple(paths)


def read_skill_pack_reference(
    pack_id: str,
    relative_path: str,
    *,
    root: Path | None = None,
    max_chars: int = 20000,
) -> str:
    normalized_pack_id = normalize_pack_id(pack_id)
    clean_path = str(relative_path or "").strip().lstrip("/")
    if not clean_path:
        raise ValueError("relative_path is required")
    if "\\" in clean_path:
        raise ValueError("relative_path must use forward slashes")
    relative = Path(clean_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("relative_path must stay inside the skill pack")
    if relative.suffix.lower() not in {"", ".md", ".txt", ".json", ".yaml", ".yml"}:
        raise ValueError("only text skill reference files can be read")
    builtin_prompt = BUILTIN_SKILL_PACK_PROMPTS.get(normalized_pack_id)
    if builtin_prompt is not None:
        if relative.as_posix() not in _BUILTIN_REFERENCE_ALIASES:
            raise FileNotFoundError(f"skill reference not found: {relative.as_posix()}")
        if len(builtin_prompt) <= max_chars:
            return builtin_prompt
        return builtin_prompt[:max_chars].rstrip() + "\n[truncated]"
    pack = get_installed_skill_pack(normalized_pack_id, root=root)
    if pack is None or not pack.path:
        raise FileNotFoundError(f"skill pack is not installed: {normalized_pack_id}")
    pack_path = Path(pack.path).resolve()
    file_path = pack_path.joinpath(relative).resolve()
    try:
        file_path.relative_to(pack_path)
    except ValueError as exc:
        raise ValueError("relative_path must stay inside the skill pack") from exc
    if not file_path.is_file():
        # Installed API gateway packs commonly expose service docs as
        # references/<service>/README.md. Accept the file-shaped shorthand only
        # when that exact structured reference exists.
        if len(relative.parts) == 1 and relative.suffix.lower() in {"", ".md", ".txt"}:
            reference_name = relative.stem if relative.suffix else relative.name
            fallback_path = pack_path / "references" / reference_name / "README.md"
            try:
                fallback_path.resolve().relative_to(pack_path)
            except ValueError as exc:
                raise ValueError("relative_path must stay inside the skill pack") from exc
            if fallback_path.is_file():
                file_path = fallback_path
            else:
                raise FileNotFoundError(f"skill reference not found: {relative.as_posix()}")
        else:
            raise FileNotFoundError(f"skill reference not found: {relative.as_posix()}")
    text = file_path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "\n[truncated]"


def format_enabled_skill_packs_for_prompt(
    enabled_pack_ids: tuple[str, ...] | list[str],
    *,
    root: Path | None = None,
    max_chars_per_skill: int = 5000,
    max_total_chars: int = 12000,
) -> str:
    installed = {pack.pack_id: pack for pack in list_installed_skill_packs(root=root)}
    blocks: list[str] = []
    total_chars = 0
    for raw_pack_id in enabled_pack_ids:
        try:
            pack_id = normalize_pack_id(str(raw_pack_id))
        except ValueError:
            continue
        builtin_prompt = BUILTIN_SKILL_PACK_PROMPTS.get(pack_id)
        if builtin_prompt:
            remaining_total = max_total_chars - total_chars
            if remaining_total <= 0:
                break
            excerpt = builtin_prompt[:remaining_total].rstrip()
            if len(builtin_prompt) > remaining_total:
                excerpt += "\n[truncated]"
            blocks.append(excerpt)
            total_chars += len(excerpt)
            continue
        pack = installed.get(pack_id)
        if pack is None or not pack.path:
            continue
        pack_path = Path(pack.path)
        skill_files = sorted(pack_path.rglob("SKILL.md"))
        if not skill_files:
            continue
        remaining_total = max_total_chars - total_chars
        if remaining_total <= 0:
            break
        skill_blocks = []
        for skill_file in skill_files:
            remaining_total = max_total_chars - total_chars
            if remaining_total <= 0:
                break
            try:
                text = skill_file.read_text(encoding="utf-8", errors="replace").strip()
            except Exception:
                continue
            if not text:
                continue
            limit = min(max_chars_per_skill, remaining_total)
            excerpt = text[:limit].rstrip()
            if len(text) > limit:
                excerpt += "\n[truncated]"
            relative_path = skill_file.relative_to(pack_path)
            skill_blocks.append(f"File: {relative_path}\n{excerpt}")
            total_chars += len(excerpt)
        if skill_blocks:
            reference_paths = list_skill_pack_reference_paths(pack.pack_id, root=root)
            reference_block = ""
            if reference_paths:
                remaining_total = max_total_chars - total_chars
                if remaining_total > 0:
                    reference_text = (
                        "Reference files available for this skill pack. "
                        "Use the skill_pack_read tool with pack_id and path when a request needs service-specific API details:\n"
                        + "\n".join(f"- {path}" for path in reference_paths)
                    )
                    reference_block = "\n\n" + reference_text[:remaining_total].rstrip()
                    if len(reference_text) > remaining_total:
                        reference_block += "\n[truncated]"
                    total_chars += len(reference_block)
            blocks.append(
                f"Skill pack: {pack.pack_id}\n"
                f"Source: {pack.source}\n"
                "Loaded instructions:\n"
                + "\n\n".join(skill_blocks)
                + reference_block
            )
    if not blocks:
        return ""
    return (
        "Enabled installed skill packs are reference instructions. "
        "Use them when relevant, but do not assume they grant account access or credentials.\n\n"
        + "\n\n---\n\n".join(blocks)
    )


def _metadata_description_excerpt(text: str, *, max_chars: int = 180) -> str:
    lines = [line.rstrip() for line in str(text or "").splitlines()]
    in_description = False
    collected: list[str] = []
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            if collected:
                break
            continue
        if line.startswith("description:"):
            value = line.partition(":")[2].strip().strip("|>- ")
            if value:
                collected.append(value)
            in_description = True
            continue
        if in_description:
            if raw_line[:1] and not raw_line.startswith((" ", "\t", "-", "  ")):
                break
            collected.append(line.lstrip("- ").strip())
    summary = " ".join(part for part in collected if part).strip()
    if not summary:
        for raw_line in lines:
            line = raw_line.strip()
            if line and not line.startswith(("---", "name:", "metadata:", "compatibility:", "Skill pack:")):
                summary = line
                break
    if len(summary) > max_chars:
        summary = summary[: max_chars - 12].rstrip() + " [truncated]"
    return summary


def format_compact_enabled_skill_packs_for_prompt(
    enabled_pack_ids: tuple[str, ...] | list[str],
    *,
    root: Path | None = None,
    max_total_chars: int = 2800,
) -> str:
    """Return compact stable skill-pack guidance for default chat context.

    Full skill pack instructions are intentionally not pasted into every turn;
    the model can fetch detailed reference text with ``skill_pack_read`` when a
    structured tool plan needs it.
    """
    installed = {pack.pack_id: pack for pack in list_installed_skill_packs(root=root)}
    lines: list[str] = [
        "Enabled skill packs are reference instructions, not account access.",
        "Use skill_pack_read for detailed installed-pack docs when a structured tool plan needs service-specific steps.",
    ]
    total_chars = sum(len(line) for line in lines)
    for raw_pack_id in enabled_pack_ids:
        try:
            pack_id = normalize_pack_id(str(raw_pack_id))
        except ValueError:
            continue
        summary = ""
        detail_hint = ""
        builtin_prompt = BUILTIN_SKILL_PACK_PROMPTS.get(pack_id)
        if builtin_prompt:
            summary = _metadata_description_excerpt(builtin_prompt)
        else:
            pack = installed.get(pack_id)
            if pack is None or not pack.path:
                continue
            pack_path = Path(pack.path)
            skill_files = sorted(pack_path.rglob("SKILL.md"))
            if skill_files:
                try:
                    summary = _metadata_description_excerpt(skill_files[0].read_text(encoding="utf-8", errors="replace"))
                except Exception:
                    summary = ""
            reference_paths = list_skill_pack_reference_paths(pack.pack_id, root=root)
            common_paths = [path for path in ("SKILL.md", "README.md") if path in reference_paths]
            if common_paths:
                detail_hint = " details: " + ", ".join(common_paths)
            elif reference_paths:
                detail_hint = f" details: {len(reference_paths)} reference file(s) via skill_pack_read"
        line = f"- {pack_id}"
        if summary:
            line += f": {summary}"
        if detail_hint:
            line += f" ({detail_hint})"
        if total_chars + len(line) > max_total_chars:
            lines.append("- additional enabled skill packs omitted from compact prompt; use /skill-packs or skill_pack_read when needed")
            break
        lines.append(line)
        total_chars += len(line)
    return "\n".join(lines).strip() if len(lines) > 2 else ""


def format_cached_enabled_skill_pack_index_for_prompt(
    enabled_pack_ids: tuple[str, ...] | list[str],
    *,
    root: Path | None = None,
    max_total_chars: int = 900,
) -> str:
    """Return a tiny cached index of enabled skills for cheap routing.

    This is safe to include in lightweight turns because it is only a compact
    index. Full instructions and reference files still require an explicit
    ``skill_pack_read`` call or a structured scope decision that opts into
    skill-pack context.
    """

    try:
        normalized_pack_ids = tuple(
            normalize_pack_id(str(pack_id))
            for pack_id in enabled_pack_ids
            if str(pack_id or "").strip()
        )
    except ValueError:
        normalized_pack_ids = tuple(
            pack_id
            for raw in enabled_pack_ids
            for pack_id in [str(raw or "").strip().lower()]
            if pack_id
        )
    if not normalized_pack_ids:
        return ""
    try:
        from nullion import runtime_cache

        cache_key = {
            "enabled": list(normalized_pack_ids),
            "signature": enabled_skill_pack_signature(normalized_pack_ids, root=root, max_paths=24),
            "max_total_chars": max_total_chars,
        }
        cached = runtime_cache.get_json(
            _SKILL_PACK_INDEX_CACHE_NAMESPACE,
            cache_key,
            version=_SKILL_PACK_INDEX_CACHE_VERSION,
            ttl_seconds=_SKILL_PACK_INDEX_CACHE_TTL_SECONDS,
            persistent=True,
        )
        if cached.hit and isinstance(cached.value, str):
            return cached.value
    except Exception:
        cache_key = None
    text = format_compact_enabled_skill_packs_for_prompt(
        normalized_pack_ids,
        root=root,
        max_total_chars=max_total_chars,
    )
    if text:
        text = (
            "Installed skill index for routing only. Do not treat this as account access; "
            "load exact docs with skill_pack_read only when the current structured plan needs them.\n"
            + text
        )
    if text and cache_key is not None:
        try:
            from nullion import runtime_cache

            runtime_cache.set_json(
                _SKILL_PACK_INDEX_CACHE_NAMESPACE,
                cache_key,
                text,
                version=_SKILL_PACK_INDEX_CACHE_VERSION,
                ttl_seconds=_SKILL_PACK_INDEX_CACHE_TTL_SECONDS,
                persistent=True,
                max_entries=64,
            )
        except Exception:
            logger.debug("Could not cache compact skill-pack index", exc_info=True)
    return text


def install_skill_pack(
    source: str,
    *,
    pack_id: str | None = None,
    root: Path | None = None,
    force: bool = False,
) -> InstalledSkillPack:
    normalized_id = normalize_pack_id(pack_id) if pack_id else derive_pack_id_from_source(source)
    destination = skill_pack_path(normalized_id, root=root)
    if destination.exists() and not force:
        raise FileExistsError(f"skill pack already installed: {normalized_id}")
    if destination.exists():
        shutil.rmtree(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    raw_source = source.strip()
    github_source = _parse_github_source_url(raw_source)
    if github_source is not None:
        _install_github_archive_source(github_source, destination)
    elif _looks_like_git_source(raw_source):
        _clone_git_source(raw_source, destination)
    else:
        source_path = Path(raw_source).expanduser()
        if not source_path.exists():
            raise FileNotFoundError(f"source path not found: {source}")
        if source_path.is_file():
            raise ValueError("source must be a directory or git repository URL")
        shutil.copytree(source_path, destination, ignore=shutil.ignore_patterns(".git", "__pycache__", ".DS_Store"))

    skill_files = sorted(destination.rglob("SKILL.md"))
    if not skill_files:
        shutil.rmtree(destination, ignore_errors=True)
        raise ValueError("no SKILL.md files found in source")
    warnings = _scan_skill_files(skill_files)
    installed_at = datetime.now(UTC).isoformat()
    manifest = {
        "pack_id": normalized_id,
        "source": raw_source,
        "installed_at": installed_at,
        "skills_count": len(skill_files),
        "warnings": warnings,
    }
    (destination / "nullion-skill-pack.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    try:
        from nullion import runtime_cache

        runtime_cache.invalidate_prefix("stable_context")
    except Exception:
        logger.debug("Could not invalidate stable context cache after skill pack install", exc_info=True)
    return InstalledSkillPack(
        pack_id=normalized_id,
        source=raw_source,
        installed_at=installed_at,
        skills_count=len(skill_files),
        warnings=tuple(warnings),
        path=str(destination),
    )


def uninstall_skill_pack(
    pack_id: str,
    *,
    root: Path | None = None,
) -> InstalledSkillPack | None:
    normalized_id = normalize_pack_id(pack_id)
    pack = get_installed_skill_pack(normalized_id, root=root)
    if pack is None:
        return None
    base = (root or default_skill_pack_root()).resolve()
    destination = Path(pack.path).resolve() if pack.path else skill_pack_path(normalized_id, root=root).resolve()
    try:
        destination.relative_to(base)
    except ValueError as exc:
        raise ValueError("installed skill pack path is outside the configured skill pack root") from exc
    shutil.rmtree(destination)
    try:
        from nullion import runtime_cache

        runtime_cache.invalidate_prefix("stable_context")
    except Exception:
        logger.debug("Could not invalidate stable context cache after skill pack uninstall", exc_info=True)
    return pack


def _looks_like_git_source(source: str) -> bool:
    parsed = urlparse(source)
    return parsed.scheme in {"http", "https", "ssh", "git"} or source.startswith("git@")


def _is_allowed_git_source(source: str) -> bool:
    parsed = urlparse(source)
    return parsed.scheme == "https" and parsed.netloc.lower() in {"github.com", "gitlab.com"}


def _parse_github_tree_url(source: str) -> tuple[str, str, str, str] | None:
    parsed = urlparse(source)
    if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() != "github.com":
        return None
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) < 4 or parts[2] != "tree":
        return None
    owner = parts[0]
    repo = parts[1].removesuffix(".git")
    ref = parts[3]
    subpath = "/".join(parts[4:])
    return owner, repo, ref, subpath


def _parse_github_repo_url(source: str) -> tuple[str, str, str, str] | None:
    parsed = urlparse(source)
    if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() != "github.com":
        return None
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) != 2:
        return None
    owner = parts[0]
    repo = parts[1].removesuffix(".git")
    return owner, repo, "main", ""


def _parse_github_source_url(source: str) -> tuple[str, str, str, str] | None:
    return _parse_github_tree_url(source) or _parse_github_repo_url(source)


def _install_github_archive_source(source: tuple[str, str, str, str], destination: Path) -> None:
    owner, repo, ref, subpath = source
    archive_url = f"https://codeload.github.com/{owner}/{repo}/zip/refs/heads/{ref}"
    with tempfile.TemporaryDirectory(prefix="nullion-skill-pack-") as tmp:
        tmp_path = Path(tmp)
        archive_path = tmp_path / "source.zip"
        try:
            with urlopen(archive_url, timeout=30) as response:
                archive_path.write_bytes(response.read())
        except Exception as exc:
            raise RuntimeError(f"could not download GitHub skill pack archive: {archive_url}") from exc

        try:
            with zipfile.ZipFile(archive_path) as archive:
                extract_dir = (tmp_path / "extract").resolve()
                extract_dir.mkdir(parents=True, exist_ok=True)
                for info in archive.infolist():
                    member = info.filename
                    if os.path.isabs(member) or ".." in Path(member).parts:
                        raise RuntimeError(
                            f"Malicious ZIP member attempts path traversal: {member!r}"
                        )
                    mode = (info.external_attr >> 16) & 0o170000
                    if mode == 0o120000:
                        raise RuntimeError(f"Symlink ZIP members are not allowed: {member!r}")
                    member_path = (extract_dir / member).resolve()
                    try:
                        member_path.relative_to(extract_dir)
                    except ValueError:
                        raise RuntimeError(
                            f"Malicious ZIP member attempts path traversal: {member!r}"
                        )
                archive.extractall(extract_dir)
        except zipfile.BadZipFile as exc:
            raise RuntimeError("downloaded GitHub archive was not a valid zip file") from exc

        extracted_roots = [path for path in (tmp_path / "extract").iterdir() if path.is_dir()]
        if not extracted_roots:
            raise RuntimeError("downloaded GitHub archive was empty")
        archive_root = extracted_roots[0]
        source_path = archive_root / subpath if subpath else archive_root
        if not source_path.exists() or not source_path.is_dir():
            raise FileNotFoundError(f"GitHub source folder not found: {subpath or repo}")
        shutil.copytree(
            source_path,
            destination,
            ignore=shutil.ignore_patterns(".git", "__pycache__", ".DS_Store"),
        )


def _clone_git_source(source: str, destination: Path) -> None:
    if not _is_allowed_git_source(source):
        raise ValueError("git skill pack sources must be HTTPS URLs on github.com or gitlab.com")
    if shutil.which("git") is None:
        raise RuntimeError("git is required to install this skill pack source")
    env = {key: value for key, value in os.environ.items() if key in {"HOME", "PATH", "LANG", "LC_ALL", "TZ"}}
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_ALLOW_PROTOCOL"] = "https"
    subprocess.run(
        [
            "git",
            "-c",
            "protocol.ext.allow=never",
            "-c",
            "protocol.file.allow=never",
            "clone",
            "--depth",
            "1",
            "--",
            source,
            str(destination),
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def _scan_skill_files(skill_files: list[Path]) -> list[str]:
    warnings: list[str] = []
    for skill_file in skill_files:
        try:
            text = skill_file.read_text(encoding="utf-8", errors="replace").lower()
        except Exception:
            continue
        hits = sorted({pattern.strip() for pattern in _SUSPICIOUS_PATTERNS if pattern in text})
        if hits:
            warnings.append(f"{skill_file.parent.name}/SKILL.md mentions: {', '.join(hits[:5])}")
    return warnings


__all__ = [
    "InstalledSkillPack",
    "BUILTIN_SKILL_PACK_PROMPTS",
    "default_skill_pack_root",
    "derive_pack_id_from_source",
    "format_compact_enabled_skill_packs_for_prompt",
    "format_cached_enabled_skill_pack_index_for_prompt",
    "format_enabled_skill_packs_for_prompt",
    "get_installed_skill_pack",
    "enabled_skill_pack_signature",
    "install_skill_pack",
    "list_installed_skill_packs",
    "list_skill_pack_reference_paths",
    "normalize_pack_id",
    "read_skill_pack_reference",
    "skill_pack_path",
    "uninstall_skill_pack",
]
