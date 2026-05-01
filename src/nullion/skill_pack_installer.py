"""Install and inspect local skill packs.

Skill packs are reference instructions, not executable packages.  The installer
copies or clones skill folders and indexes `SKILL.md` files; it never runs code
from the pack.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen


_PACK_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._/-]*$")
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
    if relative.suffix.lower() not in {".md", ".txt", ".json", ".yaml", ".yml"}:
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
    return InstalledSkillPack(
        pack_id=normalized_id,
        source=raw_source,
        installed_at=installed_at,
        skills_count=len(skill_files),
        warnings=tuple(warnings),
        path=str(destination),
    )


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
    "format_enabled_skill_packs_for_prompt",
    "get_installed_skill_pack",
    "install_skill_pack",
    "list_installed_skill_packs",
    "list_skill_pack_reference_paths",
    "normalize_pack_id",
    "read_skill_pack_reference",
    "skill_pack_path",
]
