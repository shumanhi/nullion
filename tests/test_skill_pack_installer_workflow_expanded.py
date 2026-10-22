from __future__ import annotations

import io
import json
import zipfile

import pytest


def _skill_pack(root, *, text: str = "# Skill\nUse carefully.") -> None:
    skill_dir = root / "skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(text, encoding="utf-8")
    (skill_dir / "notes.txt").write_text("reference", encoding="utf-8")
    (skill_dir / "binary.bin").write_bytes(b"\x00")


def test_pack_id_source_derivation_and_path_validation(tmp_path, monkeypatch) -> None:
    from nullion import skill_pack_installer as installer

    monkeypatch.setenv("NULLION_SKILL_PACK_DIR", str(tmp_path / "packs"))

    assert installer.default_skill_pack_root() == tmp_path / "packs"
    assert installer.normalize_pack_id(" Owner//Pack ") == "owner/pack"
    assert installer.derive_pack_id_from_source("https://github.com/OpenAI/Repo/tree/main/skills/web") == "openai/web"
    assert installer.derive_pack_id_from_source("https://github.com/OpenAI/Repo.git") == "openai/repo"
    assert installer.derive_pack_id_from_source(str(tmp_path / "local" / "pack")) == "local/pack"
    assert installer.skill_pack_path("owner/pack", root=tmp_path) == tmp_path / "owner" / "pack"

    for bad in ("", "owner", "../owner/pack", "owner/../pack"):
        with pytest.raises(ValueError):
            installer.normalize_pack_id(bad)


def test_default_skill_pack_root_is_scoped_to_runtime_home(tmp_path, monkeypatch) -> None:
    from nullion import skill_pack_installer as installer

    monkeypatch.delenv("NULLION_SKILL_PACK_DIR", raising=False)
    monkeypatch.delenv("NULLION_DATA_DIR", raising=False)
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "stage-home"))
    assert installer.default_skill_pack_root() == tmp_path / "stage-home" / "skill-packs"

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "stage-data"))
    assert installer.default_skill_pack_root() == tmp_path / "stage-data" / "skill-packs"

    monkeypatch.setenv("NULLION_SKILL_PACK_DIR", str(tmp_path / "explicit-packs"))
    assert installer.default_skill_pack_root() == tmp_path / "explicit-packs"


def test_install_list_read_and_prompt_format_local_skill_pack(tmp_path) -> None:
    from nullion import skill_pack_installer as installer

    source = tmp_path / "source"
    _skill_pack(source, text="# Skill\nRun bash only after approval. token handling matters.")
    root = tmp_path / "installed"

    installed = installer.install_skill_pack(str(source), pack_id="local/demo", root=root)

    assert installed.pack_id == "local/demo"
    assert installed.skills_count == 1
    assert installed.warnings == ("skill/SKILL.md mentions: bash, sh, token",)
    assert installer.get_installed_skill_pack("LOCAL/DEMO", root=root).pack_id == "local/demo"
    assert [pack.pack_id for pack in installer.list_installed_skill_packs(root=root)] == ["local/demo"]
    assert installer.list_skill_pack_reference_paths("local/demo", root=root) == ("skill/SKILL.md", "skill/notes.txt")
    assert installer.read_skill_pack_reference("local/demo", "skill/notes.txt", root=root) == "reference"
    assert installer.read_skill_pack_reference("local/demo", "skill/SKILL.md", root=root, max_chars=10).endswith("[truncated]")

    prompt = installer.format_enabled_skill_packs_for_prompt(["nullion/web-research", "local/demo", "bad"], root=root, max_chars_per_skill=40)
    assert "Enabled installed skill packs" in prompt
    assert "Skill pack: nullion/web-research" in prompt
    assert "Skill pack: local/demo" in prompt
    assert "Reference files available" in prompt

    with pytest.raises(FileExistsError):
        installer.install_skill_pack(str(source), pack_id="local/demo", root=root)

    reinstalled = installer.install_skill_pack(str(source), pack_id="local/demo", root=root, force=True)
    assert reinstalled.pack_id == "local/demo"

    removed = installer.uninstall_skill_pack("local/demo", root=root)
    assert removed is not None
    assert removed.pack_id == "local/demo"
    assert installer.get_installed_skill_pack("local/demo", root=root) is None
    assert "Skill pack: local/demo" not in installer.format_enabled_skill_packs_for_prompt(["local/demo"], root=root)


def test_builtin_skill_pack_prompts_are_prompt_only_without_reference_read_advertising(tmp_path) -> None:
    from nullion import skill_pack_installer as installer

    for pack_id in sorted(installer.BUILTIN_SKILL_PACK_PROMPTS):
        prompt = installer.format_enabled_skill_packs_for_prompt([pack_id], root=tmp_path)

        assert f"Skill pack: {pack_id}" in prompt
        assert "Reference files available" not in prompt
        assert "skill_pack_read" not in prompt


def test_builtin_connector_skill_stays_provider_agnostic(tmp_path) -> None:
    from nullion import skill_pack_installer as installer

    prompt = installer.format_enabled_skill_packs_for_prompt(["nullion/connector-skills"], root=tmp_path)

    assert "credential_ref on the active provider connection" in prompt
    assert "enabled skill pack's instructions" in prompt
    assert "MATON_API_KEY" not in prompt
    assert "gateway.maton.ai" not in prompt


def test_builtin_provider_connector_skills_are_available_without_local_install(tmp_path) -> None:
    from nullion import skill_pack_installer as installer

    provider_packs = {
        "maton-ai/api-gateway-skill": "MATON_API_KEY",
        "composio/mcp-connector-skill": "COMPOSIO_API_KEY",
        "nango/mcp-connector-skill": "NANGO_SECRET_KEY",
        "activepieces/mcp-connector-skill": "ACTIVEPIECES_API_KEY",
        "n8n/mcp-connector-skill": "N8N_API_KEY",
    }

    for pack_id, expected_hint in provider_packs.items():
        prompt = installer.format_enabled_skill_packs_for_prompt([pack_id], root=tmp_path)
        assert f"Skill pack: {pack_id}" in prompt
        assert expected_hint in prompt
        prompt_lower = prompt.lower()
        assert (
            "do not infer" in prompt_lower
            or "do not assume" in prompt_lower
            or "requires an authorized" in prompt_lower
        )


def test_builtin_pdf_skill_requires_real_pdf_delivery(tmp_path) -> None:
    from nullion import skill_pack_installer as installer

    prompt = installer.format_enabled_skill_packs_for_prompt(["nullion/pdf-documents"], root=tmp_path)

    assert "Skill pack: nullion/pdf-documents" in prompt
    assert "actual .pdf artifact" in prompt
    assert "not HTML" in prompt
    assert "deliverable attachment path" in prompt


def test_enabled_builtin_skill_pack_reference_read_does_not_dead_end_as_missing(tmp_path) -> None:
    from nullion import skill_pack_installer as installer

    for path in ("SKILL.md", "README.md", "README.txt"):
        assert "Skill pack: nullion/connector-skills" in installer.read_skill_pack_reference(
            "nullion/connector-skills",
            path,
            root=tmp_path,
        )
    assert installer.read_skill_pack_reference(
        "nullion/connector-skills",
        "SKILL.md",
        root=tmp_path,
        max_chars=10,
    ).endswith("[truncated]")
    with pytest.raises(FileNotFoundError, match="skill reference not found"):
        installer.read_skill_pack_reference("nullion/connector-skills", "docs/reference.md", root=tmp_path)


def test_installed_pack_reference_wins_over_builtin_prompt_for_service_docs(tmp_path) -> None:
    from nullion import skill_pack_installer as installer

    source = tmp_path / "source"
    _skill_pack(source)
    reference_dir = source / "references" / "google-mail"
    reference_dir.mkdir(parents=True)
    (reference_dir / "README.md").write_text(
        "Use /google-mail/gmail/v1/users/me/messages for Gmail message lists.",
        encoding="utf-8",
    )
    root = tmp_path / "installed"

    installer.install_skill_pack(str(source), pack_id="maton-ai/api-gateway-skill", root=root)

    text = installer.read_skill_pack_reference(
        "maton-ai/api-gateway-skill",
        "references/google-mail/README.md",
        root=root,
    )

    assert "/google-mail/gmail/v1/users/me/messages" in text
    assert "Skill pack: maton-ai/api-gateway-skill" not in text


def test_skill_reference_rejects_unsafe_paths_and_missing_pack(tmp_path) -> None:
    from nullion import skill_pack_installer as installer

    root = tmp_path / "installed"
    with pytest.raises(FileNotFoundError):
        installer.read_skill_pack_reference("local/missing", "SKILL.md", root=root)

    source = tmp_path / "source"
    _skill_pack(source)
    installer.install_skill_pack(str(source), pack_id="local/demo", root=root)

    for path in ("", "../x", "skill\\SKILL.md", "skill/binary.bin"):
        with pytest.raises(ValueError):
            installer.read_skill_pack_reference("local/demo", path, root=root)
    with pytest.raises(FileNotFoundError):
        installer.read_skill_pack_reference("local/demo", "skill/missing.md", root=root)


def test_skill_reference_resolves_structured_reference_shorthand(tmp_path) -> None:
    from nullion import skill_pack_installer as installer

    source = tmp_path / "source"
    _skill_pack(source)
    reference_dir = source / "references" / "google-mail"
    reference_dir.mkdir(parents=True)
    (reference_dir / "README.md").write_text("gmail reference", encoding="utf-8")
    root = tmp_path / "installed"

    installer.install_skill_pack(str(source), pack_id="local/demo", root=root)

    assert installer.read_skill_pack_reference("local/demo", "google-mail.md", root=root) == "gmail reference"
    assert installer.read_skill_pack_reference("local/demo", "google-mail", root=root) == "gmail reference"


def test_install_rejects_missing_file_and_no_skill_sources(tmp_path) -> None:
    from nullion import skill_pack_installer as installer

    with pytest.raises(FileNotFoundError):
        installer.install_skill_pack(str(tmp_path / "missing"), pack_id="local/missing", root=tmp_path / "root")

    file_source = tmp_path / "source.txt"
    file_source.write_text("nope", encoding="utf-8")
    with pytest.raises(ValueError, match="directory"):
        installer.install_skill_pack(str(file_source), pack_id="local/file", root=tmp_path / "root")

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError, match="no SKILL.md"):
        installer.install_skill_pack(str(empty), pack_id="local/empty", root=tmp_path / "root")


def test_git_and_github_archive_paths_are_safeguarded(tmp_path, monkeypatch) -> None:
    from nullion import skill_pack_installer as installer

    assert installer._looks_like_git_source("git@github.com:owner/repo.git")
    assert installer._is_allowed_git_source("https://github.com/owner/repo.git")
    assert not installer._is_allowed_git_source("ssh://github.com/owner/repo.git")

    monkeypatch.setattr(installer.shutil, "which", lambda name: None)
    with pytest.raises(RuntimeError, match="git is required"):
        installer._clone_git_source("https://github.com/owner/repo.git", tmp_path / "dest")
    with pytest.raises(ValueError, match="HTTPS URLs"):
        installer._clone_git_source("https://evil.example/repo.git", tmp_path / "dest")

    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("repo-main/skills/demo/SKILL.md", "# Demo")

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return payload.getvalue()

    monkeypatch.setattr(installer, "urlopen", lambda url, timeout: Response())
    destination = tmp_path / "downloaded"
    installer._install_github_archive_source(("owner", "repo", "main", "skills/demo"), destination)
    assert (destination / "SKILL.md").read_text(encoding="utf-8") == "# Demo"

    bad_payload = io.BytesIO()
    with zipfile.ZipFile(bad_payload, "w") as archive:
        archive.writestr("../escape/SKILL.md", "# Bad")

    class BadResponse(Response):
        def read(self):
            return bad_payload.getvalue()

    monkeypatch.setattr(installer, "urlopen", lambda url, timeout: BadResponse())
    with pytest.raises(RuntimeError, match="path traversal"):
        installer._install_github_archive_source(("owner", "repo", "main", ""), tmp_path / "bad")
