from __future__ import annotations

import os
from pathlib import Path
import subprocess


ROOT = Path(
    os.environ.get(
        "NULLION_APP_REPO",
        Path(__file__).resolve().parents[1],
    )
).resolve()


def _read_app_file(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def _skill_pack_function_block() -> str:
    install_sh = _read_app_file("install.sh")
    start = install_sh.index('ENABLED_SKILL_PACKS=""')
    end = install_sh.index("EXISTING_SKILL_PACKS=", start)
    return install_sh[start:end]


def test_skill_pack_list_helpers_handle_empty_values_under_set_u() -> None:
    script = f"""
set -euo pipefail
print_info() {{ echo "$*"; }}
env_value() {{ return 0; }}
{_skill_pack_function_block()}
normalized="$(normalize_skill_pack_list "")"
printf 'normalized=%s\\n' "$normalized"
print_skill_pack_list "Existing" "$normalized"
"""

    result = subprocess.run(["bash", "-c", script], text=True, capture_output=True, check=True)

    assert "normalized=\n" in result.stdout
    assert "Existing: none" in result.stdout


def test_skill_pack_list_helpers_trim_and_dedupe_values_under_set_u() -> None:
    script = f"""
set -euo pipefail
print_info() {{ echo "$*"; }}
env_value() {{ return 0; }}
{_skill_pack_function_block()}
normalized="$(normalize_skill_pack_list " nullion/email-calendar, nullion/email-calendar ,nullion/media-local ")"
printf 'normalized=%s\\n' "$normalized"
print_skill_pack_list "Enabled" "$normalized"
"""

    result = subprocess.run(["bash", "-c", script], text=True, capture_output=True, check=True)

    assert "normalized=nullion/email-calendar,nullion/media-local" in result.stdout
    assert "    - nullion/email-calendar" in result.stdout
    assert "    - nullion/media-local" in result.stdout


def test_fresh_install_defaults_include_pdf_skill_pack() -> None:
    install_sh = _read_app_file("install.sh")
    install_ps1 = _read_app_file("install.ps1")

    assert "CORE_SKILL_PACKS=" in install_sh
    assert "nullion/pdf-documents" in install_sh
    assert "request_skill_pack_choices" not in install_sh
    assert "No default skill packs" not in install_sh

    assert "$CORE_SKILL_PACKS = @(" in install_ps1
    assert '"nullion/pdf-documents"' in install_ps1
    assert "function Request-SkillPackChoices" not in install_ps1
    assert "No default skill packs" not in install_ps1


def test_connector_install_choices_enable_matching_provider_skill_packs() -> None:
    install_sh = _read_app_file("install.sh")
    install_ps1 = _read_app_file("install.ps1")

    assert 'CONNECTOR_SELECTED_SKILL_PACKS="${CONNECTOR_SELECTED_SKILL_PACKS},maton-ai/api-gateway-skill"' in install_sh
    assert 'CONNECTOR_SELECTED_SKILL_PACKS="${CONNECTOR_SELECTED_SKILL_PACKS},composio/mcp-connector-skill"' in install_sh
    assert 'CONNECTOR_SELECTED_SKILL_PACKS="${CONNECTOR_SELECTED_SKILL_PACKS},nango/mcp-connector-skill"' in install_sh
    assert 'CONNECTOR_SELECTED_SKILL_PACKS="${CONNECTOR_SELECTED_SKILL_PACKS},activepieces/mcp-connector-skill"' in install_sh
    assert 'CONNECTOR_SELECTED_SKILL_PACKS="${CONNECTOR_SELECTED_SKILL_PACKS},n8n/mcp-connector-skill"' in install_sh
    assert 'add_skill_pack_list "$CONNECTOR_SELECTED_SKILL_PACKS"' in install_sh

    assert '$CONNECTOR_SELECTED_SKILL_PACKS += "maton-ai/api-gateway-skill"' in install_ps1
    assert '$CONNECTOR_SELECTED_SKILL_PACKS += "composio/mcp-connector-skill"' in install_ps1
    assert '$CONNECTOR_SELECTED_SKILL_PACKS += "nango/mcp-connector-skill"' in install_ps1
    assert '$CONNECTOR_SELECTED_SKILL_PACKS += "activepieces/mcp-connector-skill"' in install_ps1
    assert '$CONNECTOR_SELECTED_SKILL_PACKS += "n8n/mcp-connector-skill"' in install_ps1
    assert "foreach ($pack in $CONNECTOR_SELECTED_SKILL_PACKS)" in install_ps1


def test_shell_installer_falls_back_when_dev_tty_is_not_usable() -> None:
    install_sh = _read_app_file("install.sh")
    start = install_sh.index("prompt_read() {")
    end = install_sh.index("}\n\nprompt_model_name", start)
    prompt_read = install_sh[start:end]

    assert "{ : </dev/tty; } 2>/dev/null" in prompt_read
    assert 'read "$@" </dev/tty' in prompt_read
    assert 'read "$@"' in prompt_read


def test_shell_installer_smoke_homebrew_installs_are_non_interactive() -> None:
    install_sh = _read_app_file("install.sh")
    start = install_sh.index("brew_install() {")
    end = install_sh.index("}\n\nxml_escape", start)
    brew_install = install_sh[start:end]
    install_sh_without_wrapper = install_sh[:start] + install_sh[end:]

    assert '${NULLION_INSTALLER_SMOKE:-false}' in brew_install
    assert 'HOMEBREW_NO_ASK=1 brew install "$@"' in brew_install
    assert "brew install \"${missing_packages[@]}\"" not in install_sh_without_wrapper
    assert "brew install \"${brew_packages[@]}\"" not in install_sh_without_wrapper
    assert "brew install git" not in install_sh_without_wrapper
    assert "brew install python@3.12" not in install_sh_without_wrapper
    assert "\n                        brew install himalaya" not in install_sh_without_wrapper


def test_fresh_install_provisions_media_and_browser_runtime_by_default() -> None:
    install_sh = _read_app_file("install.sh")
    install_ps1 = _read_app_file("install.ps1")

    assert 'install_playwright_runtime || true' in install_sh
    assert 'install_default_local_media_runtime' in install_sh
    assert 'ENABLED_PLUGINS="search_plugin,browser_plugin,workspace_plugin,media_plugin"' in install_sh
    assert 'PROVIDER_BINDINGS="search_plugin=${SEARCH_PROVIDER},media_plugin=local_media_provider"' in install_sh

    assert "[void](Install-PlaywrightRuntime)" in install_ps1
    assert "Install-DefaultLocalMediaRuntime" in install_ps1
    assert '$enabledPlugins = "search_plugin,browser_plugin,workspace_plugin,media_plugin"' in install_ps1
    assert '$providerBindings = "search_plugin=$SEARCH_PROVIDER,media_plugin=local_media_provider"' in install_ps1


def test_fresh_install_verifies_pdf_runtime_dependencies() -> None:
    install_sh = _read_app_file("install.sh")
    install_ps1 = _read_app_file("install.ps1")

    assert '"$VENV_DIR/bin/python" - <<' in install_sh
    assert "import PIL" in install_sh
    assert "import pypdf" in install_sh
    assert '& $VENV_PYTHON -c "import PIL; import pypdf"' in install_ps1
    assert 'throw "PDF runtime dependency check failed."' in install_ps1


def test_fresh_install_sets_verbose_off_without_planner_card_toggle() -> None:
    install_sh = _read_app_file("install.sh")
    install_ps1 = _read_app_file("install.ps1")

    assert 'echo "NULLION_ACTIVITY_TRACE_ENABLED=false"' in install_sh
    assert "NULLION_TASK_PLANNER_FEED" not in install_sh

    assert '$envLines += "NULLION_ACTIVITY_TRACE_ENABLED=false"' in install_ps1
    assert "NULLION_TASK_PLANNER_FEED" not in install_ps1
