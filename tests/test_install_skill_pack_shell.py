from __future__ import annotations

import subprocess


def _skill_pack_function_block() -> str:
    install_sh = open("install.sh", encoding="utf-8").read()
    start = install_sh.index('ENABLED_SKILL_PACKS=""')
    end = install_sh.index("install_custom_skill_pack_now()")
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
    install_sh = open("install.sh", encoding="utf-8").read()
    install_ps1 = open("install.ps1", encoding="utf-8").read()

    assert 'SKILL_CHOICES="1,2,3,4,5,6,7,8,9"' in install_sh
    assert '4) add_skill_pack "nullion/pdf-documents"' in install_sh
    assert 'return @("1", "2", "3", "4", "5", "6", "7", "8", "9")' in install_ps1
    assert '"4" { Add-SkillPackChoice "nullion/pdf-documents" }' in install_ps1


def test_fresh_install_provisions_media_and_browser_runtime_by_default() -> None:
    install_sh = open("install.sh", encoding="utf-8").read()
    install_ps1 = open("install.ps1", encoding="utf-8").read()

    assert 'install_playwright_runtime || true' in install_sh
    assert 'install_default_local_media_runtime' in install_sh
    assert 'ENABLED_PLUGINS="search_plugin,browser_plugin,workspace_plugin,media_plugin"' in install_sh
    assert 'PROVIDER_BINDINGS="search_plugin=${SEARCH_PROVIDER},media_plugin=local_media_provider"' in install_sh

    assert "[void](Install-PlaywrightRuntime)" in install_ps1
    assert "Install-DefaultLocalMediaRuntime" in install_ps1
    assert '$enabledPlugins = "search_plugin,browser_plugin,workspace_plugin,media_plugin"' in install_ps1
    assert '$providerBindings = "search_plugin=$SEARCH_PROVIDER,media_plugin=local_media_provider"' in install_ps1


def test_fresh_install_verifies_pdf_runtime_dependencies() -> None:
    install_sh = open("install.sh", encoding="utf-8").read()
    install_ps1 = open("install.ps1", encoding="utf-8").read()

    assert '"$VENV_DIR/bin/python" - <<' in install_sh
    assert "import PIL" in install_sh
    assert "import pypdf" in install_sh
    assert '& $VENV_PYTHON -c "import PIL; import pypdf"' in install_ps1
    assert 'throw "PDF runtime dependency check failed."' in install_ps1


def test_fresh_install_sets_verbose_full_by_default() -> None:
    install_sh = open("install.sh", encoding="utf-8").read()
    install_ps1 = open("install.ps1", encoding="utf-8").read()

    assert 'echo "NULLION_ACTIVITY_TRACE_ENABLED=true"' in install_sh
    assert 'echo "NULLION_TASK_PLANNER_FEED_MODE=task"' in install_sh
    assert 'echo "NULLION_TASK_PLANNER_FEED_ENABLED=true"' in install_sh

    assert '$envLines += "NULLION_ACTIVITY_TRACE_ENABLED=true"' in install_ps1
    assert '$envLines += "NULLION_TASK_PLANNER_FEED_MODE=task"' in install_ps1
    assert '$envLines += "NULLION_TASK_PLANNER_FEED_ENABLED=true"' in install_ps1
