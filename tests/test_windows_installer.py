from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "install.ps1"
SHELL_INSTALLER = ROOT / "install.sh"
PYPROJECT = ROOT / "pyproject.toml"


def _installer_text() -> str:
    return INSTALLER.read_text(encoding="utf-8")


def test_python_support_range_excludes_python_314_for_windows_dependencies() -> None:
    text = PYPROJECT.read_text(encoding="utf-8")

    assert 'requires-python = ">=3.11,<3.14"' in text


def test_windows_installer_is_ascii_safe_for_windows_powershell() -> None:
    data = INSTALLER.read_bytes()

    assert data.decode("ascii")


def test_windows_installer_documents_and_uses_iex_safe_script_path_detection() -> None:
    text = _installer_text()

    assert "piped through `irm ... | iex`" in text
    assert "$scriptPath = $null" in text
    assert '$MyInvocation.MyCommand.PSObject.Properties.Name -contains "Path"' in text
    assert "$SCRIPT_DIR -and (Test-Path" in text


def test_windows_installer_self_refreshes_remote_or_piped_runs() -> None:
    text = _installer_text()
    start = text.index("function Get-InstallerScriptPath")
    end = text.index("function Refresh-ProcessPath", start)
    refresh = text[start:end]

    assert "function Invoke-InstallerSelfRefresh" in refresh
    assert "NULLION_INSTALLER_SELF_REFRESHED" in refresh
    assert "NULLION_INSTALLER_NO_SELF_REFRESH" in refresh
    assert "Test-LocalInstallerSource $currentPath" in refresh
    assert "raw.githubusercontent.com/shumanhi/nullion/main/install.ps1?cb=" in refresh
    assert '"Cache-Control"="no-cache"' in refresh
    assert "powershell.exe -NoProfile -ExecutionPolicy Bypass -File $freshPath" in refresh
    assert "\nInvoke-InstallerSelfRefresh\n" in refresh


def test_windows_installer_avoids_unsupported_python_314_launcher_fallback() -> None:
    text = _installer_text()

    assert "Python 3.11-3.13" in text
    assert 'foreach ($minor in @("13","12","11"))' in text
    assert '@("py", "-3.$minor")' in text
    assert "py -3 --version" not in text


def test_windows_installer_bootstraps_pip_through_venv_python() -> None:
    text = _installer_text()

    assert "& $VENV_PYTHON -m ensurepip --upgrade" in text
    assert "& $VENV_PYTHON -m pip install --quiet --no-cache-dir --upgrade pip" in text
    assert "& $VENV_PYTHON -m pip install --quiet --no-cache-dir -e $SOURCE_DIR" in text
    assert '$PIP = Join-Path $NULLION_VENV_DIR "Scripts\\pip.exe"' not in text
    assert "& $PIP install" not in text


def test_windows_installer_uses_one_path_refresh_helper() -> None:
    text = _installer_text()

    assert "function Refresh-ProcessPath" in text
    assert '$machinePath = [System.Environment]::GetEnvironmentVariable("Path", "Machine")' in text
    assert '$userPath = [System.Environment]::GetEnvironmentVariable("Path", "User")' in text
    assert 'GetEnvironmentVariable("Path","Machine")' not in text
    assert 'GetEnvironmentVariable("Path","User")' not in text
    assert text.count("Refresh-ProcessPath") >= 8


def test_windows_installer_initializes_playwright_runtime_flag() -> None:
    text = _installer_text()

    assert "$PLAYWRIGHT_RUNTIME_READY = $false" in text
    assert "if ($script:PLAYWRIGHT_RUNTIME_READY) { return $true }" in text
    assert "$pwPython = Join-Path $NULLION_VENV_DIR" in text
    assert "& $pwPython -m pip install --quiet --no-cache-dir playwright" in text
    assert "$pwPip" not in text


def test_windows_installer_honors_main_install_target() -> None:
    text = _installer_text()

    assert "function Checkout-InstallTarget" in text
    assert '$script:NULLION_VERSION -eq "main"' in text
    assert "git -C $SourceDir reset --quiet --hard origin/main" in text
    assert "Checked out main." in text
    assert "Checkout-LatestRelease" not in text


def test_windows_installer_recreates_unsupported_existing_venv() -> None:
    text = _installer_text()

    assert "Existing virtual environment uses unsupported Python" in text
    assert "Existing virtual environment is incomplete. Recreating it." in text
    assert "Remove-Item -Recurse -Force $NULLION_VENV_DIR" in text
    assert "Test-SupportedPythonVersionText $venvVersion" in text


def test_windows_installer_uses_defined_install_dir_for_runtime_db() -> None:
    text = _installer_text()

    assert "$NULLION_INSTALL_DIR" not in text
    assert "$NULLION_DIR\\runtime.db" in text


def test_telegram_chat_id_setup_uses_user_info_bot_not_api_url() -> None:
    windows_text = _installer_text()
    shell_text = SHELL_INSTALLER.read_text(encoding="utf-8")

    for text in (windows_text, shell_text):
        assert "@userinfobot" in text
        assert "numeric Id/User ID" in text
        assert "getUpdates" not in text
        assert "api.telegram.org/bot<your-bot-token>" not in text


def test_windows_installer_checkpoints_messaging_before_provider_setup() -> None:
    text = _installer_text()

    checkpoint_call = text.index("\nSave-MessagingCheckpoint\n")
    provider_setup = text.index("# Model provider")
    final_env_write = text.index("# Write .env")

    assert "function Set-EnvValue" in text
    assert "function Save-MessagingCheckpoint" in text
    assert checkpoint_call < provider_setup < final_env_write
    assert '"NULLION_SETUP_MESSAGING_DONE=true"' in text


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell is not installed")
def test_windows_messaging_checkpoint_persists_telegram_values(tmp_path: Path) -> None:
    text = _installer_text()
    start = text.index("function Set-EnvValue")
    end = text.index("function Format-MaskedSecret", start)
    helpers = text[start:end]
    env_dir = str(tmp_path).replace("'", "''")
    command = f"""
Set-StrictMode -Version Latest
function Write-Ok {{ param([string]$Text) }}
$NULLION_DIR = '{env_dir}'
$NULLION_ENV_FILE = Join-Path $NULLION_DIR '.env'
$NULLION_WEB_PORT = 8742
$TELEGRAM_ENABLED = $true
$BOT_TOKEN = '123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ'
$CHAT_ID = '123456789'
$SLACK_ENABLED = $false
$SLACK_BOT_TOKEN = ''
$SLACK_APP_TOKEN = ''
$SLACK_SIGNING_SECRET = ''
$SLACK_OPERATOR_USER_ID = ''
$DISCORD_ENABLED = $false
$DISCORD_BOT_TOKEN = ''
$SKIP_MESSAGING_SETUP = $false
{helpers}
Save-MessagingCheckpoint
Get-Content $NULLION_ENV_FILE -Raw
"""

    result = subprocess.run(
        ["pwsh", "-NoProfile", "-Command", command],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert 'NULLION_TELEGRAM_BOT_TOKEN="123456:ABCDEFGHIJKLMNOPQRSTUVWXYZ"' in result.stdout
    assert 'NULLION_TELEGRAM_OPERATOR_CHAT_ID="123456789"' in result.stdout
    assert "NULLION_TELEGRAM_CHAT_ENABLED=true" in result.stdout
    assert "NULLION_SETUP_MESSAGING_DONE=true" in result.stdout


def test_windows_installer_does_not_install_local_media_before_media_choice() -> None:
    text = _installer_text()

    media_setup = text.index("# Local media tools setup")
    media_prompt = text.index('Confirm-Prompt "Configure media tools now?"')
    first_local_install = text.index("Ensure-WhisperCppRuntime", media_prompt)
    skip_message = text.index("Skipped media tools", media_prompt)
    checkpoint_call = text.index("\nSave-MediaCheckpoint\n", media_prompt)

    assert "Installing default local media runtime so you can switch" not in text
    assert "Local media packages will only be installed if you choose local audio/OCR setup." in text
    assert media_setup < media_prompt < first_local_install
    assert skip_message < checkpoint_call


def test_windows_installer_checkpoints_media_setup() -> None:
    text = _installer_text()

    assert "function Save-MediaCheckpoint" in text
    assert "Save-PluginCheckpoint" in text
    assert 'Set-EnvValue "NULLION_SETUP_MEDIA_DONE" "true" -Raw' in text
    assert '"NULLION_SETUP_MEDIA_DONE=true"' in text


def test_windows_installer_checkpoints_each_setup_section_before_final_env() -> None:
    text = _installer_text()
    final_env_write = text.index("# Write .env")
    checkpoints = {
        "provider": ("function Save-ProviderCheckpoint", "\nSave-ProviderCheckpoint\n", "# Browser setup"),
        "browser": ("function Save-BrowserCheckpoint", "\nSave-BrowserCheckpoint\n", "# Search provider setup"),
        "search": ("function Save-SearchCheckpoint", "\nSave-SearchCheckpoint\n", "# Account / API tools setup"),
        "account": ("function Save-AccountCheckpoint", "\nSave-AccountCheckpoint\n", "# Local media tools setup"),
        "media": ("function Save-MediaCheckpoint", "\nSave-MediaCheckpoint\n", "# Skill pack setup"),
        "skills": ("function Save-SkillCheckpoint", "\nSave-SkillCheckpoint\n", "# Write .env"),
    }

    for marker, call, next_section in checkpoints.values():
        assert marker in text
        call_index = text.index(call)
        assert call_index < text.index(next_section)
        assert call_index < final_env_write

    for key in (
        "NULLION_SETUP_PROVIDER_DONE",
        "NULLION_SETUP_BROWSER_DONE",
        "NULLION_SETUP_SEARCH_DONE",
        "NULLION_SETUP_ACCOUNT_DONE",
        "NULLION_SETUP_MEDIA_DONE",
        "NULLION_SETUP_SKILLS_DONE",
    ):
        assert f'Set-EnvValue "{key}" "true" -Raw' in text


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell is not installed")
def test_windows_provider_checkpoint_persists_model_values(tmp_path: Path) -> None:
    text = _installer_text()
    start = text.index("function Set-EnvValue")
    end = text.index("function Format-MaskedSecret", start)
    helpers = text[start:end]
    env_dir = str(tmp_path).replace("'", "''")
    command = f"""
Set-StrictMode -Version Latest
function Write-Ok {{ param([string]$Text) }}
$NULLION_DIR = '{env_dir}'
$NULLION_ENV_FILE = Join-Path $NULLION_DIR '.env'
$ANTHROPIC_KEY = ''
$OPENAI_KEY = 'sk-test'
$MODEL_PROVIDER = 'openai'
$MODEL_BASE_URL = ''
$MODEL_NAME = 'gpt-5.5'
{helpers}
Save-ProviderCheckpoint
Get-Content $NULLION_ENV_FILE -Raw
"""

    result = subprocess.run(
        ["pwsh", "-NoProfile", "-Command", command],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert "NULLION_SETUP_PROVIDER_DONE=true" in result.stdout
    assert 'OPENAI_API_KEY="sk-test"' in result.stdout
    assert 'NULLION_MODEL_PROVIDER="openai"' in result.stdout
    assert 'NULLION_MODEL="gpt-5.5"' in result.stdout


def test_windows_skill_pack_picker_avoids_cursor_positioning() -> None:
    text = _installer_text()
    start = text.index("function Request-SkillPackChoices")
    end = text.index("$ExistingSkillPacks", start)
    picker = text[start:end]

    assert "SetCursorPosition" not in picker
    assert "CursorTop" not in picker
    assert "CursorVisible" not in picker
    assert "ReadKey" not in picker
    assert "Skill packs [1,2,3,4,5,6,7,8,9]" in picker
    assert '"1,2,3,4,5,6,7,8,9"' in picker


def test_windows_runtime_finalization_json_keys_are_quoted() -> None:
    text = _installer_text()
    start = text.index("function Invoke-NullionRuntimeFinalization")
    end = text.index("function Get-StoredCredentialValue", start)
    finalization = text[start:end]

    assert '{"ok": True, "warnings": details.get("warnings") or []}' in finalization
    assert "{ok: True" not in finalization
    assert "details.get(warnings)" not in finalization


def test_windows_schtasks_failures_do_not_abort_installer() -> None:
    text = _installer_text()
    start = text.index("function Invoke-SchtasksCommand")
    end = text.index("function Read-ModelName", start)
    helper = text[start:end]
    autostart = text[text.index("# Register with schtasks") : text.index("if (-not $AUTOSTART_CONFIGURED)")]

    assert '$ErrorActionPreference = "Continue"' in helper
    assert "catch {" in helper
    assert "[pscustomobject]@{" in helper
    assert "$result = Invoke-SchtasksCommand $taskArgs" in autostart
    assert "$trayResult = Invoke-SchtasksCommand $trayTaskArgs" in autostart
    assert "$telegramResult = Invoke-SchtasksCommand $telegramTaskArgs" in autostart
    assert "$slackResult = Invoke-SchtasksCommand $slackTaskArgs" in autostart
    assert "$discordResult = Invoke-SchtasksCommand $discordTaskArgs" in autostart
    assert " = schtasks @" not in autostart


def test_windows_env_acl_hardening_is_best_effort() -> None:
    text = _installer_text()
    start = text.index("function Protect-EnvFile")
    end = text.index("function Format-BoolText", start)
    helper = text[start:end]
    final_env = text[text.index("# Write .env") :]

    assert "function Protect-EnvFile" in text
    assert "Set-Acl -Path $NULLION_ENV_FILE -AclObject $acl" in helper
    assert "catch {" in helper
    assert "Setup can continue" in helper
    assert "\nProtect-EnvFile\n" in final_env
    assert "\nSet-Acl $NULLION_ENV_FILE $acl" not in text


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell is not installed")
def test_windows_installer_parses_with_powershell() -> None:
    command = (
        "$tokens=$null; $errors=$null; "
        f"[System.Management.Automation.Language.Parser]::ParseFile('{INSTALLER}', [ref]$tokens, [ref]$errors) | Out-Null; "
        "if ($errors.Count) { $errors | ForEach-Object { $_.Message }; exit 1 }"
    )

    result = subprocess.run(
        ["pwsh", "-NoProfile", "-Command", command],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell is not installed")
def test_windows_installer_path_detection_survives_iex_style_scriptblock() -> None:
    text = _installer_text()
    start = text.index("$scriptPath = $null")
    end = text.index("$SOURCE_DIR = $null", start)
    path_detection = text[start:end]
    command = (
        "Set-StrictMode -Version Latest; "
        f"$sb = {{ {path_detection} if ($SCRIPT_DIR -and (Test-Path (Join-Path $SCRIPT_DIR 'pyproject.toml'))) "
        "{ 'local' } else { 'remote' } }; "
        "& $sb"
    )

    result = subprocess.run(
        ["pwsh", "-NoProfile", "-Command", command],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert result.stdout.strip() == "remote"
