from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import textwrap

import pytest


ROOT = Path(
    os.environ.get(
        "NULLION_APP_REPO",
        Path(__file__).resolve().parents[1],
    )
).resolve()


def _read_app_file(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def test_windows_installer_uses_venv_python_and_visible_oauth_token_file_flow() -> None:
    install_ps1 = _read_app_file("install.ps1")

    assert '$VENV_PYTHON = Join-Path $NULLION_VENV_DIR "Scripts\\python.exe"' in install_ps1
    assert "& $VENV_PYTHON -m nullion.auth --write-codex-access-token $tokenFile" in install_ps1
    assert '$env:PYTHONPATH = Join-Path $SOURCE_DIR "src"' in install_ps1
    assert "$OPENAI_KEY = $token" in install_ps1
    assert "--print-codex-access-token" not in install_ps1


def test_windows_installer_secure_storage_init_ignores_python_runtime_warnings() -> None:
    install_ps1 = _read_app_file("install.ps1")

    assert '$oldPythonWarnings = $env:PYTHONWARNINGS' in install_ps1
    assert '$previousErrorActionPreference = $ErrorActionPreference' in install_ps1
    assert '$env:PYTHONWARNINGS = "ignore"' in install_ps1
    assert '$ErrorActionPreference = "Continue"' in install_ps1
    assert "& $VENV_PYTHON -W ignore -m nullion.secure_storage --init --storage $Storage" in install_ps1
    assert "$ErrorActionPreference = $previousErrorActionPreference" in install_ps1
    assert "Remove-Item Env:\\PYTHONWARNINGS -ErrorAction SilentlyContinue" in install_ps1
    assert "$env:PYTHONWARNINGS = $oldPythonWarnings" in install_ps1


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell Core not installed")
def test_windows_installer_secure_storage_init_tolerates_native_stderr(tmp_path) -> None:
    fake_python = tmp_path / "python.exe"
    fake_python.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            echo "LangChainPendingDeprecationWarning: noisy dependency warning" >&2
            exit 0
            """
        ),
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    script = textwrap.dedent(
        f"""\
        $ErrorActionPreference = "Stop"
        $VENV_PYTHON = "{fake_python}"
        $NULLION_DIR = "{tmp_path}"
        function Write-Ok {{ param([string]$Message) Write-Output "OK:$Message" }}
        function Initialize-KeyStorage {{
            param([Parameter(Mandatory=$true)][string]$Storage)

            $env:NULLION_KEY_STORAGE = $Storage
            $outputFile = [System.IO.Path]::GetTempFileName()
            $errorFile = [System.IO.Path]::GetTempFileName()
            $oldPythonWarnings = $env:PYTHONWARNINGS
            $previousErrorActionPreference = $ErrorActionPreference
            try {{
                $env:PYTHONWARNINGS = "ignore"
                $ErrorActionPreference = "Continue"
                & $VENV_PYTHON -W ignore -m nullion.secure_storage --init --storage $Storage 1> $outputFile 2> $errorFile
                if ($LASTEXITCODE -eq 0) {{
                    Write-Ok "Local data key stored at $NULLION_DIR\\chat_history.key."
                    return
                }}
                $detail = (Get-Content $errorFile -Raw -ErrorAction SilentlyContinue).Trim()
                throw "Could not initialize local key storage: $detail"
            }} finally {{
                $ErrorActionPreference = $previousErrorActionPreference
                if ($null -eq $oldPythonWarnings) {{
                    Remove-Item Env:\\PYTHONWARNINGS -ErrorAction SilentlyContinue
                }} else {{
                    $env:PYTHONWARNINGS = $oldPythonWarnings
                }}
                Remove-Item -Force $outputFile, $errorFile -ErrorAction SilentlyContinue
            }}
        }}
        Initialize-KeyStorage "local"
        Write-Output "EAP=$ErrorActionPreference"
        """
    )

    result = subprocess.run(
        ["pwsh", "-NoProfile", "-NonInteractive", "-Command", script],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "OK:Local data key stored" in result.stdout
    assert "EAP=Stop" in result.stdout


def test_windows_installer_keeps_telegram_bot_token_hidden_and_out_of_urls() -> None:
    install_ps1 = _read_app_file("install.ps1")

    assert 'Read-Host "  Paste your bot token here (hidden)" -AsSecureString' in install_ps1
    assert "https://api.telegram.org/bot$BOT_TOKEN/getUpdates" not in install_ps1
    assert "getUpdates" not in install_ps1
    assert "search for @userinfobot" in install_ps1
    assert "Existing token found: ${preview}..." not in install_ps1
    assert "Existing token found: $(Format-MaskedSecret $ExistingToken)" in install_ps1


def test_windows_installer_checks_out_install_target_instead_of_shallow_main() -> None:
    install_ps1 = _read_app_file("install.ps1")

    assert "function Checkout-InstallTarget" in install_ps1
    assert '& git -C $SourceDir fetch --quiet --prune --prune-tags --force origin "refs/tags/*:refs/tags/*"' in install_ps1
    assert 'if ($script:NULLION_VERSION -eq "main")' in install_ps1
    assert "& git -C $SourceDir reset --quiet --hard origin/main" in install_ps1
    assert 'git -C $SourceDir describe --tags --abbrev=0 --match "v[0-9]*" origin/main' in install_ps1
    assert "& git -C $SourceDir reset --quiet --hard $latestTag" in install_ps1
    assert "$script:NULLION_VERSION = $latestTag.Substring(1)" in install_ps1
    assert "git clone --depth 1 $REPO_URL $SOURCE_DIR" not in install_ps1


def test_windows_installer_offers_full_fresh_reset_for_bad_existing_install() -> None:
    install_ps1 = _read_app_file("install.ps1")

    assert "function Get-ExistingInstallHealthIssues" in install_ps1
    assert "function Invoke-ExistingInstallFreshResetOffer" in install_ps1
    assert "NULLION_FRESH_INSTALL" in install_ps1
    assert "NULLION_RESET_INSTALL" in install_ps1
    assert "Start completely fresh and let setup clean this machine first?" in install_ps1
    assert "The virtual environment exists but Scripts\\python.exe is missing." in install_ps1
    assert "The app source folder exists but pyproject.toml is missing." in install_ps1
    assert "The runtime database exists but is empty." in install_ps1
    assert "Invoke-ExistingInstallFreshResetOffer" in install_ps1


def test_windows_installer_fresh_reset_cleans_installer_managed_state() -> None:
    install_ps1 = _read_app_file("install.ps1")

    assert "function Reset-NullionInstallState" in install_ps1
    assert "Remove-NullionScheduledTasks" in install_ps1
    assert "Stop-NullionProcesses" in install_ps1
    assert 'Remove-PathIfExists $NULLION_DIR "Nullion install directory"' in install_ps1
    assert 'Join-Path $env:LOCALAPPDATA "Nullion"' in install_ps1
    assert 'Join-Path $env:APPDATA "Nullion"' in install_ps1
    assert "Remove-NullionTempFiles" in install_ps1
    assert '"nullion-*"' in install_ps1
    assert '"/Delete", "/TN", $task, "/F"' in install_ps1


def test_windows_installer_dependency_failure_can_reset_and_restart_once() -> None:
    install_ps1 = _read_app_file("install.ps1")

    assert "function Invoke-FreshResetAfterInstallFailure" in install_ps1
    assert "NULLION_FRESH_RESET_ALREADY_TRIED" in install_ps1
    assert "nullion-reset-restart-{0}.ps1" in install_ps1
    assert "Copy-Item -LiteralPath $currentPath -Destination $restartPath" in install_ps1
    assert "Clean the old install completely and restart setup?" in install_ps1
    assert "Restarting installer after cleanup..." in install_ps1
    assert "& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $restartPath" in install_ps1
    assert "Invoke-FreshResetAfterInstallFailure $_.Exception.Message" in install_ps1
    assert 'throw "Nullion dependency install failed."' in install_ps1
    assert 'throw "PDF runtime dependency check failed."' in install_ps1


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell Core not installed")
def test_windows_installer_oauth_flow_keeps_device_code_visible_while_capturing_token(tmp_path) -> None:
    fake_venv = tmp_path / "venv"
    fake_scripts = fake_venv / "Scripts"
    fake_scripts.mkdir(parents=True)
    fake_python = fake_scripts / "python.exe"
    fake_python.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            token_file="${@: -1}"
            echo "  1. Open: https://auth.openai.com/codex/device"
            echo "  2. Enter code: TEST-CODE"
            printf 'test-access-token' > "$token_file"
            """
        ),
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    script = textwrap.dedent(
        f"""\
        $ErrorActionPreference = "Stop"
        $NULLION_VENV_DIR = "{fake_venv}"
        $SOURCE_DIR = "{tmp_path}"
        $VENV_PYTHON = Join-Path $NULLION_VENV_DIR "Scripts\\python.exe"
        $OPENAI_KEY = ""
        $MODEL_PROVIDER = "openai"
        $tokenFile = [System.IO.Path]::GetTempFileName()
        $oldPythonPath = $env:PYTHONPATH
        try {{
            $env:PYTHONPATH = Join-Path $SOURCE_DIR "src"
            & $VENV_PYTHON -m nullion.auth --write-codex-access-token $tokenFile
            if ($LASTEXITCODE -eq 0 -and (Test-Path $tokenFile)) {{
                $token = (Get-Content -Raw $tokenFile).Trim()
                if ($token) {{
                    $MODEL_PROVIDER = "codex"
                    $OPENAI_KEY = $token
                }}
            }}
        }} finally {{
            if ($null -eq $oldPythonPath) {{
                Remove-Item Env:\\PYTHONPATH -ErrorAction SilentlyContinue
            }} else {{
                $env:PYTHONPATH = $oldPythonPath
            }}
            Remove-Item -Force $tokenFile -ErrorAction SilentlyContinue
        }}
        Write-Output "TOKEN=$OPENAI_KEY"
        Write-Output "PROVIDER=$MODEL_PROVIDER"
        """
    )

    result = subprocess.run(
        ["pwsh", "-NoProfile", "-NonInteractive", "-Command", script],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "Enter code: TEST-CODE" in result.stdout
    assert "TOKEN=test-access-token" in result.stdout
    assert "PROVIDER=codex" in result.stdout


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell Core not installed")
def test_windows_installer_script_parses() -> None:
    subprocess.run(
        [
            "pwsh",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "$null = [scriptblock]::Create((Get-Content -Raw install.ps1)); 'ok'",
        ],
        text=True,
        capture_output=True,
        check=True,
    )
