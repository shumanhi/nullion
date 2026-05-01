from __future__ import annotations

import shutil
import subprocess
import textwrap

import pytest


def test_windows_installer_uses_venv_python_and_visible_oauth_token_file_flow() -> None:
    install_ps1 = open("install.ps1", encoding="utf-8").read()

    assert '$VENV_PYTHON = Join-Path $NULLION_VENV_DIR "Scripts\\python.exe"' in install_ps1
    assert "& $VENV_PYTHON -m nullion.auth --write-codex-access-token $tokenFile" in install_ps1
    assert '$env:PYTHONPATH = Join-Path $SOURCE_DIR "src"' in install_ps1
    assert "$OPENAI_KEY = $token" in install_ps1
    assert "--print-codex-access-token" not in install_ps1


def test_windows_installer_keeps_telegram_bot_token_hidden_and_out_of_urls() -> None:
    install_ps1 = open("install.ps1", encoding="utf-8").read()

    assert 'Read-Host "  Paste your bot token here (hidden)" -AsSecureString' in install_ps1
    assert "https://api.telegram.org/bot$BOT_TOKEN/getUpdates" not in install_ps1
    assert "https://api.telegram.org/bot<your-bot-token>/getUpdates" in install_ps1
    assert "Existing token found: ${preview}..." not in install_ps1
    assert "Existing token found: $(Format-MaskedSecret $ExistingToken)" in install_ps1


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
