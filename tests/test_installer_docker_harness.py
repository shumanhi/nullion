from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import textwrap

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_linux_installer_docker_harness_runs_real_shell_installer() -> None:
    script = (ROOT / "scripts" / "installer-docker-linux.sh").read_text(encoding="utf-8")
    dockerfile = (ROOT / "docker" / "installer-linux.Dockerfile").read_text(encoding="utf-8")

    assert "docker/installer-linux.Dockerfile" in script
    assert "bash ./install.sh" in script
    assert "NULLION_INSTALLER_SMOKE_TIMEOUT" in script
    assert "timeout \"$NULLION_INSTALLER_SMOKE_TIMEOUT\"" in script
    assert "expect <<" in script
    assert "spawn bash ./install.sh" in script
    assert "NULLION_VERSION=\"${NULLION_VERSION:-main}\"" in script
    assert "NULLION_MODEL_PROVIDER=\\\"ollama\\\"" in script
    assert "nullion-web\" --help" in script
    assert "nullion-tray\" --help" in script
    assert "nullion-webview\" --help" in script
    assert "nullion-telegram\" --help" in script
    assert "nullion-slack\" --help" in script
    assert "nullion-discord\" --help" in script
    assert '"$HOME/.nullion/venv/bin/nullion" uninstall --yes' in script
    assert '"$HOME/.nullion/webview.pid"' in script
    assert "test ! -d \"$HOME/.nullion\"" in script
    assert "run_installer y" in script
    final_prompt_block = script[script.index('"Enter confirms the checked items."') :]
    assert '"Open Nullion in your browser now?"' in final_prompt_block
    assert 'send "$open_answer\\r"' in final_prompt_block
    assert "exp_continue" in final_prompt_block
    assert "installer-linux smoke timed out waiting for final launch prompts" in final_prompt_block
    assert "http://127.0.0.1:8742/api/health" in script
    assert "Nullion web service did not become healthy after installer launch." in script
    assert "assert_update_rollback_smoke" in script
    assert '"$HOME/.nullion/venv/bin/nullion" update --hash' in script
    assert "broken update target" in script
    assert "Rolled back to previous version automatically|Rollback successful" in script
    assert "git_commit.txt" in script
    assert "FROM ubuntu:24.04" in dockerfile
    assert "expect" in dockerfile
    assert "python3-venv" in dockerfile
    assert "sudo" in dockerfile


def test_windows_installer_docker_harness_runs_real_powershell_installer() -> None:
    script = (ROOT / "scripts" / "installer-docker-windows.ps1").read_text(encoding="utf-8")
    dockerfile = (ROOT / "docker" / "installer-windows.Dockerfile").read_text(encoding="utf-8")

    assert "docker/installer-windows.Dockerfile" in script
    assert "function Wait-DockerDaemon" in script
    assert "Start-Service docker" in script
    assert "Docker daemon did not become available within ${TimeoutSeconds}s." in script
    assert "installer-smoke.ps1" in script
    assert "Get-Content -Raw $smokeScript | docker run --rm -i" in script
    assert "powershell.exe -NoProfile -ExecutionPolicy Bypass -Command -" in script
    assert "powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\\install.ps1" in script
    assert "if ($LASTEXITCODE -ne 0)" in script
    assert '$env:NULLION_VERSION = "main"' in script
    assert '$env:NULLION_INSTALLER_SMOKE = "true"' in script
    assert 'NULLION_MODEL_PROVIDER="ollama"' in script
    assert "nullion-web.exe" in script
    assert "nullion-tray.exe" in script
    assert "nullion-webview.exe" in script
    assert "nullion-telegram.exe" in script
    assert "nullion-slack.exe" in script
    assert "nullion-discord.exe" in script
    assert 'nullion.exe") uninstall --yes' in script
    assert '"webview.pid")' in script
    assert "function Complete-ScheduledUninstallCleanup" in script
    assert "Complete-ScheduledUninstallCleanup -InstallDir $installDir" in script
    assert "Remove-Item -LiteralPath $InstallDir -Recurse -Force -ErrorAction Stop" in script
    assert "NULLION_FRESH_INSTALL" in script
    assert "stale-reinstall-marker.txt" in script
    assert "Fresh reinstall reset did not remove stale install marker." in script
    assert "Start-Process" in script
    assert "Stop-InstalledServices" in script
    assert 'Get-Process -Name "nullion-web", "nullion-tray"' in script
    assert "Invoke-SmokeStep" in script
    assert "Invoke-EntrypointHelp" in script
    entrypoint_help = script[script.index("function Invoke-EntrypointHelp") : script.index("function Import-NullionEnv")]
    assert '$env:PYTHONWARNINGS = "ignore"' in entrypoint_help
    assert "Start-Process -FilePath $Path -ArgumentList \"--help\"" in entrypoint_help
    assert "-RedirectStandardOutput $outputFile -RedirectStandardError $errorFile" in entrypoint_help
    assert "if ($process.ExitCode -ne 0)" in entrypoint_help
    assert "Remove-Item -Force $outputFile, $errorFile" in entrypoint_help
    assert "function Invoke-EntrypointUpdateSmoke" in script
    assert "function Invoke-UpdateRollbackSmoke" in script
    assert "function Invoke-LockedEntrypointUpdateSmoke" in script
    assert "Assert-NoLockedEntrypointPipFailure" in script
    assert "WinError 32|\\.deleteme|process cannot access the file" in script
    assert "[System.IO.FileShare]::None" in script
    assert "locked entrypoint update target" in script
    assert "update through installed nullion.exe" in script
    assert "update while nullion.exe is locked" in script
    assert '"nullion.exe") update --hash' in script
    assert "& $pythonExe -m nullion.cli update --hash" in script
    assert '$ErrorActionPreference = "Continue"' in script
    assert "$PSNativeCommandUseErrorActionPreference = $false" in script
    assert "broken update target" in script
    assert "Rolled back to previous version automatically|Rollback successful" in script
    assert "git_commit.txt" in script
    assert 'Invoke-InstallerSmokeAnswers -OpenAnswer "n"' in script
    assert "Invoke-RestMethod -Uri \"http://127.0.0.1:8742/api/health\"" in script
    assert "Nullion web service did not become healthy after installer smoke launch." in script
    assert "FROM python:3.11-windowsservercore-ltsc2022" in dockerfile
    assert "MinGit-2.54.0-64-bit.zip" in dockerfile
    assert "Set-Variable -Name ProgressPreference -Value SilentlyContinue" in dockerfile
    assert "C:\\mingit\\cmd;C:\\mingit\\usr\\bin" in dockerfile
    assert "C:\\Python;C:\\Python\\Scripts" in dockerfile
    assert "C:\\Windows\\System32\\WindowsPowerShell\\v1.0" in dockerfile
    assert 'SHELL ["powershell.exe"' in dockerfile


@pytest.mark.skipif(shutil.which("pwsh") is None, reason="PowerShell Core not installed")
def test_windows_entrypoint_help_probe_ignores_successful_native_stderr(tmp_path) -> None:
    fake_entrypoint = tmp_path / "nullion.exe"
    fake_entrypoint.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            echo "LangChainPendingDeprecationWarning: noisy dependency warning" >&2
            echo "usage: nullion"
            exit 0
            """
        ),
        encoding="utf-8",
    )
    fake_entrypoint.chmod(0o755)
    script = textwrap.dedent(
        f"""\
        $ErrorActionPreference = "Stop"
        function Invoke-EntrypointHelp {{
            param([string]$Path)
            $oldPythonWarnings = $env:PYTHONWARNINGS
            $outputFile = [System.IO.Path]::GetTempFileName()
            $errorFile = [System.IO.Path]::GetTempFileName()
            try {{
                $env:PYTHONWARNINGS = "ignore"
                $process = Start-Process -FilePath $Path -ArgumentList "--help" -Wait -PassThru -NoNewWindow -RedirectStandardOutput $outputFile -RedirectStandardError $errorFile
                if ($process.ExitCode -ne 0) {{
                    Get-Content $outputFile -ErrorAction SilentlyContinue | Write-Host
                    Get-Content $errorFile -ErrorAction SilentlyContinue | Write-Host
                    throw "Entrypoint help failed for $Path with exit code $($process.ExitCode)"
                }}
            }} finally {{
                if ($null -eq $oldPythonWarnings) {{
                    Remove-Item Env:\\PYTHONWARNINGS -ErrorAction SilentlyContinue
                }} else {{
                    $env:PYTHONWARNINGS = $oldPythonWarnings
                }}
                Remove-Item -Force $outputFile, $errorFile -ErrorAction SilentlyContinue
            }}
        }}
        Invoke-EntrypointHelp "{fake_entrypoint}"
        Write-Output "probe-ok"
        """
    )

    result = subprocess.run(
        ["pwsh", "-NoProfile", "-NonInteractive", "-Command", script],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "probe-ok" in result.stdout


def test_installer_workflow_runs_all_platform_harnesses() -> None:
    import yaml
    workflow = yaml.load((ROOT / '.github/workflows/pr-suite.yml').read_text(), Loader=yaml.BaseLoader)
    expected = {'linux-installer': ('ubuntu-latest', './scripts/installer-docker-linux.sh'),
                'windows-installer': ('windows-2022', '.\\scripts\\installer-docker-windows.ps1'),
                'macos-installer': ('macos-14', './scripts/installer-macos.sh')}
    for key, (runner, command) in expected.items():
        job = workflow['jobs'][key]
        assert job['runs-on'] == runner
        assert int(job['timeout-minutes']) == 90
        assert any(step.get('run') == command for step in job['steps'])
        assert sum(step.get('uses', '').startswith('actions/checkout@') for step in job['steps']) == 1


def test_docker_harnesses_copy_worktrees_without_host_git_metadata() -> None:
    compose = (ROOT / 'docker-compose.e2e.yml').read_text()
    runner = (ROOT / 'scripts/run-container-tests.sh').read_text()
    linux = (ROOT / 'scripts/installer-docker-linux.sh').read_text()
    assert '.:/inputs/suite:ro' in compose
    assert ':/inputs/app:ro' in compose
    assert 'cp -a /inputs/app/. /tmp/nullion-src/' in runner
    assert 'rm -rf /tmp/nullion-src/.git' in runner
    assert 'rm -rf /tmp/nullion-work/nullion-src/.git' in linux


def test_app_pre_push_uses_container_isolated_home() -> None:
    hook = (ROOT / '.githooks/pre-push').read_text()
    compose = (ROOT / 'docker-compose.e2e.yml').read_text()
    assert 'scripts/test-docker.sh' in hook
    assert 'scripts/e2e-docker.sh' in hook
    assert 'HOME: /tmp/nullion-home' in compose
    assert 'NULLION_HOME: /tmp/nullion-home/.nullion' in compose
    assert 'NULLION_ENV_FILE: /tmp/nullion-home/.nullion/.env' in compose
    assert 'NULLION_TEST_REPO_SSH_KEY' not in hook


def test_macos_installer_harness_runs_real_shell_installer_without_docker() -> None:
    script = (ROOT / "scripts" / "installer-macos.sh").read_text(encoding="utf-8")

    assert '[[ "$(uname -s)" != "Darwin" ]]' in script
    assert "NULLION_INSTALLER_SMOKE_TIMEOUT_SECONDS" in script
    assert "NULLION_INSTALLER_SMOKE=\"true\"" in script
    assert "set timeout $env(NULLION_INSTALLER_SMOKE_TIMEOUT_SECONDS)" in script
    assert "mktemp -d" in script
    assert "export HOME=" in script
    assert "export NULLION_HOME=" in script
    assert 'cat > "${FAKE_BIN}/launchctl"' in script
    assert "Never let this isolated smoke test control host services." in script
    assert "NULLION_INSTALLER_SMOKE_PORT" in script
    assert '--port "${WEB_PORT}"' in script
    assert 'http://127.0.0.1:${WEB_PORT}/api/health' in script
    assert "spawn bash ./install.sh" in script
    assert "Set up auto-start at login?" in script
    assert "Open Nullion in your browser now?" in script
    assert 'send "$env(OPEN_ANSWER)\\r"' in script
    assert "nullion-web\" \\" in script
    assert "--host 127.0.0.1" in script
    assert "Nullion web service did not become healthy after installer smoke launch." in script
    assert "nullion-tray\" --help" in script
    assert "assert_update_rollback_smoke" in script
    assert '"$HOME/.nullion/venv/bin/nullion" update --hash' in script
    assert "broken update target" in script
    assert "Rolled back to previous version automatically|Rollback successful" in script
    assert "git_commit.txt" in script
    assert '"$HOME/.nullion/venv/bin/nullion" uninstall --yes' in script
    assert '"$HOME/.nullion/webview.pid"' in script
    assert "test ! -d \"$HOME/.nullion\"" in script
    assert "docker " not in script.lower()


def test_installer_smokes_cover_update_rollback_on_each_os() -> None:
    linux = (ROOT / "scripts" / "installer-docker-linux.sh").read_text(encoding="utf-8")
    windows = (ROOT / "scripts" / "installer-docker-windows.ps1").read_text(encoding="utf-8")
    macos = (ROOT / "scripts" / "installer-macos.sh").read_text(encoding="utf-8")

    assert "assert_update_rollback_smoke" in linux
    assert '"$HOME/.nullion/venv/bin/nullion" uninstall --yes' in linux
    assert "Invoke-UpdateRollbackSmoke" in windows
    assert "Invoke-EntrypointUpdateSmoke" in windows
    assert "Invoke-LockedEntrypointUpdateSmoke" in windows
    assert "WinError 32|\\.deleteme|process cannot access the file" in windows
    assert "assert_update_rollback_smoke" in macos
    assert '"$HOME/.nullion/venv/bin/nullion" uninstall --yes' in macos
    for script in (linux, windows, macos):
        assert "NULLION_SRC_DIR" in script
        assert "update --hash" in script
        assert "broken update target" in script
        assert "git_commit.txt" in script


def test_installer_smoke_documents_desktop_tray_limit() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    normalized = " ".join(readme.split())

    assert "GUI, browser, install, and web tests stay inside the container" in readme
    assert "desktop tray visibility" in normalized
