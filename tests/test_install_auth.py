from __future__ import annotations

import pathlib
import subprocess
import textwrap


ROOT = pathlib.Path(__file__).resolve().parents[1]

from nullion import auth


def test_installer_runs_oauth_with_installed_venv_python() -> None:
    install_sh = (ROOT / "install.sh").read_text(encoding="utf-8")

    assert '"$VENV_DIR/bin/python" -m nullion.auth --write-codex-access-token "$OAUTH_TOKEN_FILE"' in install_sh
    assert 'OAUTH_TOKEN_FILE="$(mktemp)"' in install_sh
    assert 'OAUTH_TOKEN="$(cat "$OAUTH_TOKEN_FILE" 2>/dev/null || true)"' in install_sh
    assert '"$PYTHON" -m nullion.auth --print-codex-access-token' not in install_sh
    assert 'tee /tmp/nullion_oauth_err.txt >&2' in install_sh
    assert '--print-codex-access-token' not in install_sh


def test_installer_initializes_secure_storage_after_editable_install() -> None:
    install_sh = (ROOT / "install.sh").read_text(encoding="utf-8")
    install_pos = install_sh.index('"$VENV_DIR/bin/pip" install --quiet -e "$SOURCE_DIR"')
    installed_pos = install_sh.index('print_ok "Nullion installed."')
    storage_pos = install_sh.index("initialize_key_storage\n\n# ── Step 3")

    assert install_pos < installed_pos < storage_pos


def test_installer_launchd_kickstart_warning_is_success_when_agent_is_running() -> None:
    install_sh = (ROOT / "install.sh").read_text(encoding="utf-8")

    assert "launchd_agent_is_running()" in install_sh
    assert 'if launchd_agent_is_running "$target"; then' in install_sh
    assert "launchd kickstart reported a warning, but the service is running." in install_sh


def test_installer_keeps_telegram_bot_token_hidden_and_out_of_urls() -> None:
    install_sh = (ROOT / "install.sh").read_text(encoding="utf-8")

    assert 'prompt_read -rs BOT_TOKEN' in install_sh
    assert 'https://api.telegram.org/bot${BOT_TOKEN}/getUpdates' not in install_sh
    assert 'https://api.telegram.org/bot<your-bot-token>/getUpdates' in install_sh
    assert 'Existing token found: ${EXISTING_TOKEN:0:12}...' not in install_sh
    assert 'Existing token found: $(mask_secret "$EXISTING_TOKEN")' in install_sh


def test_codex_access_token_flag_prints_only_token(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        auth,
        "codex_oauth_credentials",
        lambda: {
            "provider": "codex",
            "api_key": "test-access-token",
            "refresh_token": "test-refresh-token",
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-5.5",
        },
    )

    auth._cli_impl(["--print-codex-access-token"])

    captured = capsys.readouterr()
    assert captured.out == "test-access-token\n"


def test_codex_access_token_file_flag_keeps_device_code_visible(monkeypatch, tmp_path, capsys) -> None:
    def fake_oauth_credentials():
        print("  1. Open: https://auth.openai.com/codex/device")
        print("  2. Enter code: TEST-CODE")
        return {
            "provider": "codex",
            "api_key": "test-access-token",
            "refresh_token": "test-refresh-token",
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-5.5",
        }

    token_path = tmp_path / "token"
    monkeypatch.setattr(auth, "codex_oauth_credentials", fake_oauth_credentials)

    auth._cli_impl(["--write-codex-access-token", str(token_path)])

    captured = capsys.readouterr()
    assert "TEST-CODE" in captured.out
    assert token_path.read_text(encoding="utf-8") == "test-access-token"
    assert token_path.stat().st_mode & 0o777 == 0o600


def test_installer_oauth_shell_flow_keeps_device_code_visible_while_capturing_token(tmp_path) -> None:
    fake_venv = tmp_path / "venv"
    fake_bin = fake_venv / "bin"
    fake_bin.mkdir(parents=True)
    fake_python = fake_bin / "python"
    fake_python.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env bash
            set -euo pipefail
            token_file="${@: -1}"
            echo "  1. Open: https://auth.openai.com/codex/device"
            echo "  2. Enter code: TEST-CODE"
            echo "  Waiting for you to approve..."
            printf 'test-access-token' > "$token_file"
            """
        ),
        encoding="utf-8",
    )
    fake_python.chmod(0o755)
    err_file = tmp_path / "oauth.err"
    script = textwrap.dedent(
        f"""\
        set -euo pipefail
        VENV_DIR="{fake_venv}"
        SOURCE_DIR="{tmp_path}"
        OAUTH_TOKEN=""
        OAUTH_TOKEN_FILE="$(mktemp)"
        if PYTHONPATH="$SOURCE_DIR/src" "$VENV_DIR/bin/python" -m nullion.auth --write-codex-access-token "$OAUTH_TOKEN_FILE" 2> >(tee "{err_file}" >&2); then
            OAUTH_TOKEN="$(cat "$OAUTH_TOKEN_FILE" 2>/dev/null || true)"
        fi
        rm -f "$OAUTH_TOKEN_FILE"
        printf 'TOKEN=%s\\n' "$OAUTH_TOKEN"
        """
    )

    result = subprocess.run(
        ["bash", "-c", script],
        text=True,
        capture_output=True,
        check=True,
    )

    assert "Enter code: TEST-CODE" in result.stdout
    assert "TOKEN=test-access-token" in result.stdout
