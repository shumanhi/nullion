#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_REPO="${NULLION_APP_REPO:-"${ROOT_DIR}"}"
IMAGE_NAME="${NULLION_INSTALLER_LINUX_IMAGE:-nullion-installer-smoke:linux}"

if [[ ! -d "${APP_REPO}" ]]; then
  echo "Nullion app checkout not found: ${APP_REPO}" >&2
  echo "Set NULLION_APP_REPO=/absolute/path/to/nullion and rerun." >&2
  exit 2
fi

APP_REPO_HOST="$(cd "${APP_REPO}" && pwd)"

cd "${ROOT_DIR}"
docker build -f docker/installer-linux.Dockerfile -t "${IMAGE_NAME}" .

docker run --rm \
  --mount "type=bind,source=${APP_REPO_HOST},target=/workspace/nullion,readonly" \
  --tmpfs /tmp/nullion-home:exec \
  --tmpfs /tmp/nullion-work:exec \
  -e HOME=/tmp/nullion-home \
  -e NULLION_HOME=/tmp/nullion-home/.nullion \
  -e NULLION_VERSION="${NULLION_VERSION:-main}" \
  -e NULLION_INSTALLER_SMOKE_TIMEOUT="${NULLION_INSTALLER_SMOKE_TIMEOUT:-20m}" \
  "${IMAGE_NAME}" \
  bash -lc '
    set -euo pipefail
    cp -a /workspace/nullion /tmp/nullion-work/nullion-src
    rm -rf /tmp/nullion-work/nullion-src/.git
    cd /tmp/nullion-work/nullion-src
    git config --global --add safe.directory /tmp/nullion-work/nullion-src
    mkdir -p /tmp/nullion-keyring
    cat > /tmp/nullion-keyring/installer_smoke_keyring.py <<'"'"'PY'"'"'
import json
import os
from pathlib import Path

from keyring.backend import KeyringBackend


class FileKeyring(KeyringBackend):
    priority = 1

    @property
    def _path(self):
        return Path(os.environ.get("NULLION_INSTALLER_KEYRING_FILE", "/tmp/nullion-keyring/store.json"))

    def get_password(self, service, username):
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except OSError:
            return None
        return data.get(f"{service}\\0{username}")

    def set_password(self, service, username, password):
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except OSError:
            data = {}
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data[f"{service}\\0{username}"] = password
        self._path.write_text(json.dumps(data), encoding="utf-8")

    def delete_password(self, service, username):
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except OSError:
            return
        data.pop(f"{service}\\0{username}", None)
        self._path.write_text(json.dumps(data), encoding="utf-8")
PY
    export PYTHONPATH="/tmp/nullion-keyring:${PYTHONPATH:-}"
    export PYTHON_KEYRING_BACKEND="installer_smoke_keyring.FileKeyring"

    run_installer() {
      local open_answer="${1:-y}"
      local key_storage_answer="${2:-n}"
      timeout "$NULLION_INSTALLER_SMOKE_TIMEOUT" expect <<EXPECT
      set timeout -1
      spawn bash ./install.sh
      expect "Ready to start?"
      send "y\r"
      expect "Protect local data encryption key with the Linux system keyring?"
      send "$key_storage_answer\r"
      expect "Select one or more"
      send "4\r"
      expect "Enter 1-11"
      send "5\r"
      expect -exact {Model [llama3.3]:}
      send "\r"
      expect "Enter 1, 2, 3, or 4:"
      send "4\r"
      expect "Enter 1, 2, 3, 4, or 5:"
      send "2\r"
      expect -exact {Enter 1, 2, 3, or 4 [4]:}
      send "4\r"
      expect {
        "Use existing media setup instead of setting it up again?" {
          send "y\r"
        }
        "Configure media tools now?" {
          send "n\r"
        }
      }
      expect {
        "Enter confirms the checked items." {
          send "\r"
          exp_continue
        }
        "Set up auto-start at login?" {
          send "n\r"
          exp_continue
        }
        "Open Nullion in your browser now?" {
          send "$open_answer\r"
          exp_continue
        }
        "Nullion v" {
          exp_continue
        }
        timeout {
          puts stderr "installer-linux smoke timed out waiting for final launch prompts"
          exit 124
        }
        eof
      }
EXPECT
    }

    assert_installed_entrypoints() {
    local expected_key_storage="${1:-local}"
    test -x "$HOME/.nullion/venv/bin/nullion"
    test -x "$HOME/.nullion/venv/bin/nullion-web"
    test -x "$HOME/.nullion/venv/bin/nullion-tray"
    test -x "$HOME/.nullion/venv/bin/nullion-webview"
    test -x "$HOME/.nullion/venv/bin/nullion-telegram"
    test -x "$HOME/.nullion/venv/bin/nullion-slack"
    test -x "$HOME/.nullion/venv/bin/nullion-discord"
    test -x "$HOME/.nullion/browser-use-venv/bin/python"
    "$HOME/.nullion/browser-use-venv/bin/python" -c "import browser_use; import playwright"
    test -f "$HOME/.nullion/.env"
    grep -q "NULLION_MODEL_PROVIDER=\"ollama\"" "$HOME/.nullion/.env"
    grep -q "NULLION_WEB_PORT=8742" "$HOME/.nullion/.env"
    grep -q "NULLION_KEY_STORAGE=\"${expected_key_storage}\"" "$HOME/.nullion/.env"
    grep -q "search_plugin=builtin_search_provider" "$HOME/.nullion/.env"
    if [[ "$expected_key_storage" == "system" ]]; then
      test ! -f "$HOME/.nullion/chat_history.key"
      test -s "${NULLION_INSTALLER_KEYRING_FILE:-/tmp/nullion-keyring/store.json}"
    else
      test -f "$HOME/.nullion/chat_history.key"
    fi

    "$HOME/.nullion/venv/bin/nullion" --help >/dev/null
    "$HOME/.nullion/venv/bin/nullion-web" --help >/dev/null
    "$HOME/.nullion/venv/bin/nullion-tray" --help >/dev/null
    "$HOME/.nullion/venv/bin/nullion-webview" --help >/dev/null
    "$HOME/.nullion/venv/bin/nullion-telegram" --help >/dev/null
    "$HOME/.nullion/venv/bin/nullion-slack" --help >/dev/null
    "$HOME/.nullion/venv/bin/nullion-discord" --help >/dev/null
    }

    assert_web_health() {
    for attempt in $(seq 1 60); do
      if curl -fsS http://127.0.0.1:8742/api/health | grep -q "\"status\":\"ok\""; then
        return 0
      fi
      sleep 1
    done

    echo "Nullion web service did not become healthy after installer launch." >&2
    tail -100 "$HOME/.nullion/logs/nullion-error.log" 2>/dev/null || true
    exit 1
    }

    stop_installed_services() {
      pkill -f "$HOME/.nullion/venv/bin/nullion-web" 2>/dev/null || true
      pkill -f "$HOME/.nullion/venv/bin/nullion-tray" 2>/dev/null || true
      sleep 1
    }

    assert_update_rollback_smoke() {
      local src="/tmp/nullion-work/nullion-src"
      local origin="/tmp/nullion-work/nullion-update-origin.git"
      local output_file="/tmp/nullion-work/nullion-update-rollback.out"
      local healthy_commit
      local after_commit
      local update_status

      rm -rf "$src/.git" "$origin"
      git -C "$src" init >/dev/null
      git -C "$src" checkout -B main >/dev/null
      git -C "$src" config user.email "installer-smoke@nullion.local"
      git -C "$src" config user.name "Nullion Installer Smoke"
      git -C "$src" add -A
      git -C "$src" commit -m "healthy install source" >/dev/null
      healthy_commit="$(git -C "$src" rev-parse HEAD)"

      git init --bare "$origin" >/dev/null
      git -C "$src" remote add origin "$origin"
      git -C "$src" push -u origin main >/dev/null

      printf "\nthis is invalid python for rollback smoke\n" >> "$src/src/nullion/web_app.py"
      git -C "$src" add src/nullion/web_app.py
      git -C "$src" commit -m "broken update target" >/dev/null
      git -C "$src" push origin main >/dev/null
      git -C "$src" reset --hard "$healthy_commit" >/dev/null

      set +e
      NULLION_SRC_DIR="$src" \
      NULLION_INSTALL_DIR="$HOME/.nullion" \
      NULLION_ENV_FILE="$HOME/.nullion/.env" \
      NULLION_CHECKPOINT="$HOME/.nullion/runtime.db" \
        "$HOME/.nullion/venv/bin/nullion" update --hash >"$output_file" 2>&1
      update_status=$?
      set -e

      if [[ "$update_status" -eq 0 ]]; then
        cat "$output_file"
        echo "Update rollback smoke unexpectedly succeeded." >&2
        exit 1
      fi
      if ! grep -Eq "Rolled back to previous version automatically|Rollback successful" "$output_file"; then
        cat "$output_file"
        echo "Update rollback smoke did not report automatic rollback." >&2
        exit 1
      fi

      after_commit="$(git -C "$src" rev-parse HEAD)"
      if [[ "$after_commit" != "$healthy_commit" ]]; then
        cat "$output_file"
        echo "Update rollback smoke left HEAD at $after_commit instead of $healthy_commit." >&2
        exit 1
      fi
      find "$HOME/.nullion/backups" -name git_commit.txt -print -quit | grep -q .
    }

    run_installer y n
    assert_installed_entrypoints local
    assert_web_health
    stop_installed_services

    printf "999999\n" > "$HOME/.nullion/webview.pid"
    "$HOME/.nullion/venv/bin/nullion" uninstall --yes
    test ! -d "$HOME/.nullion"

    run_installer y y
    assert_installed_entrypoints system
    assert_web_health
    stop_installed_services
    assert_update_rollback_smoke
  '
