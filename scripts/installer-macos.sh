#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_REPO="${NULLION_APP_REPO:-"${ROOT_DIR}"}"
SMOKE_TIMEOUT_SECONDS="${NULLION_INSTALLER_SMOKE_TIMEOUT_SECONDS:-2700}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "macOS installer smoke must run on a macOS host." >&2
  exit 2
fi

if [[ ! -d "${APP_REPO}" ]]; then
  echo "Nullion app checkout not found: ${APP_REPO}" >&2
  echo "Set NULLION_APP_REPO=/absolute/path/to/nullion and rerun." >&2
  exit 2
fi

APP_REPO_HOST="$(cd "${APP_REPO}" && pwd)"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nullion-installer-macos-work.XXXXXX")"
HOME_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nullion-installer-macos-home.XXXXXX")"
WEB_PID=""
HOST_PYTHON="$(command -v python3.13 || command -v python3.12 || command -v python3.11 || command -v python3)"
WEB_PORT="${NULLION_INSTALLER_SMOKE_PORT:-$("${HOST_PYTHON}" -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')}"

cleanup() {
  if [[ -n "${WEB_PID}" ]] && kill -0 "${WEB_PID}" 2>/dev/null; then
    kill "${WEB_PID}" 2>/dev/null || true
  fi
  pkill -f "${HOME_DIR}/.nullion/venv/bin/nullion-web" 2>/dev/null || true
  pkill -f "${HOME_DIR}/.nullion/venv/bin/nullion-tray" 2>/dev/null || true
  rm -rf "${WORK_DIR}" "${HOME_DIR}"
}
trap cleanup EXIT

export HOME="${HOME_DIR}"
export NULLION_HOME="${HOME_DIR}/.nullion"
export NULLION_VERSION="${NULLION_VERSION:-main}"
export NULLION_INSTALLER_SMOKE="true"
export NULLION_INSTALLER_SMOKE_TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS}"
export NULLION_INSTALLER_KEYCHAIN_FILE="${WORK_DIR}/fake-keychain-value"

FAKE_BIN="${WORK_DIR}/fake-bin"
mkdir -p "${FAKE_BIN}"
cat > "${FAKE_BIN}/security" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

store="${NULLION_INSTALLER_KEYCHAIN_FILE:?}"
cmd="${1:-}"
shift || true
password=""

while (($# > 0)); do
  case "$1" in
    -w)
      if [[ "$cmd" == "add-generic-password" && $# -gt 1 ]]; then
        password="$2"
        shift 2
      else
        shift
      fi
      ;;
    -s|-a)
      shift 2
      ;;
    -U)
      shift
      ;;
    *)
      shift
      ;;
  esac
done

case "$cmd" in
  find-generic-password)
    [[ -s "$store" ]] || exit 44
    cat "$store"
    ;;
  add-generic-password)
    printf '%s\n' "$password" > "$store"
    ;;
  *)
    exit 2
    ;;
esac
SH
chmod +x "${FAKE_BIN}/security"

cat > "${FAKE_BIN}/launchctl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

# launchd labels live in the real user's global GUI domain even when HOME is
# temporary. Never let this isolated smoke test control host services.
case "${1:-}" in
  print|list)
    exit 113
    ;;
  *)
    exit 0
    ;;
esac
SH
chmod +x "${FAKE_BIN}/launchctl"
export PATH="${FAKE_BIN}:${PATH}"

cp -a "${APP_REPO_HOST}" "${WORK_DIR}/nullion-src"
cd "${WORK_DIR}/nullion-src"
git config --global --add safe.directory "${WORK_DIR}/nullion-src"

run_installer() {
  local open_answer="${1:-n}"
  local keychain_answer="${2:-n}"
  OPEN_ANSWER="${open_answer}" KEYCHAIN_ANSWER="${keychain_answer}" expect <<'EXPECT'
    set timeout $env(NULLION_INSTALLER_SMOKE_TIMEOUT_SECONDS)
    spawn bash ./install.sh
    expect "Ready to start?"
    send "y\r"
    expect "Protect local data encryption key with macOS Keychain?"
    send "$env(KEYCHAIN_ANSWER)\r"
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
        send "$env(OPEN_ANSWER)\r"
        exp_continue
      }
      "Nullion v" {
        exp_continue
      }
      timeout {
        puts stderr "installer-macos smoke timed out waiting for final launch prompts"
        exit 124
      }
      eof
    }
EXPECT
}

assert_installed_entrypoints() {
  local expected_key_storage="${1:-local}"
  test -x "$HOME/.nullion/browser-use-venv/bin/python"
    "$HOME/.nullion/browser-use-venv/bin/python" -c "import browser_use; import playwright"
    test -x "$HOME/.nullion/venv/bin/nullion"
  test -x "$HOME/.nullion/venv/bin/nullion-web"
  test -x "$HOME/.nullion/venv/bin/nullion-tray"
  test -x "$HOME/.nullion/venv/bin/nullion-webview"
  test -x "$HOME/.nullion/venv/bin/nullion-telegram"
  test -x "$HOME/.nullion/venv/bin/nullion-slack"
  test -x "$HOME/.nullion/venv/bin/nullion-discord"
  test -f "$HOME/.nullion/.env"
  grep -q "NULLION_MODEL_PROVIDER=\"ollama\"" "$HOME/.nullion/.env"
  grep -q "NULLION_WEB_PORT=8742" "$HOME/.nullion/.env"
  grep -q "NULLION_KEY_STORAGE=\"${expected_key_storage}\"" "$HOME/.nullion/.env"
  grep -q "search_plugin=builtin_search_provider" "$HOME/.nullion/.env"
  if [[ "$expected_key_storage" == "keychain" ]]; then
    test ! -f "$HOME/.nullion/chat_history.key"
    test -s "$NULLION_INSTALLER_KEYCHAIN_FILE"
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

start_web() {
  mkdir -p "$HOME/.nullion/logs"
  set -a
  # shellcheck source=/dev/null
  source "$HOME/.nullion/.env"
  set +a
  "$HOME/.nullion/venv/bin/nullion-web" \
    --host 127.0.0.1 \
    --port "${WEB_PORT}" \
    --env-file "$HOME/.nullion/.env" \
    --checkpoint "$HOME/.nullion/runtime.db" \
    >> "$HOME/.nullion/logs/installer-smoke-web.log" \
    2>> "$HOME/.nullion/logs/installer-smoke-web-error.log" &
  WEB_PID=$!
}

assert_web_health() {
  start_web
  for _attempt in $(seq 1 60); do
    if ! kill -0 "${WEB_PID}" 2>/dev/null; then
      echo "Nullion web service exited before becoming healthy." >&2
      tail -100 "$HOME/.nullion/logs/installer-smoke-web-error.log" 2>/dev/null || true
      exit 1
    fi
    if curl -fsS "http://127.0.0.1:${WEB_PORT}/api/health" | grep -q "\"status\":\"ok\""; then
      kill "${WEB_PID}" 2>/dev/null || true
      wait "${WEB_PID}" 2>/dev/null || true
      WEB_PID=""
      return 0
    fi
    sleep 1
  done

  echo "Nullion web service did not become healthy after installer smoke launch." >&2
  tail -100 "$HOME/.nullion/logs/installer-smoke-web.log" 2>/dev/null || true
  tail -100 "$HOME/.nullion/logs/installer-smoke-web-error.log" 2>/dev/null || true
  exit 1
}

stop_installed_services() {
  if [[ -n "${WEB_PID}" ]] && kill -0 "${WEB_PID}" 2>/dev/null; then
    kill "${WEB_PID}" 2>/dev/null || true
    wait "${WEB_PID}" 2>/dev/null || true
    WEB_PID=""
  fi
  pkill -f "$HOME/.nullion/venv/bin/nullion-web" 2>/dev/null || true
  pkill -f "$HOME/.nullion/venv/bin/nullion-tray" 2>/dev/null || true
  sleep 1
}

assert_update_rollback_smoke() {
  local src="${WORK_DIR}/nullion-src"
  local origin="${WORK_DIR}/nullion-update-origin.git"
  local output_file="${WORK_DIR}/nullion-update-rollback.out"
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

  printf '\nthis is invalid python for rollback smoke\n' >> "$src/src/nullion/web_app.py"
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

uninstall_installed_app() {
  printf "999999\n" > "$HOME/.nullion/webview.pid"
  "$HOME/.nullion/venv/bin/nullion" uninstall --yes
  test ! -d "$HOME/.nullion"
}

run_installer n n
assert_installed_entrypoints local
assert_web_health
stop_installed_services

uninstall_installed_app

run_installer n y
assert_installed_entrypoints keychain
assert_web_health
stop_installed_services
assert_update_rollback_smoke
