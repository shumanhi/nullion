#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Nullion — one-command installer (macOS + Linux)
# Usage:  curl -fsSL "https://raw.githubusercontent.com/shumanhi/nullion/main/install.sh?$(date +%s)" | bash
#     or: bash install.sh
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NULLION_VERSION="${NULLION_VERSION:-latest}"
NULLION_INSTALL_DIR="$HOME/.nullion"
NULLION_ENV_FILE="$NULLION_INSTALL_DIR/.env"
NULLION_LOG_DIR="$NULLION_INSTALL_DIR/logs"
REPO_URL="https://github.com/shumanhi/nullion.git"

# ── OS detection ──────────────────────────────────────────────────────────────
OS="$(uname -s)"
case "$OS" in
    Darwin) PLATFORM="macos" ;;
    Linux)  PLATFORM="linux" ;;
    *)
        echo "Unsupported platform: $OS"
        echo "For Windows, use install.ps1 instead."
        exit 1
        ;;
esac

# macOS-specific paths
LAUNCHD_LABEL="com.nullion.web"
LAUNCHD_PLIST="$HOME/Library/LaunchAgents/${LAUNCHD_LABEL}.plist"
TRAY_LAUNCHD_LABEL="com.nullion.tray"
TRAY_LAUNCHD_PLIST="$HOME/Library/LaunchAgents/${TRAY_LAUNCHD_LABEL}.plist"
TELEGRAM_LAUNCHD_LABEL="ai.nullion.telegram"
TELEGRAM_LAUNCHD_PLIST="$HOME/Library/LaunchAgents/${TELEGRAM_LAUNCHD_LABEL}.plist"
SLACK_LAUNCHD_LABEL="ai.nullion.slack"
SLACK_LAUNCHD_PLIST="$HOME/Library/LaunchAgents/${SLACK_LAUNCHD_LABEL}.plist"
DISCORD_LAUNCHD_LABEL="ai.nullion.discord"
DISCORD_LAUNCHD_PLIST="$HOME/Library/LaunchAgents/${DISCORD_LAUNCHD_LABEL}.plist"

# Linux-specific paths
SYSTEMD_USER_DIR="$HOME/.config/systemd/user"
SYSTEMD_SERVICE="nullion.service"
TELEGRAM_SYSTEMD_SERVICE="nullion-telegram.service"
SLACK_SYSTEMD_SERVICE="nullion-slack.service"
DISCORD_SYSTEMD_SERVICE="nullion-discord.service"

# Web UI
NULLION_WEB_PORT=8742

# Recommended local speech-to-text runtime.  base.en is small enough for CPU
# voice notes but much more reliable than tiny.en for short commands.
WHISPER_CPP_MODEL_URL="https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-base.en.bin"
WHISPER_CPP_MODEL_PATH="$NULLION_INSTALL_DIR/models/ggml-base.en.bin"

# ── colours ───────────────────────────────────────────────────────────────────
BOLD="\033[1m"
DIM="\033[2m"
GREEN="\033[32m"
YELLOW="\033[33m"
RED="\033[31m"
CYAN="\033[36m"
MAGENTA="\033[35m"
BLUE="\033[34m"
RESET="\033[0m"

print_header() {
    echo
    echo -e "  ${DIM}╭────────────────────────────────────────────────────────────╮${RESET}"
    echo -e "  ${DIM}│${RESET} ${BOLD}${CYAN}$*${RESET}"
    echo -e "  ${DIM}╰────────────────────────────────────────────────────────────╯${RESET}"
}
print_ok()     { echo -e "  ${GREEN}✓${RESET}  $*"; }
print_info()   { echo -e "  ${YELLOW}→${RESET}  $*"; }
print_err()    { echo -e "  ${RED}✗${RESET}  $*" >&2; }
print_bold()   { echo -e "\n  ${BOLD}${CYAN}◆${RESET} ${BOLD}$*${RESET}"; }
print_chip()   { echo -e "  ${DIM}[$1]${RESET} $2"; }

prompt_read() {
    if [[ -r /dev/tty ]]; then
        read "$@" </dev/tty
    else
        read "$@"
    fi
}

prompt_model_name() {
    local var_name="$1"
    local current="${!var_name}"
    local model_input
    print_info "Press Enter to use the default (${current}), or type a different model name."
    prompt_read -rp "  Model [${current}]: " model_input
    printf -v "$var_name" '%s' "${model_input:-$current}"
}

print_box_row() {
    local width="$1"
    local plain="$2"
    local styled="$3"
    local pad=$((width - ${#plain}))
    (( pad < 0 )) && pad=0
    printf "  ${DIM}│${RESET} %b%*s ${DIM}│${RESET}\n" "$styled" "$pad" ""
}

print_logo() {
    local width=84
    local border="──────────────────────────────────────────────────────────────────────────────────────"
    echo -e "  ${DIM}╭${border}╮${RESET}"
    print_box_row "$width" "   +--------+     Nullion setup studio" "   ${CYAN}╭────────╮${RESET}     ${BOLD}Nulli${CYAN}ø${RESET}${BOLD}n${RESET} ${DIM}setup studio${RESET}"
    print_box_row "$width" "   | o    o |     Local-first AI operator" "   ${CYAN}│${RESET} ${BOLD}●${RESET}    ${BOLD}●${RESET} ${CYAN}│${RESET}     ${CYAN}Local-first AI operator${RESET}"
    print_box_row "$width" "   |   --   |     v${NULLION_VERSION} - guided install" "   ${CYAN}│${RESET}   ${BOLD}━━${RESET}   ${CYAN}│${RESET}     ${DIM}v${NULLION_VERSION} · guided install${RESET}"
    print_box_row "$width" "   +--------+     * ready" "   ${CYAN}╰────────╯${RESET}     ${GREEN}● ready${RESET}"
    echo -e "  ${DIM}╰${border}╯${RESET}"
}

print_menu_item() {
    local number="$1"
    local title="$2"
    local detail="$3"
    local badge="${4:-}"
    if [[ -n "$badge" ]]; then
        echo -e "   ${BOLD}${number})${RESET} ${BOLD}${title}${RESET} ${GREEN}${badge}${RESET}"
    else
        echo -e "   ${BOLD}${number})${RESET} ${BOLD}${title}${RESET}"
    fi
    [[ -n "$detail" ]] && echo -e "      ${DIM}${detail}${RESET}"
}

print_check_item() {
    local checked="$1"
    local focused="$2"
    local title="$3"
    local detail="$4"
    local badge="${5:-}"
    local cursor=" "
    local mark=" "
    [[ "$focused" == "true" ]] && cursor="›"
    [[ "$checked" == "true" ]] && mark="x"
    if [[ -n "$badge" ]]; then
        echo -e "  ${BOLD}${cursor}${RESET} [${mark}] ${BOLD}${title}${RESET} ${GREEN}${badge}${RESET}"
    else
        echo -e "  ${BOLD}${cursor}${RESET} [${mark}] ${BOLD}${title}${RESET}"
    fi
    [[ -n "$detail" ]] && echo -e "        ${DIM}${detail}${RESET}"
}

print_setup_overview() {
    local width=84
    local border="──────────────────────────────────────────────────────────────────────────────────────"
    echo -e "  ${DIM}╭─ Setup Path ─────────────────────────────────────────────────────────────────────────╮${RESET}"
    print_box_row "$width" "1  Python runtime        check or install Python 3.11-3.13" "${BOLD}1${RESET}  Python runtime        ${DIM}check or install Python 3.11-3.13${RESET}"
    print_box_row "$width" "2  Nullion app           install into ${NULLION_INSTALL_DIR}" "${BOLD}2${RESET}  Nullion app           ${DIM}install into ${NULLION_INSTALL_DIR}${RESET}"
    print_box_row "$width" "3  Capabilities          AI, chat, browser, media, skills" "${BOLD}3${RESET}  Capabilities          ${DIM}AI, chat, browser, media, skills${RESET}"
    print_box_row "$width" "4  Launch                dashboard at http://localhost:${NULLION_WEB_PORT}" "${BOLD}4${RESET}  Launch                ${DIM}dashboard at http://localhost:${NULLION_WEB_PORT}${RESET}"
    echo -e "  ${DIM}╰${border}╯${RESET}"
}

# ── helpers ───────────────────────────────────────────────────────────────────
command_exists() { command -v "$1" &>/dev/null; }

xml_escape() {
    local value="$1"
    value="${value//&/&amp;}"
    value="${value//</&lt;}"
    value="${value//>/&gt;}"
    value="${value//\"/&quot;}"
    printf '%s' "$value"
}

launchd_agent_is_running() {
    local target="$1"
    local state
    state="$(launchctl print "$target" 2>/dev/null | awk -F'= ' '/state = / {print $2; exit}' || true)"
    [[ "$state" == "running" ]]
}

launchd_register_agent() {
    local label="$1"
    local plist="$2"
    local display="$3"
    local domain="gui/$(id -u)"
    local target="${domain}/${label}"
    local output

    if command_exists plutil; then
        if ! output="$(plutil -lint "$plist" 2>&1)"; then
            print_err "${display} LaunchAgent plist is invalid: ${output}"
            print_info "Plist: ${plist}"
            return 1
        fi
    fi

    launchctl bootout "$target" >/dev/null 2>&1 || true
    sleep 1

    local bootstrap_output=""
    local bootstrapped=false
    local attempt
    for attempt in 1 2 3; do
        if output="$(launchctl bootstrap "$domain" "$plist" 2>&1)"; then
            bootstrapped=true
            break
        fi
        bootstrap_output="$output"
        sleep "$attempt"
        if launchctl print "$target" >/dev/null 2>&1; then
            bootstrapped=true
            print_info "${display} launchd bootstrap reported a warning, but the service is registered."
            break
        fi
        launchctl bootout "$target" >/dev/null 2>&1 || true
    done

    if [[ "$bootstrapped" != "true" ]]; then
        print_err "${display} launchd bootstrap failed: ${bootstrap_output:-unknown launchctl error}"
        print_info "Plist: ${plist}"
        print_info "For deeper macOS diagnostics, run: launchctl print ${target}"
        return 1
    fi

    if ! output="$(launchctl kickstart -k "$target" 2>&1)"; then
        if launchd_agent_is_running "$target"; then
            print_info "${display} launchd kickstart reported a warning, but the service is running."
            return 0
        fi
        print_err "${display} launchd kickstart failed: ${output:-unknown launchctl error}"
        print_info "Plist: ${plist}"
        print_info "For deeper macOS diagnostics, run: launchctl print ${target}"
        return 1
    fi

    return 0
}

write_chat_launchd_plist() {
    local plist="$1"
    local label="$2"
    local command_name="$3"
    local log_prefix="$4"
    local throttle="${5:-5}"
    cat > "$plist" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$(xml_escape "$label")</string>
    <key>ProgramArguments</key>
    <array>
        <string>$(xml_escape "${VENV_DIR}/bin/${command_name}")</string>
        <string>--checkpoint</string>
        <string>$(xml_escape "${NULLION_INSTALL_DIR}/runtime.db")</string>
        <string>--env-file</string>
        <string>$(xml_escape "$NULLION_ENV_FILE")</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>NULLION_ENV_FILE</key>
        <string>$(xml_escape "$NULLION_ENV_FILE")</string>
        <key>PATH</key>
        <string>$(xml_escape "${VENV_DIR}/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")</string>
    </dict>
    <key>WorkingDirectory</key>
    <string>$(xml_escape "$NULLION_INSTALL_DIR")</string>
    <key>StandardOutPath</key>
    <string>$(xml_escape "${NULLION_LOG_DIR}/${log_prefix}.log")</string>
    <key>StandardErrorPath</key>
    <string>$(xml_escape "${NULLION_LOG_DIR}/${log_prefix}-error.log")</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>ThrottleInterval</key>
    <integer>${throttle}</integer>
</dict>
</plist>
PLIST
}

write_chat_systemd_unit() {
    local unit_path="$1"
    local description="$2"
    local command_name="$3"
    local log_prefix="$4"
    local restart_sec="${5:-5}"
    cat > "$unit_path" << UNIT
[Unit]
Description=${description}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${VENV_DIR}/bin/${command_name} --checkpoint ${NULLION_INSTALL_DIR}/runtime.db --env-file ${NULLION_ENV_FILE}
EnvironmentFile=${NULLION_ENV_FILE}
WorkingDirectory=${NULLION_INSTALL_DIR}
StandardOutput=append:${NULLION_LOG_DIR}/${log_prefix}.log
StandardError=append:${NULLION_LOG_DIR}/${log_prefix}-error.log
Restart=on-failure
RestartSec=${restart_sec}
StartLimitIntervalSec=120
StartLimitBurst=5

[Install]
WantedBy=default.target
UNIT
}

wait_for_web_ui() {
    local health_url="http://127.0.0.1:${NULLION_WEB_PORT}/api/health"
    local attempt
    if ! command_exists curl; then
        sleep 2
        return 0
    fi
    for attempt in {1..20}; do
        if curl -fsS "$health_url" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

open_native_webview_now() {
    print_info "Opening Nullion in the desktop app..."
    wait_for_web_ui || print_info "Nullion is still warming up; the window will finish loading shortly."
    if [[ -x "$VENV_DIR/bin/nullion-webview" ]]; then
        nohup "$VENV_DIR/bin/nullion-webview" \
            --port "${NULLION_WEB_PORT}" \
            --env-file "${NULLION_ENV_FILE}" \
            --browser-fallback \
            >> "$NULLION_LOG_DIR/webview.log" \
            2>> "$NULLION_LOG_DIR/webview-error.log" &
        print_ok "Opened Nullion desktop app."
    else
        print_info "Desktop app launcher is missing; opening the browser instead."
        open "http://localhost:${NULLION_WEB_PORT}" 2>/dev/null || true
    fi
}

confirm() {
    local prompt="${1:-Continue?}"
    local yn
    prompt_read -rp "  $prompt [y/N] " yn
    [[ "$(echo "$yn" | tr '[:upper:]' '[:lower:]')" =~ ^(y|yes)$ ]]
}

confirm_yes() {
    local prompt="${1:-Continue?}"
    local yn
    prompt_read -rp "  $prompt [Y/n] " yn
    [[ -z "$yn" || "$(echo "$yn" | tr '[:upper:]' '[:lower:]')" =~ ^(y|yes)$ ]]
}

choose_key_storage() {
    local existing="${1:-}"
    NULLION_KEY_STORAGE="${existing:-}"
    if [[ -n "$NULLION_KEY_STORAGE" ]]; then
        print_info "Found existing local data key storage: ${NULLION_KEY_STORAGE}"
        if confirm_yes "Keep existing encryption key storage setting?"; then
            return 0
        fi
    fi

    echo
    print_bold "  Local data encryption"
    if [[ "$PLATFORM" == "macos" ]]; then
        echo "  Nullion encrypts local chat history. You can protect the encryption key"
        echo "  with macOS Keychain, or store it locally beside your Nullion data."
        echo
        if confirm_yes "Protect local data encryption key with macOS Keychain?"; then
            NULLION_KEY_STORAGE="keychain"
        else
            NULLION_KEY_STORAGE="local"
        fi
    elif [[ "$PLATFORM" == "linux" ]]; then
        echo "  Nullion encrypts local chat history and saved provider credentials."
        echo "  On Linux, Nullion can use the Secret Service keyring when an unlocked"
        echo "  GNOME Keyring, KWallet, or compatible provider is available."
        echo
        if confirm "Protect local data encryption key with the Linux system keyring?"; then
            NULLION_KEY_STORAGE="system"
        else
            NULLION_KEY_STORAGE="local"
        fi
    else
        echo "  Nullion encrypts local chat history and saved provider credentials."
        echo "  Nullion can protect the encryption key with the operating system secret store,"
        echo "  or store it locally beside your Nullion data."
        echo
        if confirm_yes "Protect local data encryption key with the operating system secret store?"; then
            NULLION_KEY_STORAGE="system"
        else
            NULLION_KEY_STORAGE="local"
        fi
    fi
}

initialize_key_storage() {
    local requested="${NULLION_KEY_STORAGE:-local}"
    if NULLION_KEY_STORAGE="$requested" "$VENV_DIR/bin/python" -m nullion.secure_storage --init --storage "$requested" >/tmp/nullion_key_storage.out 2>/tmp/nullion_key_storage.err; then
        if [[ "$requested" == "keychain" ]]; then
            print_ok "Local data key protected with macOS Keychain."
        elif [[ "$requested" == "system" ]]; then
            print_ok "Local data key protected with the operating system secret store."
        else
            print_ok "Local data key stored at $NULLION_INSTALL_DIR/chat_history.key."
        fi
        return 0
    fi

    local err
    err="$(cat /tmp/nullion_key_storage.err 2>/dev/null || true)"
    if [[ "$requested" == "keychain" ]]; then
        print_err "Could not initialize macOS Keychain storage: ${err:-unknown error}"
        print_info "Falling back to local key storage for this install."
        NULLION_KEY_STORAGE="local"
        NULLION_KEY_STORAGE="local" "$VENV_DIR/bin/python" -m nullion.secure_storage --init --storage local >/dev/null
        print_ok "Local data key stored at $NULLION_INSTALL_DIR/chat_history.key."
        return 0
    fi

    if [[ "$requested" == "system" ]]; then
        print_err "Could not initialize operating system key storage: ${err:-unknown error}"
        print_info "Choose local key storage or configure a supported OS keyring, then rerun setup."
        exit 1
    fi

    print_err "Could not initialize local key storage: ${err:-unknown error}"
    exit 1
}

finalize_runtime_database() {
    print_info "Finalizing local runtime database..."
    if NULLION_ENV_FILE="$NULLION_ENV_FILE" \
       NULLION_INSTALL_DIR="$NULLION_INSTALL_DIR" \
       NULLION_CHECKPOINT_PATH="${NULLION_INSTALL_DIR}/runtime.db" \
       "$VENV_DIR/bin/python" - "$NULLION_ENV_FILE" "${NULLION_INSTALL_DIR}/runtime.db" "$NULLION_INSTALL_DIR" >/tmp/nullion_runtime_finalize.out 2>/tmp/nullion_runtime_finalize.err <<'PY'
import json
import sys
from pathlib import Path

from nullion.updater import run_post_update_migrations

details = run_post_update_migrations(
    env_path=Path(sys.argv[1]),
    checkpoint_path=Path(sys.argv[2]),
    install_dir=Path(sys.argv[3]),
    overwrite_env_credentials=True,
)
warnings = details.get("warnings") or []
print(json.dumps({"ok": True, "warnings": warnings}, sort_keys=True))
PY
    then
        print_ok "Local runtime database is ready."
    else
        local err
        err="$(cat /tmp/nullion_runtime_finalize.err 2>/dev/null || true)"
        print_err "Could not finalize the local runtime database. Setup can continue; run Nullion once to retry migrations.${err:+ ${err}}"
    fi
}

env_value_from_file() {
    local file="$1"
    local key="$2"
    [[ -f "$file" ]] || return 0
    grep -E "^${key}=" "$file" 2>/dev/null | tail -n 1 | cut -d= -f2- | sed -e 's/\r$//' -e 's/^"//' -e 's/"$//' || true
}

env_candidate_files() {
    local seen=""
    local candidate
    for candidate in \
        "${NULLION_ENV_FILE:-}" \
        "${SOURCE_DIR:-}/.env"; do
        [[ -n "$candidate" ]] || continue
        [[ -f "$candidate" ]] || continue
        case ":$seen:" in
            *":$candidate:"*) continue ;;
        esac
        seen="${seen:+$seen:}$candidate"
        printf '%s\n' "$candidate"
    done
}

env_value() {
    local key="$1"
    local candidate
    local value
    while IFS= read -r candidate; do
        value="$(env_value_from_file "$candidate" "$key")"
        if [[ -n "$value" ]]; then
            printf '%s' "$value"
            return 0
        fi
    done < <(env_candidate_files)
    return 0
}

env_value_any() {
    local key
    local value
    for key in "$@"; do
        value="$(env_value "$key")"
        if [[ -n "$value" ]]; then
            printf '%s' "$value"
            return 0
        fi
    done
    return 0
}

provider_key_env_names() {
    local provider
    provider="$(echo "${1:-}" | tr '[:upper:]' '[:lower:]')"
    case "$provider" in
        anthropic) printf '%s\n' "NULLION_ANTHROPIC_API_KEY" "ANTHROPIC_API_KEY" ;;
        openrouter) printf '%s\n' "NULLION_OPENROUTER_API_KEY" "OPENROUTER_API_KEY" "NULLION_OPENAI_API_KEY" "OPENAI_API_KEY" ;;
        gemini) printf '%s\n' "NULLION_GEMINI_API_KEY" "GEMINI_API_KEY" "GOOGLE_API_KEY" "NULLION_OPENAI_API_KEY" "OPENAI_API_KEY" ;;
        groq) printf '%s\n' "NULLION_GROQ_API_KEY" "GROQ_API_KEY" "NULLION_OPENAI_API_KEY" "OPENAI_API_KEY" ;;
        mistral) printf '%s\n' "NULLION_MISTRAL_API_KEY" "MISTRAL_API_KEY" "NULLION_OPENAI_API_KEY" "OPENAI_API_KEY" ;;
        deepseek) printf '%s\n' "NULLION_DEEPSEEK_API_KEY" "DEEPSEEK_API_KEY" "NULLION_OPENAI_API_KEY" "OPENAI_API_KEY" ;;
        xai) printf '%s\n' "NULLION_XAI_API_KEY" "XAI_API_KEY" "NULLION_OPENAI_API_KEY" "OPENAI_API_KEY" ;;
        together) printf '%s\n' "NULLION_TOGETHER_API_KEY" "TOGETHER_API_KEY" "NULLION_OPENAI_API_KEY" "OPENAI_API_KEY" ;;
        ollama) printf '%s\n' "NULLION_OLLAMA_API_KEY" "OLLAMA_API_KEY" "NULLION_OPENAI_API_KEY" "OPENAI_API_KEY" ;;
        *) printf '%s\n' "NULLION_OPENAI_API_KEY" "OPENAI_API_KEY" ;;
    esac
}

provider_key_value() {
    local provider="$1"
    local names=()
    local name
    while IFS= read -r name; do
        names+=("$name")
    done < <(provider_key_env_names "$provider")
    env_value_any "${names[@]}"
}

first_existing_provider_key_value() {
    local provider
    local value
    for provider in openai openrouter gemini groq mistral deepseek xai together ollama; do
        value="$(provider_key_value "$provider")"
        if [[ -n "$value" ]]; then
            printf '%s' "$value"
            return 0
        fi
    done
    return 0
}

stored_credential_value() {
    local field="$1"
    NULLION_ENV_FILE="$NULLION_ENV_FILE" \
    NULLION_INSTALL_DIR="$NULLION_INSTALL_DIR" \
    NULLION_CHECKPOINT_PATH="${NULLION_INSTALL_DIR}/runtime.db" \
    "$VENV_DIR/bin/python" - "$field" "$NULLION_ENV_FILE" "$NULLION_INSTALL_DIR" 2>/dev/null <<'PY' || true
import sys
from pathlib import Path

from nullion.config import load_env_file_into_environ
from nullion.credential_store import migrate_credentials_json_to_db

field = sys.argv[1]
env_path = Path(sys.argv[2])
install_dir = Path(sys.argv[3])
if env_path.exists():
    load_env_file_into_environ(env_path)
creds = migrate_credentials_json_to_db(install_dir / "credentials.json", db_path=install_dir / "runtime.db") or {}
provider = str(creds.get("provider") or "").strip()
keys = creds.get("keys")
if not isinstance(keys, dict):
    keys = {}
api_key = str(keys.get(provider) or creds.get("api_key") or "").strip()
models = creds.get("models")
if not isinstance(models, dict):
    models = {}
values = {
    "provider": provider,
    "api_key_prefix": api_key[:8],
    "api_key": api_key,
    "model": str(creds.get("model") or models.get(provider) or "").strip(),
    "base_url": str(creds.get("base_url") or "").strip(),
}
print(values.get(field, ""))
PY
}

mask_secret() {
    local value="$1"
    local visible="${2:-8}"
    if [[ -z "$value" ]]; then
        printf 'not set'
    else
        local suffix_len=4
        if (( ${#value} <= suffix_len )); then
            printf '••••'
        else
            printf '••••%s' "${value: -suffix_len}"
        fi
    fi
}

join_summary_parts() {
    local joined=""
    local part
    for part in "$@"; do
        [[ -z "$part" ]] && continue
        if [[ -n "$joined" ]]; then
            joined="${joined}, ${part}"
        else
            joined="$part"
        fi
    done
    printf '%s' "$joined"
}

existing_messaging_summary() {
    local telegram=""
    local slack=""
    local discord=""
    if [[ "$EXISTING_TELEGRAM_ENABLED" == "true" || -n "$EXISTING_TELEGRAM_TOKEN$EXISTING_TELEGRAM_CHAT_ID" ]]; then
        telegram="Telegram"
        [[ -n "$EXISTING_TELEGRAM_TOKEN" ]] && telegram+=" token $(mask_secret "$EXISTING_TELEGRAM_TOKEN" 12)"
        [[ -n "$EXISTING_TELEGRAM_CHAT_ID" ]] && telegram+=", chat $EXISTING_TELEGRAM_CHAT_ID"
    fi
    if [[ "$EXISTING_SLACK_ENABLED" == "true" || -n "$EXISTING_SLACK_BOT_TOKEN$EXISTING_SLACK_APP_TOKEN" ]]; then
        slack="Slack"
        [[ -n "$EXISTING_SLACK_BOT_TOKEN" ]] && slack+=" bot $(mask_secret "$EXISTING_SLACK_BOT_TOKEN" 10)"
        [[ -n "$EXISTING_SLACK_APP_TOKEN" ]] && slack+=", app $(mask_secret "$EXISTING_SLACK_APP_TOKEN" 10)"
        [[ -n "$EXISTING_SLACK_OPERATOR_USER_ID" ]] && slack+=", operator $EXISTING_SLACK_OPERATOR_USER_ID"
    fi
    if [[ "$EXISTING_DISCORD_ENABLED" == "true" || -n "$EXISTING_DISCORD_BOT_TOKEN" ]]; then
        discord="Discord"
        [[ -n "$EXISTING_DISCORD_BOT_TOKEN" ]] && discord+=" token $(mask_secret "$EXISTING_DISCORD_BOT_TOKEN" 10)"
    fi
    join_summary_parts "$telegram" "$slack" "$discord"
}

existing_ai_provider_summary() {
    local provider="$EXISTING_MODEL_PROVIDER"
    local parts=()
    if [[ -z "$provider" ]]; then
        if [[ -n "$EXISTING_ANTHROPIC_KEY" ]]; then
            provider="anthropic"
        elif [[ -n "$EXISTING_MODEL_BASE_URL" ]]; then
            provider="OpenAI-compatible"
        elif [[ -n "$EXISTING_OPENAI_KEY" ]]; then
            provider="openai"
        else
            provider="configured"
        fi
    fi
    parts+=("provider $provider")
    [[ -n "$EXISTING_MODEL_NAME" ]] && parts+=("model $EXISTING_MODEL_NAME")
    [[ -n "$EXISTING_MODEL_BASE_URL" ]] && parts+=("base URL $EXISTING_MODEL_BASE_URL")
    [[ -n "$EXISTING_OPENAI_KEY" ]] && parts+=("OpenAI-compatible key $(mask_secret "$EXISTING_OPENAI_KEY" 8)")
    [[ -n "$EXISTING_ANTHROPIC_KEY" ]] && parts+=("Anthropic key $(mask_secret "$EXISTING_ANTHROPIC_KEY" 10)")
    join_summary_parts "${parts[@]}"
}

env_escape_value() {
    local value="$1"
    value="${value//\\/\\\\}"
    value="${value//\"/\\\"}"
    printf '%s' "$value"
}

checkpoint_env_raw() {
    local key="$1"
    local raw_value="$2"
    local tmp_file

    mkdir -p "$NULLION_INSTALL_DIR"
    touch "$NULLION_ENV_FILE"
    chmod 600 "$NULLION_ENV_FILE"

    tmp_file="$(mktemp)"
    awk -v key="$key" -v line="${key}=${raw_value}" '
        BEGIN { written = 0 }
        $0 ~ "^" key "=" {
            if (!written) {
                print line
                written = 1
            }
            next
        }
        { print }
        END {
            if (!written) {
                print line
            }
        }
    ' "$NULLION_ENV_FILE" > "$tmp_file"
    mv "$tmp_file" "$NULLION_ENV_FILE"
    chmod 600 "$NULLION_ENV_FILE"
}

checkpoint_env_value() {
    local key="$1"
    local value="$2"
    checkpoint_env_raw "$key" "\"$(env_escape_value "$value")\""
}

checkpoint_env_value_if_set() {
    local key="$1"
    local value="${2:-}"
    if [[ -n "$value" ]]; then
        checkpoint_env_value "$key" "$value"
    fi
    return 0
}

checkpoint_plugin_setup() {
    local enabled_plugins="search_plugin,browser_plugin,workspace_plugin,media_plugin"
    local provider_bindings="search_plugin=${SEARCH_PROVIDER:-builtin_search_provider},media_plugin=local_media_provider"

    if [[ "${EMAIL_CALENDAR_ENABLED:-false}" == "true" ]]; then
        enabled_plugins="${enabled_plugins},email_plugin,calendar_plugin"
        provider_bindings="${provider_bindings},email_plugin=google_workspace_provider,calendar_plugin=google_workspace_provider"
    elif [[ "${CUSTOM_EMAIL_API_ENABLED:-false}" == "true" ]]; then
        enabled_plugins="${enabled_plugins},email_plugin"
        provider_bindings="${provider_bindings},email_plugin=custom_api_provider"
    fi
    checkpoint_env_value "NULLION_ENABLED_PLUGINS" "$enabled_plugins"
    checkpoint_env_value "NULLION_PROVIDER_BINDINGS" "$provider_bindings"
}

checkpoint_provider_setup() {
    checkpoint_env_raw "NULLION_SETUP_PROVIDER_DONE" true
    checkpoint_env_value_if_set "ANTHROPIC_API_KEY" "${ANTHROPIC_KEY:-}"
    checkpoint_env_value_if_set "OPENAI_API_KEY" "${OPENAI_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MODEL_PROVIDER" "${MODEL_PROVIDER:-}"
    checkpoint_env_value_if_set "NULLION_OPENAI_BASE_URL" "${MODEL_BASE_URL:-}"
    checkpoint_env_value_if_set "NULLION_MODEL" "${MODEL_NAME:-}"
}

checkpoint_browser_setup() {
    checkpoint_env_raw "NULLION_SETUP_BROWSER_DONE" true
    checkpoint_env_value_if_set "NULLION_BROWSER_BACKEND" "${BROWSER_BACKEND:-}"
    checkpoint_env_value_if_set "NULLION_BROWSER_CDP_URL" "${BROWSER_CDP_URL:-}"
    checkpoint_env_value_if_set "NULLION_BROWSER_PREFERRED" "${BROWSER_PREFERRED:-}"
}

checkpoint_search_setup() {
    checkpoint_env_raw "NULLION_SETUP_SEARCH_DONE" true
    checkpoint_plugin_setup
    checkpoint_env_value_if_set "NULLION_BRAVE_SEARCH_API_KEY" "${BRAVE_SEARCH_KEY:-}"
    checkpoint_env_value_if_set "NULLION_GOOGLE_SEARCH_API_KEY" "${GOOGLE_SEARCH_KEY:-}"
    checkpoint_env_value_if_set "NULLION_GOOGLE_SEARCH_CX" "${GOOGLE_SEARCH_CX:-}"
    checkpoint_env_value_if_set "NULLION_PERPLEXITY_API_KEY" "${PERPLEXITY_SEARCH_KEY:-}"
}

checkpoint_account_setup() {
    checkpoint_env_raw "NULLION_SETUP_ACCOUNT_DONE" true
    checkpoint_plugin_setup
    checkpoint_env_value_if_set "MATON_API_KEY" "${MATON_API_KEY:-}"
    checkpoint_env_value_if_set "COMPOSIO_API_KEY" "${COMPOSIO_API_KEY:-}"
    checkpoint_env_value_if_set "NANGO_SECRET_KEY" "${NANGO_SECRET_KEY:-}"
    checkpoint_env_value_if_set "ACTIVEPIECES_API_KEY" "${ACTIVEPIECES_API_KEY:-}"
    checkpoint_env_value_if_set "N8N_BASE_URL" "${N8N_BASE_URL:-}"
    checkpoint_env_value_if_set "N8N_API_KEY" "${N8N_API_KEY:-}"
    [[ "${MATON_CONNECTOR_ENABLED:-false}" == "true" ]] && checkpoint_env_value "NULLION_CONNECTOR_GATEWAY" "maton"
    checkpoint_env_value_if_set "NULLION_CUSTOM_API_BASE_URL" "${CUSTOM_API_BASE_URL:-}"
    checkpoint_env_value_if_set "NULLION_CUSTOM_API_TOKEN" "${CUSTOM_API_TOKEN:-}"
}

checkpoint_media_setup() {
    checkpoint_env_raw "NULLION_SETUP_MEDIA_DONE" true
    checkpoint_plugin_setup
    checkpoint_env_value_if_set "NULLION_MEDIA_OPENAI_API_KEY" "${MEDIA_OPENAI_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MEDIA_ANTHROPIC_API_KEY" "${MEDIA_ANTHROPIC_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MEDIA_OPENROUTER_API_KEY" "${MEDIA_OPENROUTER_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MEDIA_GEMINI_API_KEY" "${MEDIA_GEMINI_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MEDIA_GROQ_API_KEY" "${MEDIA_GROQ_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MEDIA_MISTRAL_API_KEY" "${MEDIA_MISTRAL_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MEDIA_DEEPSEEK_API_KEY" "${MEDIA_DEEPSEEK_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MEDIA_XAI_API_KEY" "${MEDIA_XAI_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MEDIA_TOGETHER_API_KEY" "${MEDIA_TOGETHER_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MEDIA_CUSTOM_API_KEY" "${MEDIA_CUSTOM_KEY:-}"
    checkpoint_env_value_if_set "NULLION_MEDIA_CUSTOM_BASE_URL" "${MEDIA_CUSTOM_BASE_URL:-}"
    checkpoint_env_value_if_set "NULLION_IMAGE_OCR_COMMAND" "${IMAGE_OCR_COMMAND:-}"
    checkpoint_env_value_if_set "NULLION_AUDIO_TRANSCRIBE_COMMAND" "${AUDIO_TRANSCRIBE_COMMAND:-}"
    checkpoint_env_value_if_set "NULLION_IMAGE_GENERATE_COMMAND" "${IMAGE_GENERATE_COMMAND:-}"
    [[ "${AUDIO_TRANSCRIBE_ENABLED:-false}" == "true" ]] && checkpoint_env_raw "NULLION_AUDIO_TRANSCRIBE_ENABLED" true
    checkpoint_env_value_if_set "NULLION_AUDIO_TRANSCRIBE_PROVIDER" "${AUDIO_TRANSCRIBE_PROVIDER:-}"
    checkpoint_env_value_if_set "NULLION_AUDIO_TRANSCRIBE_MODEL" "${AUDIO_TRANSCRIBE_MODEL:-}"
    [[ "${IMAGE_OCR_ENABLED:-false}" == "true" ]] && checkpoint_env_raw "NULLION_IMAGE_OCR_ENABLED" true
    checkpoint_env_value_if_set "NULLION_IMAGE_OCR_PROVIDER" "${IMAGE_OCR_PROVIDER:-}"
    checkpoint_env_value_if_set "NULLION_IMAGE_OCR_MODEL" "${IMAGE_OCR_MODEL:-}"
    [[ "${IMAGE_GENERATE_ENABLED:-false}" == "true" ]] && checkpoint_env_raw "NULLION_IMAGE_GENERATE_ENABLED" true
    checkpoint_env_value_if_set "NULLION_IMAGE_GENERATE_PROVIDER" "${IMAGE_GENERATE_PROVIDER:-}"
    checkpoint_env_value_if_set "NULLION_IMAGE_GENERATE_MODEL" "${IMAGE_GENERATE_MODEL:-}"
    [[ "${VIDEO_INPUT_ENABLED:-false}" == "true" ]] && checkpoint_env_raw "NULLION_VIDEO_INPUT_ENABLED" true
    checkpoint_env_value_if_set "NULLION_VIDEO_INPUT_PROVIDER" "${VIDEO_INPUT_PROVIDER:-}"
    checkpoint_env_value_if_set "NULLION_VIDEO_INPUT_MODEL" "${VIDEO_INPUT_MODEL:-}"
}

checkpoint_skill_setup() {
    checkpoint_env_raw "NULLION_SETUP_SKILLS_DONE" true
    checkpoint_env_value_if_set "NULLION_ENABLED_SKILL_PACKS" "${ENABLED_SKILL_PACKS:-}"
    if [[ -n "${ENABLED_SKILL_PACKS:-}" ]]; then
        checkpoint_env_raw "NULLION_SKILL_PACK_ACCESS_ENABLED" true
    fi
    if [[ ",${ENABLED_SKILL_PACKS:-}," == *",nullion/connector-skills,"* || "${ENABLED_SKILL_PACKS:-}" == *"api-gateway"* ]]; then
        checkpoint_env_raw "NULLION_CONNECTOR_ACCESS_ENABLED" true
    fi
}

download_whisper_cpp_model() {
    if [[ -f "$WHISPER_CPP_MODEL_PATH" ]]; then
        print_ok "Found whisper.cpp base.en model."
        return 0
    fi
    if ! command_exists curl; then
        print_info "curl not found. Download this model later:"
        print_info "$WHISPER_CPP_MODEL_URL"
        return 1
    fi
    mkdir -p "$(dirname "$WHISPER_CPP_MODEL_PATH")"
    print_info "Downloading whisper.cpp base.en model (~148 MB)..."
    if curl -fL --progress-bar "$WHISPER_CPP_MODEL_URL" -o "${WHISPER_CPP_MODEL_PATH}.tmp"; then
        mv "${WHISPER_CPP_MODEL_PATH}.tmp" "$WHISPER_CPP_MODEL_PATH"
        print_ok "Downloaded whisper.cpp base.en model."
        return 0
    fi
    rm -f "${WHISPER_CPP_MODEL_PATH}.tmp"
    print_err "Could not download the whisper.cpp model."
    return 1
}

browser_installed() {
    local browser="$1"
    if [[ "$browser" == "brave" ]]; then
        if [[ "$PLATFORM" == "macos" ]]; then
            open -Ra "Brave Browser" 2>/dev/null
            return $?
        fi
        command_exists brave-browser || command_exists brave || command_exists brave-browser-stable
        return $?
    fi
    if [[ "$browser" == "chrome" ]]; then
        if [[ "$PLATFORM" == "macos" ]]; then
            open -Ra "Google Chrome" 2>/dev/null
            return $?
        fi
        command_exists google-chrome || command_exists google-chrome-stable || command_exists chromium || command_exists chromium-browser
        return $?
    fi
    return 1
}

browser_status_label() {
    local browser="$1"
    if browser_installed "$browser"; then
        echo "installed"
    else
        echo "not detected"
    fi
}

install_playwright_runtime() {
    if [[ "${PLAYWRIGHT_RUNTIME_READY:-false}" == "true" ]]; then
        return 0
    fi
    if [[ -z "${VENV_DIR:-}" || ! -x "$VENV_DIR/bin/pip" ]]; then
        print_info "Playwright runtime will be installed after the virtual environment is ready."
        return 1
    fi
    print_info "Installing Playwright Chromium runtime so browser automation is ready when enabled..."
    "$VENV_DIR/bin/pip" install --quiet playwright || {
        print_err "Could not install the Playwright Python package."
        return 1
    }
    if "$VENV_DIR/bin/playwright" install chromium --with-deps 2>/dev/null || "$VENV_DIR/bin/playwright" install chromium; then
        PLAYWRIGHT_RUNTIME_READY=true
        print_ok "Playwright Chromium runtime ready."
        return 0
    fi
    print_err "Could not install Playwright Chromium. Re-run 'playwright install chromium' later if browser automation fails."
    return 1
}

ensure_whisper_cpp_runtime() {
    install_default_local_media_runtime
    local missing_packages=()
    if ! command_exists whisper-cli; then
        missing_packages+=("whisper-cpp")
    fi
    if ! command_exists ffmpeg; then
        missing_packages+=("ffmpeg")
    fi

    if ((${#missing_packages[@]} > 0)); then
        if [[ "$PLATFORM" == "macos" ]] && command_exists brew; then
            print_info "Installing ${missing_packages[*]} via Homebrew..."
            brew install "${missing_packages[@]}"
        else
            print_info "Install ${missing_packages[*]} for default audio transcription."
        fi
    fi

    if ! command_exists whisper-cli; then
        print_info "whisper-cli was not found. Add NULLION_AUDIO_TRANSCRIBE_COMMAND later."
        return 1
    fi
    if ! command_exists ffmpeg; then
        print_info "ffmpeg was not found. Telegram OGG/Opus voice note conversion will not be available."
        return 1
    fi
    if ! download_whisper_cpp_model; then
        print_info "Download ggml-base.en.bin later or add NULLION_AUDIO_TRANSCRIBE_COMMAND."
        return 1
    fi
    WHISPER_CPP_READY=true
    AUDIO_TRANSCRIBE_COMMAND="whisper-cli -m \"$WHISPER_CPP_MODEL_PATH\" -f {input} -nt"
    AUDIO_TRANSCRIBE_ENABLED=true
    print_ok "Audio transcription will use whisper.cpp defaults."
    return 0
}

install_default_local_media_runtime() {
    local brew_packages=()
    local apt_packages=()
    local installed_any=false

    if ! command_exists whisper-cli; then
        if [[ "$PLATFORM" == "macos" ]] && command_exists brew; then
            brew_packages+=("whisper-cpp")
        else
            print_info "whisper.cpp is not installed; install whisper-cli later to switch audio transcription to local."
        fi
    fi
    if ! command_exists ffmpeg; then
        if [[ "$PLATFORM" == "macos" ]] && command_exists brew; then
            brew_packages+=("ffmpeg")
        elif command_exists apt-get; then
            apt_packages+=("ffmpeg")
        else
            print_info "ffmpeg is not installed; install it later for audio conversion."
        fi
    fi
    if ! command_exists tesseract; then
        if [[ "$PLATFORM" == "macos" ]] && command_exists brew; then
            brew_packages+=("tesseract")
        elif command_exists apt-get; then
            apt_packages+=("tesseract-ocr")
        else
            print_info "Tesseract is not installed; install it later to switch image OCR to local."
        fi
    fi

    if ((${#brew_packages[@]} > 0)); then
        print_info "Installing local media packages via Homebrew: ${brew_packages[*]}"
        if brew install "${brew_packages[@]}"; then
            installed_any=true
        fi
    fi
    if ((${#apt_packages[@]} > 0)); then
        print_info "Installing local media packages via apt: ${apt_packages[*]}"
        if sudo apt-get update -qq && sudo apt-get install -y "${apt_packages[@]}"; then
            installed_any=true
        fi
    fi
    if command_exists whisper-cli; then
        download_whisper_cpp_model || true
    fi
    if [[ "$installed_any" == "true" || "$(command_exists whisper-cli && echo yes)$(command_exists ffmpeg && echo yes)$(command_exists tesseract && echo yes)" == *yes* ]]; then
        print_ok "Local media runtime checked. You can switch audio/OCR to local later in Settings."
    fi
}

ensure_git() {
    if command_exists git; then
        print_ok "Found git."
        return 0
    fi

    print_info "git not found. Attempting to install..."
    if [[ "$PLATFORM" == "macos" ]]; then
        if command_exists brew; then
            brew install git
        else
            print_err "git is required to clone Nullion."
            print_info "Install Git or Homebrew, then re-run this script."
            exit 1
        fi
    elif [[ "$PLATFORM" == "linux" ]]; then
        if command_exists apt-get; then
            sudo apt-get update -qq
            sudo apt-get install -y git
        elif command_exists dnf; then
            sudo dnf install -y git
        elif command_exists pacman; then
            sudo pacman -Sy --noconfirm git
        elif command_exists zypper; then
            sudo zypper install -y git
        else
            print_err "git is required to clone Nullion."
            print_info "Install Git with your package manager, then re-run this script."
            exit 1
        fi
    fi

    if ! command_exists git; then
        print_err "git installation did not finish successfully."
        exit 1
    fi
    print_ok "git installed."
}

checkout_latest_release() {
    local source_dir="$1"
    local latest_tag

    if [[ "$(git -C "$source_dir" rev-parse --is-shallow-repository 2>/dev/null || echo false)" == "true" ]]; then
        git -C "$source_dir" fetch --quiet --unshallow origin
    else
        git -C "$source_dir" fetch --quiet origin main
    fi
    git -C "$source_dir" fetch --quiet --prune --prune-tags --force origin "refs/tags/*:refs/tags/*"
    if [[ "$NULLION_VERSION" == "main" ]]; then
        git -C "$source_dir" reset --quiet --hard origin/main
        git -C "$source_dir" clean --quiet -ffd
        print_ok "Checked out main."
        return
    fi
    latest_tag="$(git -C "$source_dir" describe --tags --abbrev=0 --match "v[0-9]*" origin/main)"
    if [[ -z "$latest_tag" ]]; then
        print_err "No release tags found in Nullion repository."
        exit 1
    fi
    git -C "$source_dir" reset --quiet --hard "$latest_tag"
    git -C "$source_dir" clean --quiet -ffd
    NULLION_VERSION="${latest_tag#v}"
    print_ok "Checked out latest release $latest_tag."
}

python_version_supported() {
    local version_text="$1"
    local major minor
    if [[ "$version_text" =~ ([0-9]+)\.([0-9]+) ]]; then
        major="${BASH_REMATCH[1]}"
        minor="${BASH_REMATCH[2]}"
        [[ "$major" -eq 3 && "$minor" -ge 11 && "$minor" -le 13 ]]
        return
    fi
    return 1
}

# ── banner ────────────────────────────────────────────────────────────────────
clear 2>/dev/null || true
echo
print_logo
echo
print_chip "platform" "$OS"
print_setup_overview
echo

if ! confirm_yes "Ready to start?"; then
    echo "  Cancelled."
    exit 0
fi

# ── Step 1: Python ─────────────────────────────────────────────────────────
print_header "Step 1 of 4 — Python"

PYTHON=""
for candidate in python3.11 python3.12 python3.13 python3; do
    if command_exists "$candidate"; then
        version=$("$candidate" --version 2>&1 | awk '{print $2}')
        if python_version_supported "Python $version"; then
            PYTHON="$candidate"
            print_ok "Found $candidate ($version)"
            break
        fi
    fi
done

if [[ -z "$PYTHON" ]]; then
    print_info "Python 3.11-3.13 not found. Attempting to install..."

    if [[ "$PLATFORM" == "macos" ]]; then
        if command_exists brew; then
            print_info "Installing Python 3.12 via Homebrew..."
            brew install python@3.12
            PYTHON="python3.12"
            print_ok "Python 3.12 installed."
        else
            print_err "Homebrew not found."
            print_info "Install Homebrew from https://brew.sh, then re-run this script."
            print_info "Or download Python 3.12 directly from https://python.org"
            exit 1
        fi

    elif [[ "$PLATFORM" == "linux" ]]; then
        # Detect package manager and install Python
        if command_exists apt-get; then
            print_info "Installing python3.12 via apt..."
            sudo apt-get update -qq
            sudo apt-get install -y python3.12 python3.12-venv python3.12-dev
            PYTHON="python3.12"
        elif command_exists dnf; then
            print_info "Installing python3.12 via dnf..."
            sudo dnf install -y python3.12
            PYTHON="python3.12"
        elif command_exists pacman; then
            print_info "Installing python via pacman..."
            sudo pacman -Sy --noconfirm python
            PYTHON="python3"
        elif command_exists zypper; then
            print_info "Installing python312 via zypper..."
            sudo zypper install -y python312
            PYTHON="python3.12"
        else
            print_err "No supported package manager found (apt, dnf, pacman, zypper)."
            print_info "Please install Python 3.12 manually from https://python.org"
            exit 1
        fi

        # Verify install succeeded
        if ! command_exists "$PYTHON"; then
            print_err "Python install failed. Please install Python 3.12 manually."
            exit 1
        fi
        print_ok "Python installed: $($PYTHON --version)"
    fi
fi

# Ensure we have venv module available (some Linux distros package it separately)
if [[ "$PLATFORM" == "linux" ]]; then
    if ! "$PYTHON" -m venv --help &>/dev/null; then
        print_info "Installing python3-venv..."
        if command_exists apt-get; then
            sudo apt-get install -y python3-venv python3-pip
        fi
    fi
fi

# ── Step 2: Install Nullion ────────────────────────────────────────────────
print_header "Step 2 of 4 — Installing Nullion"

mkdir -p "$NULLION_INSTALL_DIR" "$NULLION_LOG_DIR"

# If we're running from inside a cloned repo, install from there.
# Under `curl | bash`, Bash may not expose BASH_SOURCE[0], so treat stdin as remote install.
SCRIPT_SOURCE="${BASH_SOURCE[0]-}"
SCRIPT_DIR=""
if [[ -n "$SCRIPT_SOURCE" && -f "$SCRIPT_SOURCE" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_SOURCE")" && pwd)"
fi
if [[ -n "$SCRIPT_DIR" && -f "$SCRIPT_DIR/pyproject.toml" ]]; then
    SOURCE_DIR="$SCRIPT_DIR"
    print_info "Installing from local source at $SOURCE_DIR"
else
    print_info "Cloning Nullion from GitHub..."
    ensure_git
    SOURCE_DIR="$NULLION_INSTALL_DIR/src"
    cd "$NULLION_INSTALL_DIR"
    if [[ -d "$SOURCE_DIR/.git" ]]; then
        git -C "$SOURCE_DIR" remote set-url origin "$REPO_URL" >/dev/null 2>&1 || true
        checkout_latest_release "$SOURCE_DIR"
    else
        git clone --quiet "$REPO_URL" "$SOURCE_DIR"
        print_ok "Cloned."
        checkout_latest_release "$SOURCE_DIR"
    fi
fi

VENV_DIR="$NULLION_INSTALL_DIR/venv"
if [[ -d "$VENV_DIR" ]]; then
    recreate_venv=false
    if [[ ! -x "$VENV_DIR/bin/python" ]]; then
        print_info "Existing virtual environment is incomplete. Recreating it."
        recreate_venv=true
    else
        venv_version="$("$VENV_DIR/bin/python" --version 2>&1 || true)"
        if ! python_version_supported "$venv_version"; then
            print_info "Existing virtual environment uses unsupported Python ($venv_version). Recreating it."
            recreate_venv=true
        fi
    fi
    if [[ "$recreate_venv" == "true" ]]; then
        rm -rf "$VENV_DIR"
    fi
fi

if [[ ! -d "$VENV_DIR" ]]; then
    print_info "Creating virtual environment..."
    "$PYTHON" -m venv "$VENV_DIR"
    print_ok "Virtual environment created."
fi

print_info "Installing dependencies (this may take a minute)..."
"$VENV_DIR/bin/python" -m ensurepip --upgrade
"$VENV_DIR/bin/python" -m pip install --quiet --no-cache-dir --upgrade pip
"$VENV_DIR/bin/python" -m pip install --quiet --no-cache-dir -e "$SOURCE_DIR"

verify_python_runtime() {
    "$VENV_DIR/bin/python" - <<'PY'
import pydantic_core._pydantic_core
import nullion.builder
import nullion.web_app
PY
    return $?
}

repair_pydantic_runtime() {
    if [[ "$PLATFORM" == "macos" ]]; then
        print_info "Refreshing macOS pydantic runtime..."
    else
        print_info "Repairing pydantic runtime..."
    fi
    "$VENV_DIR/bin/python" -m pip install --quiet --no-cache-dir --force-reinstall --only-binary=:all: "pydantic>=2,<3" "pydantic-core>=2,<3"
}

if [[ "$PLATFORM" == "macos" ]] || ! verify_python_runtime; then
    repair_pydantic_runtime
fi
if ! verify_python_runtime; then
    print_err "Python runtime is still broken after repair."
    print_err "This usually points to an incomplete dependency install in the ephemeral installer environment."
    exit 1
fi
"$VENV_DIR/bin/python" - <<'PY'
import PIL
import pypdf
PY
verify_python_runtime

install_playwright_runtime || true
if ! verify_python_runtime; then
    repair_pydantic_runtime
    if ! verify_python_runtime; then
        print_err "Python runtime is still broken after Playwright refresh."
        exit 1
    fi
fi
print_ok "Nullion installed."

EXISTING_KEY_STORAGE="$(env_value NULLION_KEY_STORAGE)"
choose_key_storage "$EXISTING_KEY_STORAGE"
initialize_key_storage

# ── Step 3: Capabilities ─────────────────────────────────────────────────
print_header "Step 3 of 4 — Capabilities (optional)"

echo
echo "  Nullion's web dashboard runs at http://localhost:${NULLION_WEB_PORT} — no setup needed."
echo
echo "  Next you can enable optional capabilities: messaging apps, AI provider,"
echo "  browser/search access, account/API tools, media tools, and skill packs."
echo
echo "  First, choose any messaging apps you want to connect."
echo

BOT_TOKEN=""
CHAT_ID=""
TELEGRAM_ENABLED=false
SLACK_ENABLED=false
SLACK_BOT_TOKEN=""
SLACK_APP_TOKEN=""
SLACK_SIGNING_SECRET=""
SLACK_OPERATOR_USER_ID=""
DISCORD_ENABLED=false
DISCORD_BOT_TOKEN=""
SKIP_MESSAGING_SETUP=false

EXISTING_TELEGRAM_TOKEN="$(env_value NULLION_TELEGRAM_BOT_TOKEN)"
EXISTING_TELEGRAM_CHAT_ID="$(env_value NULLION_TELEGRAM_OPERATOR_CHAT_ID)"
EXISTING_TELEGRAM_ENABLED="$(env_value NULLION_TELEGRAM_CHAT_ENABLED)"
EXISTING_SLACK_ENABLED="$(env_value NULLION_SLACK_ENABLED)"
EXISTING_SLACK_BOT_TOKEN="$(env_value NULLION_SLACK_BOT_TOKEN)"
EXISTING_SLACK_APP_TOKEN="$(env_value NULLION_SLACK_APP_TOKEN)"
EXISTING_SLACK_SIGNING_SECRET="$(env_value NULLION_SLACK_SIGNING_SECRET)"
EXISTING_SLACK_OPERATOR_USER_ID="$(env_value NULLION_SLACK_OPERATOR_USER_ID)"
EXISTING_DISCORD_ENABLED="$(env_value NULLION_DISCORD_ENABLED)"
EXISTING_DISCORD_BOT_TOKEN="$(env_value NULLION_DISCORD_BOT_TOKEN)"
EXISTING_MESSAGING_DONE="$(env_value NULLION_SETUP_MESSAGING_DONE)"

if [[ "$EXISTING_MESSAGING_DONE" == "true" || -n "$EXISTING_TELEGRAM_TOKEN$EXISTING_SLACK_BOT_TOKEN$EXISTING_DISCORD_BOT_TOKEN" ]]; then
    EXISTING_MESSAGING_SUMMARY="$(existing_messaging_summary)"
    print_info "Found existing messaging settings in $NULLION_ENV_FILE: ${EXISTING_MESSAGING_SUMMARY:-configured}."
    if confirm_yes "Use existing messaging setup instead of setting it up again?"; then
        SKIP_MESSAGING_SETUP=true
        if [[ "$EXISTING_TELEGRAM_ENABLED" == "true" || -n "$EXISTING_TELEGRAM_TOKEN$EXISTING_TELEGRAM_CHAT_ID" ]]; then
            TELEGRAM_ENABLED=true
            BOT_TOKEN="$EXISTING_TELEGRAM_TOKEN"
            CHAT_ID="$EXISTING_TELEGRAM_CHAT_ID"
        fi
        if [[ "$EXISTING_SLACK_ENABLED" == "true" || -n "$EXISTING_SLACK_BOT_TOKEN$EXISTING_SLACK_APP_TOKEN" ]]; then
            SLACK_ENABLED=true
            SLACK_BOT_TOKEN="$EXISTING_SLACK_BOT_TOKEN"
            SLACK_APP_TOKEN="$EXISTING_SLACK_APP_TOKEN"
            SLACK_SIGNING_SECRET="$EXISTING_SLACK_SIGNING_SECRET"
            SLACK_OPERATOR_USER_ID="$EXISTING_SLACK_OPERATOR_USER_ID"
        fi
        if [[ "$EXISTING_DISCORD_ENABLED" == "true" || -n "$EXISTING_DISCORD_BOT_TOKEN" ]]; then
            DISCORD_ENABLED=true
            DISCORD_BOT_TOKEN="$EXISTING_DISCORD_BOT_TOKEN"
        fi
        print_ok "Using existing messaging setup."
    fi
fi

if [[ "$SKIP_MESSAGING_SETUP" == "false" ]]; then
print_bold "  Choose messaging apps to configure:"
print_menu_item "1" "Telegram" "Best mobile setup, voice notes, and direct chats" "[recommended]"
print_menu_item "2" "Slack" "Team workspace messaging through Slack Socket Mode"
print_menu_item "3" "Discord" "Server/community bot with Message Content intent"
print_menu_item "4" "Skip" "Set up messaging later from the web dashboard"
echo
prompt_read -rp "  Select one or more [1]: " MESSAGING_CHOICES
MESSAGING_CHOICES="${MESSAGING_CHOICES:-1}"
MESSAGING_CHOICES="$(echo "$MESSAGING_CHOICES" | tr '[:upper:]' '[:lower:]' | tr -d ' ')"

if [[ "$MESSAGING_CHOICES" == *"4"* || "$MESSAGING_CHOICES" == "skip" || "$MESSAGING_CHOICES" == "none" ]]; then
    MESSAGING_CHOICES=""
    print_ok "Skipped messaging apps. You can set them up later from the web dashboard at http://localhost:${NULLION_WEB_PORT}"
fi

if [[ "$MESSAGING_CHOICES" == *"1"* || "$MESSAGING_CHOICES" == *"telegram"* ]]; then
    TELEGRAM_ENABLED=true

    # Load existing config if present
    EXISTING_TOKEN=""
    EXISTING_CHAT_ID=""
    EXISTING_TOKEN="$(env_value NULLION_TELEGRAM_BOT_TOKEN)"
    EXISTING_CHAT_ID="$(env_value NULLION_TELEGRAM_OPERATOR_CHAT_ID)"

    echo
    echo -e "  You need a Telegram bot token. Here's how to get one in ~2 minutes:"
    echo
    echo -e "  ${BOLD}1.${RESET} Open Telegram and search for  ${CYAN}@BotFather${RESET}"
    echo -e "  ${BOLD}2.${RESET} Send:  ${CYAN}/newbot${RESET}"
    echo -e "  ${BOLD}3.${RESET} Give your bot a name (e.g. \"My Nullion\")"
    echo -e "  ${BOLD}4.${RESET} Give it a username ending in 'bot' (e.g. \"my_nullion_bot\")"
    echo -e "  ${BOLD}5.${RESET} BotFather will send you a token that looks like:"
    echo -e "       ${YELLOW}1234567890:ABCdef...${RESET}"
    echo

    if [[ -n "$EXISTING_TOKEN" ]]; then
        print_info "Existing token found: $(mask_secret "$EXISTING_TOKEN")"
        if confirm "Keep this token?"; then
            BOT_TOKEN="$EXISTING_TOKEN"
        else
            EXISTING_TOKEN=""
        fi
    fi

    if [[ -z "$EXISTING_TOKEN" ]]; then
        while true; do
            echo -n "  Paste your bot token here (hidden): "
            prompt_read -rs BOT_TOKEN
            echo
            BOT_TOKEN="$(echo "$BOT_TOKEN" | tr -d ' ')"
            if [[ "$BOT_TOKEN" =~ ^[0-9]{6,}:[A-Za-z0-9_-]{20,}$ ]]; then
                print_ok "Token format looks good."
                break
            else
                print_err "That doesn't look like a valid bot token. It should match: 123456789:ABCdef..."
            fi
        done
    fi

    echo
    echo -e "  Now we need your Telegram chat ID so the bot knows who to talk to."
    echo
    echo -e "  ${BOLD}1.${RESET} In Telegram, search for ${CYAN}@userinfobot${RESET}"
    echo -e "  ${BOLD}2.${RESET} Open it and send: ${YELLOW}/start${RESET}"
    echo -e "  ${BOLD}3.${RESET} Copy the numeric Id/User ID it replies with."
    echo -e "     That number is your chat ID."
    echo

    if [[ -n "$EXISTING_CHAT_ID" ]]; then
        print_info "Existing chat ID found: $EXISTING_CHAT_ID"
        if confirm "Keep this chat ID?"; then
            CHAT_ID="$EXISTING_CHAT_ID"
        else
            EXISTING_CHAT_ID=""
        fi
    fi

    if [[ -z "$EXISTING_CHAT_ID" ]]; then
        while true; do
            prompt_read -rp "  Enter your Telegram chat ID (numbers only): " CHAT_ID
            CHAT_ID="$(echo "$CHAT_ID" | tr -d ' -')"
            if [[ "$CHAT_ID" =~ ^-?[0-9]+$ ]]; then
                print_ok "Chat ID: $CHAT_ID"
                break
            else
                print_err "That doesn't look right — it should be a number like 123456789."
            fi
        done
    fi
fi

if [[ "$MESSAGING_CHOICES" == *"2"* || "$MESSAGING_CHOICES" == *"slack"* ]]; then
    SLACK_ENABLED=true
    echo
    print_bold "  Slack setup"
    echo "  Create a Slack app with Socket Mode enabled, then add bot and app-level tokens."
    echo "  Required: bot token (xoxb-...) and app-level token (xapp-...)."
    echo
    while true; do
        prompt_read -rsp "  Slack bot token (xoxb-...): " SLACK_BOT_TOKEN
        echo
        SLACK_BOT_TOKEN="$(echo "$SLACK_BOT_TOKEN" | tr -d ' ')"
        if [[ "$SLACK_BOT_TOKEN" == xoxb-* ]]; then
            break
        fi
        print_err "That should start with xoxb-."
    done
    while true; do
        prompt_read -rsp "  Slack app-level token (xapp-...): " SLACK_APP_TOKEN
        echo
        SLACK_APP_TOKEN="$(echo "$SLACK_APP_TOKEN" | tr -d ' ')"
        if [[ "$SLACK_APP_TOKEN" == xapp-* ]]; then
            break
        fi
        print_err "That should start with xapp-."
    done
    prompt_read -rsp "  Slack signing secret (optional): " SLACK_SIGNING_SECRET
    echo
    prompt_read -rp "  Operator Slack user ID (optional, e.g. U012ABCDEF): " SLACK_OPERATOR_USER_ID
    print_ok "Slack messaging configured."
fi

if [[ "$MESSAGING_CHOICES" == *"3"* || "$MESSAGING_CHOICES" == *"discord"* ]]; then
    DISCORD_ENABLED=true
    echo
    print_bold "  Discord setup"
    echo "  Create a Discord application bot, enable Message Content intent, and paste its token."
    echo
    while true; do
        prompt_read -rsp "  Discord bot token: " DISCORD_BOT_TOKEN
        echo
        DISCORD_BOT_TOKEN="$(echo "$DISCORD_BOT_TOKEN" | tr -d ' ')"
        if [[ -n "$DISCORD_BOT_TOKEN" ]]; then
            break
        fi
        print_err "Discord needs a bot token."
    done
    print_ok "Discord messaging configured."
fi

if [[ "$TELEGRAM_ENABLED" == "true" || "$SLACK_ENABLED" == "true" || "$DISCORD_ENABLED" == "true" || "$SKIP_MESSAGING_SETUP" == "true" ]]; then
    checkpoint_env_raw "NULLION_SETUP_MESSAGING_DONE" true
    checkpoint_env_raw "NULLION_WEB_PORT" "$NULLION_WEB_PORT"
    checkpoint_env_raw "NULLION_TELEGRAM_CHAT_ENABLED" "$TELEGRAM_ENABLED"
    if [[ "$TELEGRAM_ENABLED" == "true" ]]; then
        checkpoint_env_value "NULLION_TELEGRAM_BOT_TOKEN" "$BOT_TOKEN"
        checkpoint_env_value "NULLION_TELEGRAM_OPERATOR_CHAT_ID" "$CHAT_ID"
    fi
    checkpoint_env_raw "NULLION_SLACK_ENABLED" "$SLACK_ENABLED"
    if [[ "$SLACK_ENABLED" == "true" ]]; then
        checkpoint_env_value "NULLION_SLACK_BOT_TOKEN" "$SLACK_BOT_TOKEN"
        checkpoint_env_value "NULLION_SLACK_APP_TOKEN" "$SLACK_APP_TOKEN"
        [[ -n "$SLACK_SIGNING_SECRET" ]] && checkpoint_env_value "NULLION_SLACK_SIGNING_SECRET" "$SLACK_SIGNING_SECRET"
        [[ -n "$SLACK_OPERATOR_USER_ID" ]] && checkpoint_env_value "NULLION_SLACK_OPERATOR_USER_ID" "$SLACK_OPERATOR_USER_ID"
    fi
    checkpoint_env_raw "NULLION_DISCORD_ENABLED" "$DISCORD_ENABLED"
    if [[ "$DISCORD_ENABLED" == "true" ]]; then
        checkpoint_env_value "NULLION_DISCORD_BOT_TOKEN" "$DISCORD_BOT_TOKEN"
    fi
    print_ok "Messaging setup checkpoint saved to $NULLION_ENV_FILE"
fi
fi

# ── Check for existing credentials ───────────────────────────────────────────
CREDENTIALS_FILE="$NULLION_INSTALL_DIR/credentials.json"
ANTHROPIC_KEY=""
OPENAI_KEY=""
MODEL_PROVIDER=""
MODEL_BASE_URL=""
MODEL_NAME=""
SKIP_PROVIDER=false

EXISTING_MODEL_PROVIDER="$(env_value NULLION_MODEL_PROVIDER)"
EXISTING_MODEL_BASE_URL="$(env_value_any NULLION_OPENAI_BASE_URL OPENAI_BASE_URL)"
EXISTING_MODEL_NAME="$(env_value_any NULLION_MODEL NULLION_OPENAI_MODEL OPENAI_MODEL)"
EXISTING_OPENAI_KEY="$(provider_key_value "$EXISTING_MODEL_PROVIDER")"
if [[ -z "$EXISTING_OPENAI_KEY" ]]; then
    EXISTING_OPENAI_KEY="$(first_existing_provider_key_value)"
fi
EXISTING_ANTHROPIC_KEY="$(env_value_any NULLION_ANTHROPIC_API_KEY ANTHROPIC_API_KEY)"
EXISTING_PROVIDER_DONE="$(env_value NULLION_SETUP_PROVIDER_DONE)"
if [[ "$EXISTING_PROVIDER_DONE" == "true" || -n "$EXISTING_MODEL_PROVIDER$EXISTING_MODEL_NAME$EXISTING_OPENAI_KEY$EXISTING_ANTHROPIC_KEY" ]]; then
    echo
    print_ok "Found existing AI provider settings: $(existing_ai_provider_summary)"
    if confirm_yes "Use existing AI provider setup instead of setting it up again?"; then
        SKIP_PROVIDER=true
        MODEL_PROVIDER="$EXISTING_MODEL_PROVIDER"
        MODEL_BASE_URL="$EXISTING_MODEL_BASE_URL"
        MODEL_NAME="$EXISTING_MODEL_NAME"
        OPENAI_KEY="$EXISTING_OPENAI_KEY"
        ANTHROPIC_KEY="$EXISTING_ANTHROPIC_KEY"
        print_ok "Using existing AI provider setup."
    fi
fi

_EXISTING_STORED_PROVIDER="$(stored_credential_value provider)"
_EXISTING_STORED_KEY="$(stored_credential_value api_key_prefix)"
if [[ "$SKIP_PROVIDER" == "false" && -n "$_EXISTING_STORED_PROVIDER" && -n "$_EXISTING_STORED_KEY" ]]; then
    echo
    print_ok "Found existing encrypted credentials for: $_EXISTING_STORED_PROVIDER"
    if confirm "Keep existing credentials and skip provider setup?"; then
        SKIP_PROVIDER=true
        MODEL_PROVIDER="$_EXISTING_STORED_PROVIDER"
        MODEL_BASE_URL="$(stored_credential_value base_url)"
        MODEL_NAME="$(stored_credential_value model)"
        OPENAI_KEY="$(stored_credential_value api_key)"
        print_ok "Using existing encrypted credentials."
    fi
fi

if [[ "$SKIP_PROVIDER" == "false" ]]; then

# Ask for model provider
echo
print_bold "  Choose your AI provider:"
print_menu_item "1" "OpenAI" "GPT-5.5, GPT-4.5, GPT-4o, o4-mini..." "[recommended]"
print_menu_item "2" "Anthropic" "Claude Opus 4.6, Sonnet 4.6..."
print_menu_item "3" "OpenRouter" "GPT, Gemini, Llama, Claude, DeepSeek, and many more" "[broadest]"
print_menu_item "4" "Google Gemini" "Gemini models through the OpenAI-compatible API"
print_menu_item "5" "Ollama local" "OpenAI-compatible localhost endpoint; private and low-cost"
print_menu_item "6" "Groq" "Fast hosted inference"
print_menu_item "7" "Mistral" "Mistral and Pixtral models"
print_menu_item "8" "DeepSeek" "DeepSeek chat and reasoning models"
print_menu_item "9" "xAI" "Grok models"
print_menu_item "10" "Together AI" "Open-source model hosting"
print_menu_item "11" "Local / custom endpoint" "vLLM, LM Studio, LiteLLM, or any compatible URL"
echo
prompt_read -rp "  Enter 1-11: " PROVIDER_CHOICE

case "$PROVIDER_CHOICE" in
    1)
        MODEL_PROVIDER="openai"
        MODEL_NAME="gpt-5.5"
        echo
        print_bold "  How would you like to authenticate with OpenAI?"
        print_menu_item "1" "API key" "Paste a key from platform.openai.com"
        print_menu_item "2" "OAuth" "Sign in with your OpenAI account in the browser"
        echo
        prompt_read -rp "  Enter 1 or 2: " OPENAI_AUTH_CHOICE
        echo

        if [[ "$OPENAI_AUTH_CHOICE" == "2" ]]; then
            print_info "Opening your browser for OpenAI sign-in…"
            OAUTH_TOKEN=""
            OAUTH_ERR=""
            OAUTH_TOKEN_FILE="$(mktemp)"
            if PYTHONPATH="$SOURCE_DIR/src" "$VENV_DIR/bin/python" -m nullion.auth --write-codex-access-token "$OAUTH_TOKEN_FILE" 2> >(tee /tmp/nullion_oauth_err.txt >&2); then
                OAUTH_TOKEN="$(cat "$OAUTH_TOKEN_FILE" 2>/dev/null || true)"
            fi
            rm -f "$OAUTH_TOKEN_FILE"
            OAUTH_ERR=$(cat /tmp/nullion_oauth_err.txt 2>/dev/null || true)
            if [[ -n "$OAUTH_TOKEN" ]]; then
                MODEL_PROVIDER="codex"
                OPENAI_KEY="$OAUTH_TOKEN"
                print_ok "Authenticated via OAuth."
            else
                print_err "OAuth failed: ${OAUTH_ERR:-unknown error}. Falling back to API key."
                OPENAI_AUTH_CHOICE="1"
                MODEL_PROVIDER="openai"
            fi
        fi

        if [[ "$OPENAI_AUTH_CHOICE" != "2" || -z "$OPENAI_KEY" ]]; then
            echo -e "  Get an API key at ${CYAN}https://platform.openai.com/api-keys${RESET}"
            while true; do
                echo -n "  Paste your OpenAI API key (hidden): "
                prompt_read -rs OPENAI_KEY
                echo
                if [[ "$OPENAI_KEY" =~ ^sk- ]]; then
                    print_ok "Key accepted."
                    break
                else
                    print_err "Key should start with 'sk-'. Try again."
                fi
            done
        fi
        prompt_model_name MODEL_NAME
        ;;
    2)
        MODEL_PROVIDER="anthropic"
        MODEL_NAME="claude-opus-4-6"
        echo
        echo -e "  Get an API key at ${CYAN}https://console.anthropic.com/settings/keys${RESET}"
        while true; do
            echo -n "  Paste your Anthropic API key (hidden): "
            prompt_read -rs ANTHROPIC_KEY
            echo
            if [[ "$ANTHROPIC_KEY" =~ ^sk-ant- ]]; then
                print_ok "Key accepted."
                break
            else
                print_err "Key should start with 'sk-ant-'. Try again."
            fi
        done
        prompt_model_name MODEL_NAME
        ;;
    3)
        MODEL_PROVIDER="openrouter"
        MODEL_BASE_URL="https://openrouter.ai/api/v1"
        MODEL_NAME="openai/gpt-4o"
        echo
        echo -e "  Get an API key at ${CYAN}https://openrouter.ai/keys${RESET}"
        while true; do
            echo -n "  Paste your OpenRouter API key (hidden): "
            prompt_read -rs OPENAI_KEY
            echo
            if [[ "$OPENAI_KEY" =~ ^sk-or- ]]; then
                print_ok "Key accepted."
                break
            else
                print_err "Key should start with 'sk-or-'. Try again."
            fi
        done
        prompt_model_name MODEL_NAME
        ;;
    4)
        MODEL_PROVIDER="gemini"
        MODEL_BASE_URL="https://generativelanguage.googleapis.com/v1beta/openai/"
        MODEL_NAME="models/gemini-2.5-flash"
        echo
        echo -e "  Get an API key at ${CYAN}https://aistudio.google.com/app/apikey${RESET}"
        echo -n "  Paste your Gemini API key (hidden): "
        prompt_read -rs OPENAI_KEY
        echo
        prompt_model_name MODEL_NAME
        print_ok "Gemini selected."
        ;;
    5)
        MODEL_PROVIDER="ollama"
        MODEL_BASE_URL="http://127.0.0.1:11434/v1"
        MODEL_NAME="llama3.3"
        OPENAI_KEY="ollama-local"
        echo
        print_info "Using Ollama's OpenAI-compatible endpoint at ${MODEL_BASE_URL}."
        print_info "Run 'ollama serve' and 'ollama pull ${MODEL_NAME}' if you have not already."
        prompt_model_name MODEL_NAME
        ;;
    6)
        MODEL_PROVIDER="groq"
        MODEL_BASE_URL="https://api.groq.com/openai/v1"
        MODEL_NAME="llama-3.3-70b-versatile"
        echo -e "  Get an API key at ${CYAN}https://console.groq.com/keys${RESET}"
        echo -n "  Paste your Groq API key (hidden): "
        prompt_read -rs OPENAI_KEY
        echo
        prompt_model_name MODEL_NAME
        ;;
    7)
        MODEL_PROVIDER="mistral"
        MODEL_BASE_URL="https://api.mistral.ai/v1"
        MODEL_NAME="mistral-large-latest"
        echo -e "  Get an API key at ${CYAN}https://console.mistral.ai/api-keys/${RESET}"
        echo -n "  Paste your Mistral API key (hidden): "
        prompt_read -rs OPENAI_KEY
        echo
        prompt_model_name MODEL_NAME
        ;;
    8)
        MODEL_PROVIDER="deepseek"
        MODEL_BASE_URL="https://api.deepseek.com/v1"
        MODEL_NAME="deepseek-chat"
        echo -e "  Get an API key at ${CYAN}https://platform.deepseek.com/api_keys${RESET}"
        echo -n "  Paste your DeepSeek API key (hidden): "
        prompt_read -rs OPENAI_KEY
        echo
        prompt_model_name MODEL_NAME
        ;;
    9)
        MODEL_PROVIDER="xai"
        MODEL_BASE_URL="https://api.x.ai/v1"
        MODEL_NAME="grok-4"
        echo -e "  Get an API key at ${CYAN}https://console.x.ai/${RESET}"
        echo -n "  Paste your xAI API key (hidden): "
        prompt_read -rs OPENAI_KEY
        echo
        prompt_model_name MODEL_NAME
        ;;
    10)
        MODEL_PROVIDER="together"
        MODEL_BASE_URL="https://api.together.xyz/v1"
        MODEL_NAME="meta-llama/Llama-3.3-70B-Instruct-Turbo"
        echo -e "  Get an API key at ${CYAN}https://api.together.xyz/settings/api-keys${RESET}"
        echo -n "  Paste your Together API key (hidden): "
        prompt_read -rs OPENAI_KEY
        echo
        prompt_model_name MODEL_NAME
        ;;
    11)
        MODEL_PROVIDER="custom"
        echo
        prompt_read -rp "  OpenAI-compatible base URL (e.g. http://localhost:1234/v1): " MODEL_BASE_URL
        prompt_read -rp "  Model name: " MODEL_NAME
        if confirm "Does this endpoint require an API key?"; then
            echo -n "  Paste API key (hidden): "
            prompt_read -rs OPENAI_KEY
            echo
        else
            OPENAI_KEY="local"
        fi
        ;;
esac

fi  # end SKIP_PROVIDER

checkpoint_provider_setup
print_ok "AI provider setup checkpoint saved to $NULLION_ENV_FILE"

# ── Browser setup ──────────────────────────────────────────────────────────
echo
print_bold "  Would you like Nullion to control a browser?"
echo "  This lets it browse the web, fill forms, and take screenshots on your behalf."
echo
BROWSER_BACKEND=""
BROWSER_CDP_URL=""
BROWSER_PREFERRED=""
BROWSER_EXTRA_NOTE=""
SKIP_BROWSER_SETUP=false

EXISTING_BROWSER_BACKEND="$(env_value NULLION_BROWSER_BACKEND)"
EXISTING_BROWSER_CDP_URL="$(env_value NULLION_BROWSER_CDP_URL)"
EXISTING_BROWSER_PREFERRED="$(env_value NULLION_BROWSER_PREFERRED)"
EXISTING_BROWSER_DONE="$(env_value NULLION_SETUP_BROWSER_DONE)"
if [[ "$EXISTING_BROWSER_DONE" == "true" || -n "$EXISTING_BROWSER_BACKEND$EXISTING_BROWSER_CDP_URL$EXISTING_BROWSER_PREFERRED" ]]; then
    print_info "Found existing browser setup: ${EXISTING_BROWSER_PREFERRED:-${EXISTING_BROWSER_BACKEND}}"
    if confirm_yes "Use existing browser setup instead of setting it up again?"; then
        SKIP_BROWSER_SETUP=true
        BROWSER_BACKEND="$EXISTING_BROWSER_BACKEND"
        BROWSER_CDP_URL="$EXISTING_BROWSER_CDP_URL"
        BROWSER_PREFERRED="$EXISTING_BROWSER_PREFERRED"
        print_ok "Using existing browser setup."
    fi
fi

if [[ "$SKIP_BROWSER_SETUP" == "false" ]]; then
BRAVE_STATUS="$(browser_status_label brave)"
CHROME_STATUS="$(browser_status_label chrome)"
print_menu_item "1" "Attach to Brave" "Uses your existing Brave window (${BRAVE_STATUS})" "[recommended]"
print_menu_item "2" "Attach to Chrome" "Uses your existing Chrome window (${CHROME_STATUS})"
print_menu_item "3" "Headless" "Invisible Chromium running in the background"
print_menu_item "4" "None" "No browser access"
echo
prompt_read -rp "  Enter 1, 2, 3, or 4: " BROWSER_CHOICE
case "$BROWSER_CHOICE" in
    1)
        BROWSER_BACKEND="auto"
        BROWSER_CDP_URL="http://localhost:9222"
        BROWSER_PREFERRED="brave"
        print_ok "Brave selected."
        if ! browser_installed brave; then
            print_info "Brave was not detected. Install Brave or choose another browser if attach fails."
        fi
        BROWSER_EXTRA_NOTE="  Browser automation will attach to Brave on port 9222 if available, otherwise Nullion will open a visible automation window."
        ;;
    2)
        BROWSER_BACKEND="auto"
        BROWSER_CDP_URL="http://localhost:9222"
        BROWSER_PREFERRED="chrome"
        print_ok "Chrome selected."
        if ! browser_installed chrome; then
            print_info "Chrome was not detected. Install Chrome or choose another browser if attach fails."
        fi
        BROWSER_EXTRA_NOTE="  Browser automation will attach to Chrome on port 9222 if available, otherwise Nullion will open a visible automation window."
        ;;
    3)
        BROWSER_BACKEND="playwright"
        install_playwright_runtime || true
        if [[ "${PLAYWRIGHT_RUNTIME_READY:-false}" == "true" ]]; then
            print_ok "Headless browser ready."
        else
            print_info "Headless browser selected. Install Playwright Chromium later if browser automation fails."
        fi
        ;;
    *)
        print_info "No browser — skipped."
        ;;
esac
fi

checkpoint_browser_setup
print_ok "Browser setup checkpoint saved to $NULLION_ENV_FILE"

# ── Search provider setup ─────────────────────────────────────────────────
echo
print_bold "  Choose your search provider:"
SEARCH_PROVIDER="builtin_search_provider"
BRAVE_SEARCH_KEY=""
GOOGLE_SEARCH_KEY=""
GOOGLE_SEARCH_CX=""
PERPLEXITY_SEARCH_KEY=""
SKIP_SEARCH_SETUP=false

EXISTING_PROVIDER_BINDINGS="$(env_value NULLION_PROVIDER_BINDINGS)"
EXISTING_BRAVE_SEARCH_KEY="$(env_value NULLION_BRAVE_SEARCH_API_KEY)"
EXISTING_GOOGLE_SEARCH_KEY="$(env_value NULLION_GOOGLE_SEARCH_API_KEY)"
EXISTING_GOOGLE_SEARCH_CX="$(env_value NULLION_GOOGLE_SEARCH_CX)"
EXISTING_PERPLEXITY_SEARCH_KEY="$(env_value NULLION_PERPLEXITY_API_KEY)"
EXISTING_SEARCH_DONE="$(env_value NULLION_SETUP_SEARCH_DONE)"
if [[ "$EXISTING_PROVIDER_BINDINGS" =~ search_plugin=([^,]+) ]]; then
    EXISTING_SEARCH_PROVIDER="${BASH_REMATCH[1]}"
elif [[ "$EXISTING_SEARCH_DONE" == "true" ]]; then
    EXISTING_SEARCH_PROVIDER="builtin_search_provider"
fi
if [[ -n "${EXISTING_SEARCH_PROVIDER:-}" ]]; then
    print_info "Found existing search provider: $EXISTING_SEARCH_PROVIDER"
    if confirm_yes "Use existing search setup instead of setting it up again?"; then
        SKIP_SEARCH_SETUP=true
        SEARCH_PROVIDER="$EXISTING_SEARCH_PROVIDER"
        BRAVE_SEARCH_KEY="$EXISTING_BRAVE_SEARCH_KEY"
        GOOGLE_SEARCH_KEY="$EXISTING_GOOGLE_SEARCH_KEY"
        GOOGLE_SEARCH_CX="$EXISTING_GOOGLE_SEARCH_CX"
        PERPLEXITY_SEARCH_KEY="$EXISTING_PERPLEXITY_SEARCH_KEY"
        print_ok "Using existing search setup."
    fi
fi

if [[ "$SKIP_SEARCH_SETUP" == "false" ]]; then
print_menu_item "1" "Built-in local adapter" "Default search/fetch behavior; no extra key" "[default]"
print_menu_item "2" "Brave Search API" "Independent web index"
print_menu_item "3" "Google Custom Search API" "Requires API key plus search engine ID"
print_menu_item "4" "Perplexity Search API" "Ranked AI-oriented web results"
print_menu_item "5" "DuckDuckGo Instant Answers" "Keyless, but not full web search"
echo
prompt_read -rp "  Enter 1, 2, 3, 4, or 5: " SEARCH_CHOICE
case "$SEARCH_CHOICE" in
    2)
        SEARCH_PROVIDER="brave_search_provider"
        echo -e "  Get a key at ${CYAN}https://api-dashboard.search.brave.com/${RESET}"
        echo -n "  Paste your Brave Search API key (hidden): "
        prompt_read -rs BRAVE_SEARCH_KEY
        echo
        print_ok "Brave Search selected."
        ;;
    3)
        SEARCH_PROVIDER="google_custom_search_provider"
        echo -e "  Custom Search docs: ${CYAN}https://developers.google.com/custom-search/v1/overview${RESET}"
        echo -n "  Paste your Google Search API key (hidden): "
        prompt_read -rs GOOGLE_SEARCH_KEY
        echo
        prompt_read -rp "  Paste your Programmable Search Engine ID (cx): " GOOGLE_SEARCH_CX
        print_ok "Google Custom Search selected."
        ;;
    4)
        SEARCH_PROVIDER="perplexity_search_provider"
        echo -e "  Get a key at ${CYAN}https://www.perplexity.ai/settings/api${RESET}"
        echo -n "  Paste your Perplexity API key (hidden): "
        prompt_read -rs PERPLEXITY_SEARCH_KEY
        echo
        print_ok "Perplexity Search selected."
        ;;
    5)
        SEARCH_PROVIDER="duckduckgo_instant_answer_provider"
        print_ok "DuckDuckGo Instant Answers selected."
        ;;
    *)
        print_ok "Built-in search selected."
        ;;
esac
fi

checkpoint_search_setup
print_ok "Search setup checkpoint saved to $NULLION_ENV_FILE"

# ── Account / API tools setup ──────────────────────────────────────────────
echo
print_bold "  Choose account/API tools to enable:"
echo "  These add account-aware tools. Native support is available for Gmail/Google"
echo "  Calendar; connector gateways can bridge other apps when they expose a"
echo "  compatible HTTP API."
echo

EMAIL_CALENDAR_ENABLED=false
MATON_CONNECTOR_ENABLED=false
CONNECTOR_SKILLS_ENABLED=false
CUSTOM_EMAIL_API_ENABLED=false
MATON_API_KEY="$(env_value MATON_API_KEY)"
COMPOSIO_API_KEY="$(env_value COMPOSIO_API_KEY)"
NANGO_SECRET_KEY="$(env_value NANGO_SECRET_KEY)"
ACTIVEPIECES_API_KEY="$(env_value ACTIVEPIECES_API_KEY)"
N8N_API_KEY="$(env_value N8N_API_KEY)"
N8N_BASE_URL="$(env_value N8N_BASE_URL)"
CUSTOM_API_BASE_URL="$(env_value NULLION_CUSTOM_API_BASE_URL)"
CUSTOM_API_TOKEN="$(env_value NULLION_CUSTOM_API_TOKEN)"
EXISTING_ENABLED_PLUGINS="$(env_value NULLION_ENABLED_PLUGINS)"
EXISTING_ACCOUNT_DONE="$(env_value NULLION_SETUP_ACCOUNT_DONE)"
EXISTING_CONNECTOR_GATEWAY="$(env_value NULLION_CONNECTOR_GATEWAY)"
if [[ -n "$MATON_API_KEY$COMPOSIO_API_KEY$NANGO_SECRET_KEY$ACTIVEPIECES_API_KEY$N8N_API_KEY$EXISTING_CONNECTOR_GATEWAY" ]]; then
    MATON_CONNECTOR_ENABLED=true
    CONNECTOR_SKILLS_ENABLED=true
fi
if [[ "$EXISTING_ACCOUNT_DONE" == "true" || ",$EXISTING_ENABLED_PLUGINS," == *",email_plugin,"* || ",$EXISTING_ENABLED_PLUGINS," == *",calendar_plugin,"* || "$CONNECTOR_SKILLS_ENABLED" == "true" ]]; then
    print_info "Found existing account/API tools setup."
    if confirm_yes "Use existing account/API setup instead of setting it up again?"; then
        if [[ "$(env_value NULLION_PROVIDER_BINDINGS)" == *"email_plugin=custom_api_provider"* ]]; then
            CUSTOM_EMAIL_API_ENABLED=true
        elif [[ ",$EXISTING_ENABLED_PLUGINS," == *",email_plugin,"* || ",$EXISTING_ENABLED_PLUGINS," == *",calendar_plugin,"* ]]; then
            EMAIL_CALENDAR_ENABLED=true
        fi
        if [[ -n "$MATON_API_KEY$COMPOSIO_API_KEY$NANGO_SECRET_KEY$ACTIVEPIECES_API_KEY$N8N_API_KEY$EXISTING_CONNECTOR_GATEWAY" ]]; then
            MATON_CONNECTOR_ENABLED=true
            CONNECTOR_SKILLS_ENABLED=true
        fi
        print_ok "Using existing account/API setup."
    fi
fi

if [[ "$EMAIL_CALENDAR_ENABLED" == "false" && "$CUSTOM_EMAIL_API_ENABLED" == "false" && "$MATON_CONNECTOR_ENABLED" == "false" ]]; then
    print_menu_item "1" "Gmail / Google Calendar" "Local setup with Himalaya plus the Google API wrapper" "[recommended]"
    print_menu_item "2" "Connector skill credentials" "Maton, Composio, Nango, Activepieces, n8n, or custom gateway"
    print_menu_item "3" "Custom email API bridge" "Bind Nullion email tools to your own HTTP bridge"
    print_menu_item "4" "Skip" "Set up account/API tools later in the web UI"
    echo
    prompt_read -rp "  Enter 1, 2, 3, or 4 [4]: " ACCOUNT_TOOLS_CHOICE
    ACCOUNT_TOOLS_CHOICE="${ACCOUNT_TOOLS_CHOICE:-4}"
    case "$ACCOUNT_TOOLS_CHOICE" in
        1)
            EMAIL_CALENDAR_ENABLED=true
            if command_exists himalaya; then
                print_ok "Found Himalaya: $(himalaya --version 2>/dev/null | head -1)"
            else
                print_info "Himalaya is not installed on this machine."
                if [[ "$PLATFORM" == "macos" ]] && command_exists brew; then
                    if confirm "Install Himalaya now with Homebrew?"; then
                        brew install himalaya
                        print_ok "Himalaya installed."
                    else
                        print_info "Skipped Himalaya install. Install it later with: brew install himalaya"
                    fi
                else
                    print_info "Install Himalaya later from https://github.com/pimalaya/himalaya"
                    print_info "Then configure a Gmail account profile and add it in Settings → Users → Workspace connections."
                fi
            fi
            echo
            echo "  After Himalaya has a Gmail account profile, open:"
            echo "    Settings → Users → Workspace connections"
            echo "  Then add a Gmail / Google Workspace connection using that profile name."
            print_ok "Email/calendar plugins will be enabled."
            ;;
        2)
            MATON_CONNECTOR_ENABLED=true
            CONNECTOR_SKILLS_ENABLED=true
            echo
            echo "  Connector skills are broad workflow guidance for SaaS/API gateways."
            echo "  They do not grant access by themselves; setup saves credentials for the"
            echo "  connector or MCP tools you choose to use."
            print_menu_item "1" "Maton" "API gateway and MCP toolkit for many SaaS apps" "[recommended]"
            print_menu_item "2" "Composio" "MCP/direct API toolkits for connected apps"
            print_menu_item "3" "Nango" "Open-source OAuth and integration platform"
            print_menu_item "4" "Activepieces" "Open-source automation pieces"
            print_menu_item "5" "n8n" "Self-hostable workflow automation"
            print_menu_item "6" "Skip credentials" "Enable the connector skills only"
            echo
            prompt_read -rp "  Select one or more [1]: " CONNECTOR_CHOICES
            CONNECTOR_CHOICES="${CONNECTOR_CHOICES:-1}"
            CONNECTOR_CHOICES_NORMALIZED="$(echo "$CONNECTOR_CHOICES" | tr -cs '0-9' ',')"
            CONNECTOR_CHOICES_NORMALIZED=",${CONNECTOR_CHOICES_NORMALIZED#,}"
            if [[ "$CONNECTOR_CHOICES_NORMALIZED" != *, ]]; then
                CONNECTOR_CHOICES_NORMALIZED="${CONNECTOR_CHOICES_NORMALIZED},"
            fi
            if [[ "$CONNECTOR_CHOICES_NORMALIZED" == *",1,"* ]]; then
                echo -n "  Maton API key (hidden): "
                prompt_read -rs MATON_API_KEY
                echo
            fi
            if [[ "$CONNECTOR_CHOICES_NORMALIZED" == *",2,"* ]]; then
                echo -n "  Composio API key (hidden): "
                prompt_read -rs COMPOSIO_API_KEY
                echo
            fi
            if [[ "$CONNECTOR_CHOICES_NORMALIZED" == *",3,"* ]]; then
                echo -n "  Nango secret key (hidden): "
                prompt_read -rs NANGO_SECRET_KEY
                echo
            fi
            if [[ "$CONNECTOR_CHOICES_NORMALIZED" == *",4,"* ]]; then
                echo -n "  Activepieces API key (hidden): "
                prompt_read -rs ACTIVEPIECES_API_KEY
                echo
            fi
            if [[ "$CONNECTOR_CHOICES_NORMALIZED" == *",5,"* ]]; then
                prompt_read -rp "  n8n base URL (e.g. http://localhost:5678): " N8N_BASE_URL
                echo -n "  n8n API key (hidden): "
                prompt_read -rs N8N_API_KEY
                echo
            fi
            print_ok "Connector/API skill pack will be enabled."
            ;;
        3)
            CUSTOM_EMAIL_API_ENABLED=true
            echo
            echo "  Nullion's custom email provider expects:"
            echo "    GET /email/search?q=...&limit=..."
            echo "    GET /email/read/{id}"
            echo "  A bridge can call Maton, Composio, n8n, Activepieces, Nango, or any API behind those endpoints."
            prompt_read -rp "  Custom API base URL: " CUSTOM_API_BASE_URL
            echo -n "  Custom API bearer token (hidden): "
            prompt_read -rs CUSTOM_API_TOKEN
            echo
            print_ok "Custom email API tools will be enabled."
            ;;
        *)
            print_info "Skipped account/API tools. You can easily enable them later in the web UI."
            ;;
    esac
fi

checkpoint_account_setup
print_ok "Account/API setup checkpoint saved to $NULLION_ENV_FILE"

media_model_supports() {
    local capability="$1"
    local provider="$(echo "${2:-}" | tr '[:upper:]' '[:lower:]')"
    local model="$(echo "${3:-}" | tr '[:upper:]' '[:lower:]')"
    [[ -z "$provider" || -z "$model" ]] && return 1
    case "$capability" in
        audio)
            [[ "$provider" =~ ^(openai|groq|custom)$ && "$model" =~ (transcribe|whisper|audio) ]]
            ;;
        image_ocr)
            [[ "$provider" =~ ^(anthropic|codex)$ || "$model" =~ (gpt-4o|gpt-4\.1|gpt-5|vision|vl|llava|pixtral|gemini|claude|sonnet|opus|haiku) ]]
            ;;
        image_generate)
            if [[ "$provider" == "openai" ]]; then
                [[ "$model" =~ (gpt-image|dall-e|image) ]]
            else
                [[ "$model" =~ (image|imagen|flux|stable-diffusion|sdxl) || "$provider" == "custom" ]]
            fi
            ;;
        video)
            if [[ "$provider" == "openai" ]]; then
                [[ "$model" =~ (gpt-4o|gpt-4\.1|gpt-5|video|sora) ]]
            else
                [[ "$model" =~ (video|veo|gemini|vision|vl) ]]
            fi
            ;;
        *)
            return 1
            ;;
    esac
}

current_media_model_usable() {
    local provider="$(echo "${1:-}" | tr '[:upper:]' '[:lower:]')"
    local key
    key="$(get_media_provider_key "$provider")"
    if [[ "$provider" == "openai" ]]; then
        [[ "$key" == sk-* ]]
    elif [[ "$provider" == "codex" ]]; then
        return 1
    else
        [[ -n "$key" ]]
    fi
}

media_provider_default_model() {
    local capability="$1"
    local provider="$2"
    case "$capability:$provider" in
        audio:openai) echo "gpt-4o-transcribe" ;;
        audio:groq) echo "whisper-large-v3-turbo" ;;
        image_ocr:openai) echo "gpt-4o" ;;
        image_ocr:anthropic) echo "claude-sonnet-4-6" ;;
        image_ocr:openrouter) echo "openai/gpt-4o" ;;
        image_ocr:gemini) echo "models/gemini-2.5-flash" ;;
        image_ocr:mistral) echo "pixtral-large-latest" ;;
        image_generate:openai) echo "gpt-image-1" ;;
        image_generate:openrouter) echo "google/gemini-3.1-flash-image-preview" ;;
        image_generate:gemini) echo "gemini-3.1-flash-image-preview" ;;
        image_generate:xai) echo "grok-2-image" ;;
        image_generate:together) echo "black-forest-labs/FLUX.1-schnell-Free" ;;
        video:openai) echo "gpt-4o" ;;
        video:openrouter) echo "openai/gpt-4o" ;;
        video:gemini) echo "models/gemini-2.5-flash" ;;
        *) echo "" ;;
    esac
}

set_media_provider_key() {
    local provider="$1"
    local key="$2"
    case "$provider" in
        anthropic) MEDIA_ANTHROPIC_KEY="$key" ;;
        openai) MEDIA_OPENAI_KEY="$key" ;;
        openrouter) MEDIA_OPENROUTER_KEY="$key" ;;
        gemini) MEDIA_GEMINI_KEY="$key" ;;
        groq) MEDIA_GROQ_KEY="$key" ;;
        mistral) MEDIA_MISTRAL_KEY="$key" ;;
        deepseek) MEDIA_DEEPSEEK_KEY="$key" ;;
        xai) MEDIA_XAI_KEY="$key" ;;
        together) MEDIA_TOGETHER_KEY="$key" ;;
        custom) MEDIA_CUSTOM_KEY="$key" ;;
    esac
}

get_media_provider_key() {
    case "$1" in
        anthropic) echo "${MEDIA_ANTHROPIC_KEY:-$ANTHROPIC_KEY}" ;;
        openai) echo "${MEDIA_OPENAI_KEY:-$OPENAI_KEY}" ;;
        openrouter) echo "${MEDIA_OPENROUTER_KEY:-}" ;;
        gemini) echo "${MEDIA_GEMINI_KEY:-}" ;;
        groq) echo "${MEDIA_GROQ_KEY:-}" ;;
        mistral) echo "${MEDIA_MISTRAL_KEY:-}" ;;
        deepseek) echo "${MEDIA_DEEPSEEK_KEY:-}" ;;
        xai) echo "${MEDIA_XAI_KEY:-}" ;;
        together) echo "${MEDIA_TOGETHER_KEY:-}" ;;
        custom) echo "${MEDIA_CUSTOM_KEY:-}" ;;
        *) echo "" ;;
    esac
}

prompt_media_api_key() {
    local provider="$1"
    local key_url="$2"
    local key
    key="$(get_media_provider_key "$provider")"
    if [[ "$provider" == "openai" && "$key" != sk-* ]]; then
        key=""
        print_info "OpenAI OAuth sign-in cannot be reused for media API calls; paste an API key for this media model."
    fi
    if [[ -z "$key" ]]; then
        echo -e "  Get an API key at ${CYAN}${key_url}${RESET}"
        echo -n "  Paste $(media_provider_label "$provider") media API key (hidden): "
        prompt_read -rs key
        echo
        set_media_provider_key "$provider" "$key"
    fi
}

prompt_media_api_provider() {
    local capability="$1"
    local title="$2"
    local result_prefix="$3"
    local default_provider="$4"
    local default_model="$5"
    local allow_openai_only="${6:-false}"
    local choice provider model key_url

    echo
    print_bold "  ${title} API provider"
    print_menu_item "1" "OpenAI" "OpenAI platform API key"
    if [[ "$capability" == "audio" && "$allow_openai_only" != "true" ]]; then
        print_menu_item "2" "Groq" "OpenAI-compatible transcription API"
        print_menu_item "3" "Custom endpoint" "Any OpenAI-compatible audio transcription endpoint"
        prompt_read -rp "  Enter 1-3 [1]: " choice
    elif [[ "$capability" == "image_generate" && "$allow_openai_only" != "true" ]]; then
        print_menu_item "2" "OpenRouter" "OpenAI-compatible image model routing"
        print_menu_item "3" "Google Gemini" "Imagen through the Gemini API"
        print_menu_item "4" "xAI" "Image generation models"
        print_menu_item "5" "Together AI" "FLUX and other image models"
        print_menu_item "6" "Custom endpoint" "OpenAI-compatible base URL and model"
        prompt_read -rp "  Enter 1-6 [1]: " choice
    elif [[ "$allow_openai_only" != "true" ]]; then
        print_menu_item "2" "Anthropic" "Claude models"
        print_menu_item "3" "OpenRouter" "OpenAI-compatible model routing"
        print_menu_item "4" "Google Gemini" "OpenAI-compatible Gemini API"
        print_menu_item "5" "Mistral" "Mistral and Pixtral models"
        print_menu_item "6" "Custom endpoint" "OpenAI-compatible base URL and model"
        prompt_read -rp "  Enter 1-6 [1]: " choice
    else
        prompt_read -rp "  Enter 1 [1]: " choice
    fi
    choice="${choice:-1}"
    case "$choice" in
        2)
            if [[ "$capability" == "audio" ]]; then
                provider="groq"; key_url="https://console.groq.com/keys"
            elif [[ "$capability" == "image_generate" ]]; then
                provider="openrouter"; key_url="https://openrouter.ai/keys"
            else
                provider="anthropic"; key_url="https://console.anthropic.com/settings/keys"
            fi
            ;;
        3)
            if [[ "$capability" == "audio" ]]; then
                provider="custom"
                prompt_read -rp "  OpenAI-compatible base URL (e.g. http://localhost:1234/v1): " MEDIA_CUSTOM_BASE_URL
                prompt_read -rp "  API key setup URL (optional): " key_url
            elif [[ "$capability" == "image_generate" ]]; then
                provider="gemini"; key_url="https://aistudio.google.com/app/apikey"
            else
                provider="openrouter"; key_url="https://openrouter.ai/keys"
            fi
            ;;
        4)
            if [[ "$capability" == "image_generate" ]]; then
                provider="xai"; key_url="https://console.x.ai/"
            else
                provider="gemini"; key_url="https://aistudio.google.com/app/apikey"
            fi
            ;;
        5)
            if [[ "$capability" == "image_generate" ]]; then
                provider="together"; key_url="https://api.together.xyz/settings/api-keys"
            else
                provider="mistral"; key_url="https://console.mistral.ai/api-keys/"
            fi
            ;;
        6)
            provider="custom"
            prompt_read -rp "  OpenAI-compatible base URL (e.g. http://localhost:1234/v1): " MEDIA_CUSTOM_BASE_URL
            prompt_read -rp "  API key setup URL (optional): " key_url
            ;;
        *) provider="$default_provider"; key_url="https://platform.openai.com/api-keys" ;;
    esac
    model="$(media_provider_default_model "$capability" "$provider")"
    model="${model:-$default_model}"
    print_info "Press Enter to use the default (${model}), or type a different model name."
    prompt_read -rp "  Model [${model}]: " _MEDIA_MODEL_INPUT
    model="${_MEDIA_MODEL_INPUT:-$model}"
    if media_model_supports "$capability" "$provider" "$model"; then
        print_ok "$(media_provider_label "$provider") · ${model} supports ${title}."
    elif [[ "$provider" == "custom" ]]; then
        print_info "Custom provider selected. Nullion will use this if its OpenAI-compatible endpoint supports ${title}."
    else
        print_info "$(media_provider_label "$provider") · ${model} is not a known default for ${title}; make sure this model supports the tool."
    fi
    prompt_media_api_key "$provider" "${key_url:-your provider dashboard}"
    eval "${result_prefix}_PROVIDER=\"\$provider\""
    eval "${result_prefix}_MODEL=\"\$model\""
    eval "${result_prefix}_ENABLED=true"
}

media_provider_label() {
    case "$1" in
        anthropic) echo "Anthropic" ;;
        codex) echo "Codex" ;;
        openai) echo "OpenAI" ;;
        openrouter) echo "OpenRouter" ;;
        gemini) echo "Gemini" ;;
        ollama) echo "Ollama" ;;
        groq) echo "Groq" ;;
        mistral) echo "Mistral" ;;
        deepseek) echo "DeepSeek" ;;
        xai) echo "xAI" ;;
        together) echo "Together AI" ;;
        *) echo "${1:-provider}" ;;
    esac
}

prompt_media_model_provider() {
    local capability="$1"
    local title="$2"
    local recommended_provider="$3"
    local recommended_model="$4"
    local key_url="$5"
    local result_prefix="$6"
    local current_supported=false
    if media_model_supports "$capability" "$MODEL_PROVIDER" "$MODEL_NAME" && current_media_model_usable "$MODEL_PROVIDER"; then
        current_supported=true
    fi

    echo
    print_bold "  ${title}"
    if [[ "$current_supported" == "true" ]]; then
        print_menu_item "1" "Use current provider" "$(media_provider_label "$MODEL_PROVIDER") · ${MODEL_NAME}"
        print_menu_item "2" "Add/configure ${recommended_provider} media model" "$recommended_model"
        print_menu_item "3" "Skip" "Set this up later in the web UI"
        prompt_read -rp "  Enter 1, 2, or 3: " _MEDIA_CHOICE
        case "$_MEDIA_CHOICE" in
            1)
                eval "${result_prefix}_PROVIDER=\"\$MODEL_PROVIDER\""
                eval "${result_prefix}_MODEL=\"\$MODEL_NAME\""
                eval "${result_prefix}_ENABLED=true"
                return
                ;;
            2) ;;
            *) return ;;
        esac
    else
        print_menu_item "1" "Add/configure ${recommended_provider} media model" "$recommended_model"
        print_menu_item "2" "Skip" "Set this up later in the web UI"
        prompt_read -rp "  Enter 1 or 2: " _MEDIA_CHOICE
        [[ "$_MEDIA_CHOICE" != "1" ]] && return
    fi

    eval "${result_prefix}_PROVIDER=\"openai\""
    eval "${result_prefix}_MODEL=\"${recommended_model}\""
    eval "${result_prefix}_ENABLED=true"
    prompt_media_api_key "openai" "$key_url"
}

# ── Local media tools setup ────────────────────────────────────────────────
echo
print_bold "  Configure media tools?"
echo "  We'll set these up separately so local tools are used where they are cheap"
echo "  and fast, while image/video AI can use your current provider or a media provider."
echo

MEDIA_ENABLED=false
IMAGE_OCR_COMMAND=""
AUDIO_TRANSCRIBE_COMMAND=""
IMAGE_GENERATE_COMMAND=""
MEDIA_OPENAI_KEY=""
MEDIA_ANTHROPIC_KEY=""
MEDIA_OPENROUTER_KEY=""
MEDIA_GEMINI_KEY=""
MEDIA_GROQ_KEY=""
MEDIA_MISTRAL_KEY=""
MEDIA_DEEPSEEK_KEY=""
MEDIA_XAI_KEY=""
MEDIA_TOGETHER_KEY=""
MEDIA_CUSTOM_KEY=""
MEDIA_CUSTOM_BASE_URL=""
AUDIO_TRANSCRIBE_PROVIDER=""
AUDIO_TRANSCRIBE_MODEL=""
AUDIO_TRANSCRIBE_ENABLED=false
IMAGE_OCR_PROVIDER=""
IMAGE_OCR_MODEL=""
IMAGE_OCR_ENABLED=false
IMAGE_GENERATE_PROVIDER=""
IMAGE_GENERATE_MODEL=""
IMAGE_GENERATE_ENABLED=false
VIDEO_INPUT_PROVIDER=""
VIDEO_INPUT_MODEL=""
VIDEO_INPUT_ENABLED=false
WHISPER_CPP_READY=false
print_info "Installing default local media runtime so you can switch to local audio/OCR later."
install_default_local_media_runtime
EXISTING_MEDIA_DONE="$(env_value NULLION_SETUP_MEDIA_DONE)"
if [[ "$EXISTING_MEDIA_DONE" == "true" || ",$EXISTING_ENABLED_PLUGINS," == *",media_plugin,"* ]]; then
    print_info "Found existing media tools setup."
    if confirm_yes "Use existing media setup instead of setting it up again?"; then
        MEDIA_ENABLED=true
        MEDIA_OPENAI_KEY="$(env_value NULLION_MEDIA_OPENAI_API_KEY)"
        MEDIA_ANTHROPIC_KEY="$(env_value NULLION_MEDIA_ANTHROPIC_API_KEY)"
        MEDIA_OPENROUTER_KEY="$(env_value NULLION_MEDIA_OPENROUTER_API_KEY)"
        MEDIA_GEMINI_KEY="$(env_value NULLION_MEDIA_GEMINI_API_KEY)"
        MEDIA_GROQ_KEY="$(env_value NULLION_MEDIA_GROQ_API_KEY)"
        MEDIA_MISTRAL_KEY="$(env_value NULLION_MEDIA_MISTRAL_API_KEY)"
        MEDIA_DEEPSEEK_KEY="$(env_value NULLION_MEDIA_DEEPSEEK_API_KEY)"
        MEDIA_XAI_KEY="$(env_value NULLION_MEDIA_XAI_API_KEY)"
        MEDIA_TOGETHER_KEY="$(env_value NULLION_MEDIA_TOGETHER_API_KEY)"
        MEDIA_CUSTOM_KEY="$(env_value NULLION_MEDIA_CUSTOM_API_KEY)"
        MEDIA_CUSTOM_BASE_URL="$(env_value NULLION_MEDIA_CUSTOM_BASE_URL)"
        IMAGE_OCR_COMMAND="$(env_value NULLION_IMAGE_OCR_COMMAND)"
        AUDIO_TRANSCRIBE_COMMAND="$(env_value NULLION_AUDIO_TRANSCRIBE_COMMAND)"
        IMAGE_GENERATE_COMMAND="$(env_value NULLION_IMAGE_GENERATE_COMMAND)"
        AUDIO_TRANSCRIBE_ENABLED="$(env_value NULLION_AUDIO_TRANSCRIBE_ENABLED)"
        AUDIO_TRANSCRIBE_PROVIDER="$(env_value NULLION_AUDIO_TRANSCRIBE_PROVIDER)"
        AUDIO_TRANSCRIBE_MODEL="$(env_value NULLION_AUDIO_TRANSCRIBE_MODEL)"
        IMAGE_OCR_ENABLED="$(env_value NULLION_IMAGE_OCR_ENABLED)"
        IMAGE_OCR_PROVIDER="$(env_value NULLION_IMAGE_OCR_PROVIDER)"
        IMAGE_OCR_MODEL="$(env_value NULLION_IMAGE_OCR_MODEL)"
        IMAGE_GENERATE_ENABLED="$(env_value NULLION_IMAGE_GENERATE_ENABLED)"
        IMAGE_GENERATE_PROVIDER="$(env_value NULLION_IMAGE_GENERATE_PROVIDER)"
        IMAGE_GENERATE_MODEL="$(env_value NULLION_IMAGE_GENERATE_MODEL)"
        VIDEO_INPUT_ENABLED="$(env_value NULLION_VIDEO_INPUT_ENABLED)"
        VIDEO_INPUT_PROVIDER="$(env_value NULLION_VIDEO_INPUT_PROVIDER)"
        VIDEO_INPUT_MODEL="$(env_value NULLION_VIDEO_INPUT_MODEL)"
        print_ok "Using existing media setup."
    fi
fi
if [[ "$MEDIA_ENABLED" == "false" ]] && confirm "Configure media tools now?"; then
    if [[ "$MODEL_PROVIDER" == "codex" || ( "$MODEL_PROVIDER" == "openai" && "${OPENAI_KEY:-}" != sk-* ) ]]; then
        print_info "Codex/OpenAI OAuth works for chat sign-in, but audio transcription APIs need a provider API key or custom endpoint."
    fi
    echo
    print_bold "  Audio transcription"
    print_menu_item "1" "Local whisper.cpp" "Fast, private, no per-minute API cost" "[recommended]"
    if media_model_supports audio "$MODEL_PROVIDER" "$MODEL_NAME" && current_media_model_usable "$MODEL_PROVIDER"; then
        print_menu_item "2" "Use connected provider/model" "$(media_provider_label "$MODEL_PROVIDER") · ${MODEL_NAME} supports audio transcription"
        print_menu_item "3" "Add/configure API transcription provider" "OpenAI, Groq, or any OpenAI-compatible endpoint"
        print_menu_item "4" "Skip" "Set up audio transcription later in the web UI"
        prompt_read -rp "  Enter 1, 2, 3, or 4: " AUDIO_CHOICE
    else
        print_menu_item "2" "Add/configure API transcription provider" "OpenAI, Groq, or any OpenAI-compatible endpoint"
        print_menu_item "3" "Skip" "Set up audio transcription later in the web UI"
        prompt_read -rp "  Enter 1, 2, or 3: " AUDIO_CHOICE
    fi
    case "$AUDIO_CHOICE" in
        1)
            MEDIA_ENABLED=true
            if ensure_whisper_cpp_runtime; then
                :
            else
                print_info "Default audio transcription is not fully installed."
                if confirm "Configure a custom audio transcription command now?"; then
                    echo "  Example: whisper-cli -m \"$WHISPER_CPP_MODEL_PATH\" -f {input} -nt"
                    prompt_read -rp "  Audio command template: " AUDIO_TRANSCRIBE_COMMAND
                    [[ -n "$AUDIO_TRANSCRIBE_COMMAND" ]] && AUDIO_TRANSCRIBE_ENABLED=true
                fi
            fi
            ;;
        2)
            MEDIA_ENABLED=true
            if media_model_supports audio "$MODEL_PROVIDER" "$MODEL_NAME" && current_media_model_usable "$MODEL_PROVIDER"; then
                AUDIO_TRANSCRIBE_PROVIDER="$MODEL_PROVIDER"
                AUDIO_TRANSCRIBE_MODEL="$MODEL_NAME"
                AUDIO_TRANSCRIBE_ENABLED=true
                print_ok "$(media_provider_label "$MODEL_PROVIDER") · ${MODEL_NAME} will be used for audio transcription."
            else
                prompt_media_api_provider "audio" "Audio transcription" "AUDIO_TRANSCRIBE" "openai" "gpt-4o-transcribe" "false"
            fi
            ;;
        3)
            if media_model_supports audio "$MODEL_PROVIDER" "$MODEL_NAME" && current_media_model_usable "$MODEL_PROVIDER"; then
                MEDIA_ENABLED=true
                prompt_media_api_provider "audio" "Audio transcription" "AUDIO_TRANSCRIBE" "openai" "gpt-4o-transcribe" "false"
            fi
            ;;
    esac

    echo
    print_bold "  Image text extraction / OCR"
    print_menu_item "1" "Local Tesseract" "Fast, private, no image API cost" "[recommended]"
    if media_model_supports image_ocr "$MODEL_PROVIDER" "$MODEL_NAME" && current_media_model_usable "$MODEL_PROVIDER"; then
        print_menu_item "2" "Use current provider" "$(media_provider_label "$MODEL_PROVIDER") · ${MODEL_NAME}"
        print_menu_item "3" "Add/configure API vision provider" "OpenAI, Anthropic, OpenRouter, Gemini, Mistral, or custom"
        print_menu_item "4" "Skip" "Set up image text extraction later in the web UI"
        prompt_read -rp "  Enter 1, 2, 3, or 4: " OCR_CHOICE
    else
        print_menu_item "2" "Add/configure API vision provider" "OpenAI, Anthropic, OpenRouter, Gemini, Mistral, or custom"
        print_menu_item "3" "Skip" "Set up image text extraction later in the web UI"
        prompt_read -rp "  Enter 1, 2, or 3: " OCR_CHOICE
    fi
    case "$OCR_CHOICE" in
        1)
            MEDIA_ENABLED=true
            if command_exists tesseract; then
                IMAGE_OCR_COMMAND="tesseract {input} stdout"
                print_ok "Found Tesseract for image text extraction."
            else
                print_info "Tesseract not found. Install it later or configure NULLION_IMAGE_OCR_COMMAND in Settings."
            fi
            ;;
        2)
            MEDIA_ENABLED=true
            if media_model_supports image_ocr "$MODEL_PROVIDER" "$MODEL_NAME" && current_media_model_usable "$MODEL_PROVIDER"; then
                IMAGE_OCR_PROVIDER="$MODEL_PROVIDER"
                IMAGE_OCR_MODEL="$MODEL_NAME"
            else
                prompt_media_api_provider "image_ocr" "Image text extraction" "IMAGE_OCR" "openai" "gpt-4o" "false"
            fi
            IMAGE_OCR_ENABLED=true
            ;;
        3)
            if media_model_supports image_ocr "$MODEL_PROVIDER" "$MODEL_NAME" && current_media_model_usable "$MODEL_PROVIDER"; then
                MEDIA_ENABLED=true
                prompt_media_api_provider "image_ocr" "Image text extraction" "IMAGE_OCR" "openai" "gpt-4o" "false"
            fi
            ;;
    esac

    echo
    print_bold "  Image generation"
    if media_model_supports image_generate "$MODEL_PROVIDER" "$MODEL_NAME" && current_media_model_usable "$MODEL_PROVIDER"; then
        print_menu_item "1" "Use current provider" "$(media_provider_label "$MODEL_PROVIDER") · ${MODEL_NAME}"
        print_menu_item "2" "Add/configure API image generation provider" "OpenAI, OpenRouter, Gemini, xAI, Together, or custom"
        print_menu_item "3" "Skip" "Set up image generation later in the web UI"
        prompt_read -rp "  Enter 1, 2, or 3: " IMAGE_GEN_CHOICE
        case "$IMAGE_GEN_CHOICE" in
            1)
                MEDIA_ENABLED=true
                IMAGE_GENERATE_PROVIDER="$MODEL_PROVIDER"
                IMAGE_GENERATE_MODEL="$MODEL_NAME"
                IMAGE_GENERATE_ENABLED=true
                ;;
            2)
                MEDIA_ENABLED=true
                prompt_media_api_provider "image_generate" "Image generation" "IMAGE_GENERATE" "openai" "gpt-image-1" "false"
                ;;
        esac
    else
        print_menu_item "1" "Add/configure API image generation provider" "OpenAI, OpenRouter, Gemini, xAI, Together, or custom"
        print_menu_item "2" "Skip" "Set up image generation later in the web UI"
        prompt_read -rp "  Enter 1 or 2: " IMAGE_GEN_CHOICE
        if [[ "$IMAGE_GEN_CHOICE" == "1" ]]; then
            MEDIA_ENABLED=true
            prompt_media_api_provider "image_generate" "Image generation" "IMAGE_GENERATE" "openai" "gpt-image-1" "false"
        fi
    fi

    echo
    print_bold "  Video / rich image understanding"
    if media_model_supports video "$MODEL_PROVIDER" "$MODEL_NAME" && current_media_model_usable "$MODEL_PROVIDER"; then
        print_menu_item "1" "Use current provider" "$(media_provider_label "$MODEL_PROVIDER") · ${MODEL_NAME}"
        print_menu_item "2" "Add/configure API vision/video provider" "OpenAI, OpenRouter, Gemini, or custom"
        print_menu_item "3" "Skip" "Set up video understanding later in the web UI"
        prompt_read -rp "  Enter 1, 2, or 3: " VIDEO_CHOICE
        case "$VIDEO_CHOICE" in
            1)
                MEDIA_ENABLED=true
                VIDEO_INPUT_PROVIDER="$MODEL_PROVIDER"
                VIDEO_INPUT_MODEL="$MODEL_NAME"
                VIDEO_INPUT_ENABLED=true
                ;;
            2)
                MEDIA_ENABLED=true
                prompt_media_api_provider "video" "Video understanding" "VIDEO_INPUT" "openai" "gpt-4o" "false"
                ;;
        esac
    else
        print_menu_item "1" "Add/configure API vision/video provider" "OpenAI, OpenRouter, Gemini, or custom"
        print_menu_item "2" "Skip" "Set up video understanding later in the web UI"
        prompt_read -rp "  Enter 1 or 2: " VIDEO_CHOICE
        if [[ "$VIDEO_CHOICE" == "1" ]]; then
            MEDIA_ENABLED=true
            prompt_media_api_provider "video" "Video understanding" "VIDEO_INPUT" "openai" "gpt-4o" "false"
        fi
    fi
elif [[ "$MEDIA_ENABLED" == "false" ]]; then
    print_info "Skipped media tools. You can easily set them up later in the web UI."
fi

checkpoint_media_setup
print_ok "Media setup checkpoint saved to $NULLION_ENV_FILE"

# ── Skill pack setup ───────────────────────────────────────────────────────
echo
print_bold "  Choose skill packs to enable:"
echo "  All built-in skill packs ship with Nullion and are selected by default."
echo "  Skill packs add workflow guidance only; account access still requires"
echo "  workspace-scoped provider connections and enabled tools."
echo
ENABLED_SKILL_PACKS=""
trim_skill_pack_id() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "$value"
}

add_skill_pack() {
    local pack_id="$1"
    pack_id="$(trim_skill_pack_id "$pack_id")"
    [[ -z "$pack_id" ]] && return 0
    if [[ -z "$ENABLED_SKILL_PACKS" ]]; then
        ENABLED_SKILL_PACKS="$pack_id"
    elif [[ ",$ENABLED_SKILL_PACKS," != *",$pack_id,"* ]]; then
        ENABLED_SKILL_PACKS="${ENABLED_SKILL_PACKS},${pack_id}"
    fi
}

add_skill_pack_list() {
    local raw="$1"
    local part
    local old_ifs="$IFS"
    IFS=','
    for part in $raw; do
        add_skill_pack "$part"
    done
    IFS="$old_ifs"
}

normalize_skill_pack_list() {
    ENABLED_SKILL_PACKS=""
    add_skill_pack_list "$1"
    printf '%s' "$ENABLED_SKILL_PACKS"
    ENABLED_SKILL_PACKS=""
}

print_skill_pack_list() {
    local label="$1"
    local raw="$2"
    local normalized
    local part
    normalized="$(normalize_skill_pack_list "$raw")"
    if [[ -z "$normalized" ]]; then
        print_info "${label}: none"
        return 0
    fi
    print_info "${label}:"
    local old_ifs="$IFS"
    IFS=','
    for part in $normalized; do
        echo "    - $part"
    done
    IFS="$old_ifs"
}

install_custom_skill_pack_now() {
    local source="$1"
    local pack_id="$2"
    local force_flag="${3:-false}"
    "$VENV_DIR/bin/python" -c '
import sys
from nullion.skill_pack_installer import install_skill_pack
source = sys.argv[1]
pack_id = sys.argv[2] or None
force = sys.argv[3].lower() == "true"
pack = install_skill_pack(source, pack_id=pack_id, force=force)
print(pack.pack_id)
' "$source" "$pack_id" "$force_flag"
}

request_skill_pack_choices() {
    SKILL_CHOICES=""
    if [[ ! -t 0 ]]; then
        SKILL_CHOICES="1,2,3,4,5,6,7,8,9"
        print_info "No interactive terminal detected; using all default skill packs."
        return 0
    fi

    local titles=(
        "Web research"
        "Browser automation"
        "Files and documents"
        "PDF documents"
        "Email and calendar"
        "GitHub and code review"
        "Local media"
        "Productivity and memory"
        "Connector/API skills"
        "Install custom skill pack"
        "No default skill packs"
    )
    local details=(
        "Search, fetch, source-backed answers"
        "Web navigation, forms, screenshots"
        "Local files, docs, sheets, decks"
        "PDF generation, conversion, verification, delivery"
        "Inbox triage, replies, scheduling"
        "Repos, PRs, issues, release notes"
        "Audio transcription, OCR, image workflows"
        "Tasks, routines, preferences, reminders"
        "Maton, Composio, Nango, Activepieces, n8n, custom APIs"
        "Git URL, GitHub folder, or local folder with SKILL.md"
        "Start with no enabled reference packs"
    )
    local choice_values=(1 2 3 4 5 6 7 8 9 10 11)
    local badges=("" "" "" "" "" "" "" "" "" "" "")
    local selected=(true true true true true true true true true false false)
    local current=0
    local total=${#titles[@]}
    local key
    local old_stty=""
    local alt_screen=false

    toggle_current_item() {
        if [[ "${choice_values[$current]}" == "11" ]]; then
            for ((i = 0; i < total - 1; i++)); do selected[$i]=false; done
            selected[$current]=true
        else
            if [[ "${selected[$current]}" == "true" ]]; then
                selected[$current]=false
            else
                selected[$current]=true
            fi
            selected[$((total - 1))]=false
        fi
    }
    draw_menu() {
        printf '\033[H\033[J'
        echo "  Use ↑/↓ to move, Space to select/deselect, Enter to continue."
        echo "  You can also press the visible number for single-digit items."
        echo
        for ((i = 0; i < total; i++)); do
            print_check_item "${selected[$i]}" "$([[ $i -eq $current ]] && echo true || echo false)" "$((i + 1)). ${titles[$i]}" "${details[$i]}" "${badges[$i]}"
        done
        echo
        echo -e "  ${DIM}Enter confirms the checked items.${RESET}"
    }

    old_stty="$(stty -g 2>/dev/null || true)"
    [[ -n "$old_stty" ]] && stty -echo -icanon min 1 time 0 2>/dev/null || true
    tput civis 2>/dev/null || true
    if [[ -n "${TERM:-}" && "${TERM:-}" != "dumb" ]] && tput smcup 2>/dev/null; then
        alt_screen=true
    fi
    while true; do
        draw_menu

        IFS= prompt_read -rsn1 key || key=""
        if [[ "$key" == $'\x1b' ]]; then
            local seq=""
            IFS= prompt_read -rsn2 -t 1 seq || true
            case "$seq" in
                "[A" | "OA") current=$(((current - 1 + total) % total)) ;;
                "[B" | "OB") current=$(((current + 1) % total)) ;;
            esac
        elif [[ "$key" =~ ^[1-9]$ ]]; then
            current=$((key - 1))
            toggle_current_item
        elif [[ "$key" == " " ]]; then
            toggle_current_item
        elif [[ "$key" == "" || "$key" == $'\n' || "$key" == $'\r' ]]; then
            break
        fi
    done
    [[ "$alt_screen" == "true" ]] && tput rmcup 2>/dev/null || true
    [[ -n "$old_stty" ]] && stty "$old_stty" 2>/dev/null || true
    tput cnorm 2>/dev/null || true

    local choices=()
    for ((i = 0; i < total; i++)); do
        [[ "${selected[$i]}" == "true" ]] && choices+=("${choice_values[$i]}")
    done
    if ((${#choices[@]} == 0)); then
        choices=(11)
    fi
    SKILL_CHOICES="$(IFS=,; echo "${choices[*]}")"
}

EXISTING_SKILL_PACKS="$(normalize_skill_pack_list "$(env_value NULLION_ENABLED_SKILL_PACKS)")"
EXISTING_SKILLS_DONE="$(env_value NULLION_SETUP_SKILLS_DONE)"
SKIP_SKILL_SETUP=false
if [[ "$EXISTING_SKILLS_DONE" == "true" || -n "$EXISTING_SKILL_PACKS" ]]; then
    print_skill_pack_list "Found existing skill packs" "$EXISTING_SKILL_PACKS"
    if confirm_yes "Use existing skill packs instead of choosing them again?"; then
        add_skill_pack_list "$EXISTING_SKILL_PACKS"
        SKIP_SKILL_SETUP=true
        print_ok "Using existing skill packs."
    fi
fi

if [[ "$SKIP_SKILL_SETUP" == "false" ]]; then
request_skill_pack_choices

if [[ ",${SKILL_CHOICES// /}," == *",11,"* ]]; then
    print_info "Skipped default skill packs. You can enable them later in Settings."
else
    IFS=',' read -ra _SKILL_PARTS <<< "$SKILL_CHOICES"
    for choice in "${_SKILL_PARTS[@]}"; do
        choice="$(echo "$choice" | tr -d '[:space:]')"
        case "$choice" in
            1) add_skill_pack "nullion/web-research" ;;
            2) add_skill_pack "nullion/browser-automation" ;;
            3) add_skill_pack "nullion/files-and-docs" ;;
            4) add_skill_pack "nullion/pdf-documents" ;;
            5) add_skill_pack "nullion/email-calendar" ;;
            6) add_skill_pack "nullion/github-code" ;;
            7) add_skill_pack "nullion/media-local" ;;
            8) add_skill_pack "nullion/productivity-memory" ;;
            9) add_skill_pack "nullion/connector-skills" ;;
            10)
                prompt_read -rp "  Skill pack source URL/path: " CUSTOM_SKILL_PACK_SOURCE
                prompt_read -rp "  Pack id [auto]: " CUSTOM_SKILL_PACK_ID
                if [[ -n "$CUSTOM_SKILL_PACK_SOURCE" ]]; then
                    if CUSTOM_INSTALLED_PACK_ID="$(install_custom_skill_pack_now "$CUSTOM_SKILL_PACK_SOURCE" "$CUSTOM_SKILL_PACK_ID" "true")"; then
                        add_skill_pack "$CUSTOM_INSTALLED_PACK_ID"
                        print_ok "Installed skill pack: $CUSTOM_INSTALLED_PACK_ID"
                    else
                        print_err "Could not install custom skill pack. You can add it later in Settings."
                    fi
                fi
                ;;
            "") ;;
            *) print_info "Ignoring unknown skill choice: $choice" ;;
        esac
    done
    if [[ -n "$ENABLED_SKILL_PACKS" ]]; then
        print_skill_pack_list "Skill packs enabled" "$ENABLED_SKILL_PACKS"
    else
        print_info "No skill packs selected."
    fi
fi
fi
ENABLED_SKILL_PACKS="$(normalize_skill_pack_list "$ENABLED_SKILL_PACKS")"

checkpoint_skill_setup
print_ok "Skill setup checkpoint saved to $NULLION_ENV_FILE"

# Write .env
BOT_TOKEN="${BOT_TOKEN:-$(env_value NULLION_TELEGRAM_BOT_TOKEN)}"
CHAT_ID="${CHAT_ID:-$(env_value NULLION_TELEGRAM_OPERATOR_CHAT_ID)}"
if [[ "$TELEGRAM_ENABLED" != "true" && "$(env_value NULLION_TELEGRAM_CHAT_ENABLED)" != "false" && -n "$BOT_TOKEN$CHAT_ID" ]]; then
    TELEGRAM_ENABLED=true
fi
SLACK_BOT_TOKEN="${SLACK_BOT_TOKEN:-$(env_value NULLION_SLACK_BOT_TOKEN)}"
SLACK_APP_TOKEN="${SLACK_APP_TOKEN:-$(env_value NULLION_SLACK_APP_TOKEN)}"
SLACK_SIGNING_SECRET="${SLACK_SIGNING_SECRET:-$(env_value NULLION_SLACK_SIGNING_SECRET)}"
SLACK_OPERATOR_USER_ID="${SLACK_OPERATOR_USER_ID:-$(env_value NULLION_SLACK_OPERATOR_USER_ID)}"
if [[ "$SLACK_ENABLED" != "true" && "$(env_value NULLION_SLACK_ENABLED)" != "false" && -n "$SLACK_BOT_TOKEN$SLACK_APP_TOKEN" ]]; then
    SLACK_ENABLED=true
fi
DISCORD_BOT_TOKEN="${DISCORD_BOT_TOKEN:-$(env_value NULLION_DISCORD_BOT_TOKEN)}"
if [[ "$DISCORD_ENABLED" != "true" && "$(env_value NULLION_DISCORD_ENABLED)" != "false" && -n "$DISCORD_BOT_TOKEN" ]]; then
    DISCORD_ENABLED=true
fi
MODEL_PROVIDER="${MODEL_PROVIDER:-$(env_value NULLION_MODEL_PROVIDER)}"
MODEL_BASE_URL="${MODEL_BASE_URL:-$(env_value_any NULLION_OPENAI_BASE_URL OPENAI_BASE_URL)}"
MODEL_NAME="${MODEL_NAME:-$(env_value_any NULLION_MODEL NULLION_OPENAI_MODEL OPENAI_MODEL)}"
if [[ -z "$ANTHROPIC_KEY" ]]; then
    ANTHROPIC_KEY="$(env_value_any NULLION_ANTHROPIC_API_KEY ANTHROPIC_API_KEY)"
fi
if [[ -z "$OPENAI_KEY" ]]; then
    OPENAI_KEY="$(provider_key_value "$MODEL_PROVIDER")"
fi
if [[ -z "$OPENAI_KEY" ]]; then
    OPENAI_KEY="$(first_existing_provider_key_value)"
fi

ENABLED_PLUGINS="search_plugin,browser_plugin,workspace_plugin,media_plugin"
PROVIDER_BINDINGS="search_plugin=${SEARCH_PROVIDER},media_plugin=local_media_provider"
if [[ "$EMAIL_CALENDAR_ENABLED" == "true" ]]; then
    ENABLED_PLUGINS="${ENABLED_PLUGINS},email_plugin,calendar_plugin"
    PROVIDER_BINDINGS="${PROVIDER_BINDINGS},email_plugin=google_workspace_provider,calendar_plugin=google_workspace_provider"
elif [[ "$CUSTOM_EMAIL_API_ENABLED" == "true" ]]; then
    ENABLED_PLUGINS="${ENABLED_PLUGINS},email_plugin"
    PROVIDER_BINDINGS="${PROVIDER_BINDINGS},email_plugin=custom_api_provider"
fi

{
    echo "# Nullion configuration — generated by install.sh on $(date)"
    echo "NULLION_WEB_PORT=${NULLION_WEB_PORT}"
    echo "NULLION_KEY_STORAGE=\"${NULLION_KEY_STORAGE:-local}\""
    echo "NULLION_SETUP_MESSAGING_DONE=true"
    echo "NULLION_SETUP_PROVIDER_DONE=true"
    echo "NULLION_SETUP_BROWSER_DONE=true"
    echo "NULLION_SETUP_SEARCH_DONE=true"
    echo "NULLION_SETUP_ACCOUNT_DONE=true"
    echo "NULLION_SETUP_MEDIA_DONE=true"
    echo "NULLION_SETUP_SKILLS_DONE=true"
    if [[ "$TELEGRAM_ENABLED" == "true" ]]; then
        echo "NULLION_TELEGRAM_BOT_TOKEN=\"$BOT_TOKEN\""
        echo "NULLION_TELEGRAM_OPERATOR_CHAT_ID=\"$CHAT_ID\""
        echo "NULLION_TELEGRAM_CHAT_ENABLED=true"
    else
        echo "NULLION_TELEGRAM_CHAT_ENABLED=false"
    fi
    if [[ "$SLACK_ENABLED" == "true" ]]; then
        echo "NULLION_SLACK_ENABLED=true"
        echo "NULLION_SLACK_BOT_TOKEN=\"$SLACK_BOT_TOKEN\""
        echo "NULLION_SLACK_APP_TOKEN=\"$SLACK_APP_TOKEN\""
        [[ -n "$SLACK_SIGNING_SECRET" ]] && echo "NULLION_SLACK_SIGNING_SECRET=\"$SLACK_SIGNING_SECRET\""
        [[ -n "$SLACK_OPERATOR_USER_ID" ]] && echo "NULLION_SLACK_OPERATOR_USER_ID=\"$SLACK_OPERATOR_USER_ID\""
    else
        echo "NULLION_SLACK_ENABLED=false"
    fi
    if [[ "$DISCORD_ENABLED" == "true" ]]; then
        echo "NULLION_DISCORD_ENABLED=true"
        echo "NULLION_DISCORD_BOT_TOKEN=\"$DISCORD_BOT_TOKEN\""
    else
        echo "NULLION_DISCORD_ENABLED=false"
    fi
    [[ -n "$ANTHROPIC_KEY" ]] && echo "ANTHROPIC_API_KEY=\"$ANTHROPIC_KEY\""
    [[ -n "$OPENAI_KEY" ]] && echo "OPENAI_API_KEY=\"$OPENAI_KEY\""
    [[ -n "$MODEL_PROVIDER" ]] && echo "NULLION_MODEL_PROVIDER=\"$MODEL_PROVIDER\""
    [[ -n "$MODEL_BASE_URL" ]] && echo "NULLION_OPENAI_BASE_URL=\"$MODEL_BASE_URL\""
    [[ -n "$MODEL_NAME" ]] && echo "NULLION_MODEL=\"$MODEL_NAME\""
    [[ -n "$BROWSER_BACKEND" ]] && echo "NULLION_BROWSER_BACKEND=\"$BROWSER_BACKEND\""
    [[ -n "$BROWSER_CDP_URL" ]] && echo "NULLION_BROWSER_CDP_URL=\"$BROWSER_CDP_URL\""
    [[ -n "$BROWSER_PREFERRED" ]] && echo "NULLION_BROWSER_PREFERRED=\"$BROWSER_PREFERRED\""
    [[ -n "$BRAVE_SEARCH_KEY" ]] && echo "NULLION_BRAVE_SEARCH_API_KEY=\"$BRAVE_SEARCH_KEY\""
    [[ -n "$GOOGLE_SEARCH_KEY" ]] && echo "NULLION_GOOGLE_SEARCH_API_KEY=\"$GOOGLE_SEARCH_KEY\""
    [[ -n "$GOOGLE_SEARCH_CX" ]] && echo "NULLION_GOOGLE_SEARCH_CX=\"$GOOGLE_SEARCH_CX\""
    [[ -n "$PERPLEXITY_SEARCH_KEY" ]] && echo "NULLION_PERPLEXITY_API_KEY=\"$PERPLEXITY_SEARCH_KEY\""
    [[ -n "$MATON_API_KEY" ]] && echo "MATON_API_KEY=\"$MATON_API_KEY\""
    [[ -n "$COMPOSIO_API_KEY" ]] && echo "COMPOSIO_API_KEY=\"$COMPOSIO_API_KEY\""
    [[ -n "$NANGO_SECRET_KEY" ]] && echo "NANGO_SECRET_KEY=\"$NANGO_SECRET_KEY\""
    [[ -n "$ACTIVEPIECES_API_KEY" ]] && echo "ACTIVEPIECES_API_KEY=\"$ACTIVEPIECES_API_KEY\""
    [[ -n "$N8N_BASE_URL" ]] && echo "N8N_BASE_URL=\"$N8N_BASE_URL\""
    [[ -n "$N8N_API_KEY" ]] && echo "N8N_API_KEY=\"$N8N_API_KEY\""
    [[ "$MATON_CONNECTOR_ENABLED" == "true" ]] && echo "NULLION_CONNECTOR_GATEWAY=\"maton\""
    [[ -n "$CUSTOM_API_BASE_URL" ]] && echo "NULLION_CUSTOM_API_BASE_URL=\"$CUSTOM_API_BASE_URL\""
    [[ -n "$CUSTOM_API_TOKEN" ]] && echo "NULLION_CUSTOM_API_TOKEN=\"$CUSTOM_API_TOKEN\""
    echo "NULLION_ENABLED_PLUGINS=\"${ENABLED_PLUGINS}\""
    echo "NULLION_PROVIDER_BINDINGS=\"${PROVIDER_BINDINGS}\""
    echo "NULLION_ACTIVITY_TRACE_ENABLED=true"
    echo "NULLION_TASK_PLANNER_FEED_MODE=task"
    echo "NULLION_TASK_PLANNER_FEED_ENABLED=true"
    if [[ "$MEDIA_ENABLED" == "true" ]]; then
        [[ -n "$MEDIA_OPENAI_KEY" ]] && echo "NULLION_MEDIA_OPENAI_API_KEY=\"$MEDIA_OPENAI_KEY\""
        [[ -n "$MEDIA_ANTHROPIC_KEY" ]] && echo "NULLION_MEDIA_ANTHROPIC_API_KEY=\"$MEDIA_ANTHROPIC_KEY\""
        [[ -n "$MEDIA_OPENROUTER_KEY" ]] && echo "NULLION_MEDIA_OPENROUTER_API_KEY=\"$MEDIA_OPENROUTER_KEY\""
        [[ -n "$MEDIA_GEMINI_KEY" ]] && echo "NULLION_MEDIA_GEMINI_API_KEY=\"$MEDIA_GEMINI_KEY\""
        [[ -n "$MEDIA_GROQ_KEY" ]] && echo "NULLION_MEDIA_GROQ_API_KEY=\"$MEDIA_GROQ_KEY\""
        [[ -n "$MEDIA_MISTRAL_KEY" ]] && echo "NULLION_MEDIA_MISTRAL_API_KEY=\"$MEDIA_MISTRAL_KEY\""
        [[ -n "$MEDIA_DEEPSEEK_KEY" ]] && echo "NULLION_MEDIA_DEEPSEEK_API_KEY=\"$MEDIA_DEEPSEEK_KEY\""
        [[ -n "$MEDIA_XAI_KEY" ]] && echo "NULLION_MEDIA_XAI_API_KEY=\"$MEDIA_XAI_KEY\""
        [[ -n "$MEDIA_TOGETHER_KEY" ]] && echo "NULLION_MEDIA_TOGETHER_API_KEY=\"$MEDIA_TOGETHER_KEY\""
        [[ -n "$MEDIA_CUSTOM_KEY" ]] && echo "NULLION_MEDIA_CUSTOM_API_KEY=\"$MEDIA_CUSTOM_KEY\""
        [[ -n "$MEDIA_CUSTOM_BASE_URL" ]] && echo "NULLION_MEDIA_CUSTOM_BASE_URL=\"$MEDIA_CUSTOM_BASE_URL\""
        [[ -n "$IMAGE_OCR_COMMAND" ]] && echo "NULLION_IMAGE_OCR_COMMAND=\"$IMAGE_OCR_COMMAND\""
        [[ -n "$AUDIO_TRANSCRIBE_COMMAND" ]] && echo "NULLION_AUDIO_TRANSCRIBE_COMMAND=\"$AUDIO_TRANSCRIBE_COMMAND\""
        [[ -n "$IMAGE_GENERATE_COMMAND" ]] && echo "NULLION_IMAGE_GENERATE_COMMAND=\"$IMAGE_GENERATE_COMMAND\""
        [[ "$AUDIO_TRANSCRIBE_ENABLED" == "true" ]] && echo "NULLION_AUDIO_TRANSCRIBE_ENABLED=true"
        [[ -n "$AUDIO_TRANSCRIBE_PROVIDER" ]] && echo "NULLION_AUDIO_TRANSCRIBE_PROVIDER=\"$AUDIO_TRANSCRIBE_PROVIDER\""
        [[ -n "$AUDIO_TRANSCRIBE_MODEL" ]] && echo "NULLION_AUDIO_TRANSCRIBE_MODEL=\"$AUDIO_TRANSCRIBE_MODEL\""
        [[ "$IMAGE_OCR_ENABLED" == "true" ]] && echo "NULLION_IMAGE_OCR_ENABLED=true"
        [[ -n "$IMAGE_OCR_PROVIDER" ]] && echo "NULLION_IMAGE_OCR_PROVIDER=\"$IMAGE_OCR_PROVIDER\""
        [[ -n "$IMAGE_OCR_MODEL" ]] && echo "NULLION_IMAGE_OCR_MODEL=\"$IMAGE_OCR_MODEL\""
        [[ "$IMAGE_GENERATE_ENABLED" == "true" ]] && echo "NULLION_IMAGE_GENERATE_ENABLED=true"
        [[ -n "$IMAGE_GENERATE_PROVIDER" ]] && echo "NULLION_IMAGE_GENERATE_PROVIDER=\"$IMAGE_GENERATE_PROVIDER\""
        [[ -n "$IMAGE_GENERATE_MODEL" ]] && echo "NULLION_IMAGE_GENERATE_MODEL=\"$IMAGE_GENERATE_MODEL\""
        [[ "$VIDEO_INPUT_ENABLED" == "true" ]] && echo "NULLION_VIDEO_INPUT_ENABLED=true"
        [[ -n "$VIDEO_INPUT_PROVIDER" ]] && echo "NULLION_VIDEO_INPUT_PROVIDER=\"$VIDEO_INPUT_PROVIDER\""
        [[ -n "$VIDEO_INPUT_MODEL" ]] && echo "NULLION_VIDEO_INPUT_MODEL=\"$VIDEO_INPUT_MODEL\""
    fi
    [[ -n "$ENABLED_SKILL_PACKS" ]] && echo "NULLION_ENABLED_SKILL_PACKS=\"$ENABLED_SKILL_PACKS\""
    if [[ -n "$ENABLED_SKILL_PACKS" ]]; then
        echo "NULLION_SKILL_PACK_ACCESS_ENABLED=true"
    fi
    if [[ ",$ENABLED_SKILL_PACKS," == *",nullion/connector-skills,"* || "$ENABLED_SKILL_PACKS" == *"api-gateway"* ]]; then
        echo "NULLION_CONNECTOR_ACCESS_ENABLED=true"
    fi
    echo "NULLION_LOG_LEVEL=INFO"
} > "$NULLION_ENV_FILE"
chmod 600 "$NULLION_ENV_FILE"
print_ok "Configuration saved to $NULLION_ENV_FILE"
finalize_runtime_database

# ── Step 4: Auto-start ────────────────────────────────────────────────────
print_header "Step 4 of 4 — Auto-start"

echo

if [[ "$PLATFORM" == "macos" ]]; then
    echo "  Nullion can start automatically when you log in to your Mac."
    if [[ "$TELEGRAM_ENABLED" == "true" ]]; then
        echo "  This will register the web dashboard, menu bar icon, and your Telegram operator bot."
    else
        echo "  This will register the web dashboard and menu bar icon."
    fi
    echo "  This uses launchd — the standard macOS service manager."
    echo

    if confirm_yes "Set up auto-start at login?"; then
        mkdir -p "$(dirname "$LAUNCHD_PLIST")"
        cat > "$LAUNCHD_PLIST" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$(xml_escape "$LAUNCHD_LABEL")</string>
    <key>ProgramArguments</key>
    <array>
        <string>$(xml_escape "${VENV_DIR}/bin/python")</string>
        <string>-m</string>
        <string>nullion.web_app</string>
        <string>--port</string>
        <string>$(xml_escape "$NULLION_WEB_PORT")</string>
        <string>--checkpoint</string>
        <string>$(xml_escape "${NULLION_INSTALL_DIR}/runtime.db")</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>NULLION_ENV_FILE</key>
        <string>$(xml_escape "$NULLION_ENV_FILE")</string>
        <key>PATH</key>
        <string>$(xml_escape "${VENV_DIR}/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")</string>
    </dict>
    <key>WorkingDirectory</key>
    <string>$(xml_escape "$NULLION_INSTALL_DIR")</string>
    <key>StandardOutPath</key>
    <string>$(xml_escape "${NULLION_LOG_DIR}/nullion.log")</string>
    <key>StandardErrorPath</key>
    <string>$(xml_escape "${NULLION_LOG_DIR}/nullion-error.log")</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>
</dict>
</plist>
PLIST

        WEB_LAUNCHD_CONFIGURED=false
        if launchd_register_agent "$LAUNCHD_LABEL" "$LAUNCHD_PLIST" "Web dashboard"; then
            print_ok "Web auto-start registered via launchd."
            WEB_LAUNCHD_CONFIGURED=true
        fi

        cat > "$TRAY_LAUNCHD_PLIST" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$(xml_escape "$TRAY_LAUNCHD_LABEL")</string>
    <key>ProgramArguments</key>
    <array>
        <string>$(xml_escape "${VENV_DIR}/bin/nullion-tray")</string>
        <string>--port</string>
        <string>$(xml_escape "$NULLION_WEB_PORT")</string>
        <string>--env-file</string>
        <string>$(xml_escape "$NULLION_ENV_FILE")</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>NULLION_ENV_FILE</key>
        <string>$(xml_escape "$NULLION_ENV_FILE")</string>
        <key>PATH</key>
        <string>$(xml_escape "${VENV_DIR}/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")</string>
    </dict>
    <key>WorkingDirectory</key>
    <string>$(xml_escape "$NULLION_INSTALL_DIR")</string>
    <key>StandardOutPath</key>
    <string>$(xml_escape "${NULLION_LOG_DIR}/tray.log")</string>
    <key>StandardErrorPath</key>
    <string>$(xml_escape "${NULLION_LOG_DIR}/tray-error.log")</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>ThrottleInterval</key>
    <integer>10</integer>
</dict>
</plist>
PLIST
        TRAY_LAUNCHD_CONFIGURED=false
        if launchd_register_agent "$TRAY_LAUNCHD_LABEL" "$TRAY_LAUNCHD_PLIST" "Menu bar icon"; then
            print_ok "Menu bar icon registered via launchd."
            MACOS_TRAY_CONFIGURED=true
            TRAY_LAUNCHD_CONFIGURED=true
        fi

        if [[ "$TELEGRAM_ENABLED" == "true" ]]; then
            cat > "$TELEGRAM_LAUNCHD_PLIST" << PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$(xml_escape "$TELEGRAM_LAUNCHD_LABEL")</string>
    <key>ProgramArguments</key>
    <array>
        <string>$(xml_escape "${VENV_DIR}/bin/nullion-telegram")</string>
        <string>--checkpoint</string>
        <string>$(xml_escape "${NULLION_INSTALL_DIR}/runtime.db")</string>
        <string>--env-file</string>
        <string>$(xml_escape "$NULLION_ENV_FILE")</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>NULLION_ENV_FILE</key>
        <string>$(xml_escape "$NULLION_ENV_FILE")</string>
        <key>PATH</key>
        <string>$(xml_escape "${VENV_DIR}/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")</string>
    </dict>
    <key>WorkingDirectory</key>
    <string>$(xml_escape "$NULLION_INSTALL_DIR")</string>
    <key>StandardOutPath</key>
    <string>$(xml_escape "${NULLION_LOG_DIR}/telegram.log")</string>
    <key>StandardErrorPath</key>
    <string>$(xml_escape "${NULLION_LOG_DIR}/telegram.error.log")</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>ThrottleInterval</key>
    <integer>5</integer>
</dict>
</plist>
PLIST
            TELEGRAM_LAUNCHD_CONFIGURED=false
            if launchd_register_agent "$TELEGRAM_LAUNCHD_LABEL" "$TELEGRAM_LAUNCHD_PLIST" "Telegram operator"; then
                print_ok "Telegram auto-start registered via launchd."
                TELEGRAM_LAUNCHD_CONFIGURED=true
            fi
        else
            launchctl bootout "gui/$(id -u)/${TELEGRAM_LAUNCHD_LABEL}" >/dev/null 2>&1 || true
            rm -f "$TELEGRAM_LAUNCHD_PLIST"
            print_info "Telegram auto-start disabled."
        fi

        if [[ "$SLACK_ENABLED" == "true" ]]; then
            write_chat_launchd_plist "$SLACK_LAUNCHD_PLIST" "$SLACK_LAUNCHD_LABEL" "nullion-slack" "slack" 5
            SLACK_LAUNCHD_CONFIGURED=false
            if launchd_register_agent "$SLACK_LAUNCHD_LABEL" "$SLACK_LAUNCHD_PLIST" "Slack adapter"; then
                print_ok "Slack auto-start registered via launchd."
                SLACK_LAUNCHD_CONFIGURED=true
            fi
        else
            launchctl bootout "gui/$(id -u)/${SLACK_LAUNCHD_LABEL}" >/dev/null 2>&1 || true
            rm -f "$SLACK_LAUNCHD_PLIST"
        fi

        if [[ "$DISCORD_ENABLED" == "true" ]]; then
            write_chat_launchd_plist "$DISCORD_LAUNCHD_PLIST" "$DISCORD_LAUNCHD_LABEL" "nullion-discord" "discord" 5
            DISCORD_LAUNCHD_CONFIGURED=false
            if launchd_register_agent "$DISCORD_LAUNCHD_LABEL" "$DISCORD_LAUNCHD_PLIST" "Discord adapter"; then
                print_ok "Discord auto-start registered via launchd."
                DISCORD_LAUNCHD_CONFIGURED=true
            fi
        else
            launchctl bootout "gui/$(id -u)/${DISCORD_LAUNCHD_LABEL}" >/dev/null 2>&1 || true
            rm -f "$DISCORD_LAUNCHD_PLIST"
        fi

        AUTOSTART_STOP_CMD="launchctl bootout gui/$(id -u)/${LAUNCHD_LABEL} && launchctl bootout gui/$(id -u)/${TRAY_LAUNCHD_LABEL} && launchctl bootout gui/$(id -u)/${TELEGRAM_LAUNCHD_LABEL} && launchctl bootout gui/$(id -u)/${SLACK_LAUNCHD_LABEL} && launchctl bootout gui/$(id -u)/${DISCORD_LAUNCHD_LABEL}"
        if [[ "$WEB_LAUNCHD_CONFIGURED" == "true" || "$TRAY_LAUNCHD_CONFIGURED" == "true" || "${TELEGRAM_LAUNCHD_CONFIGURED:-false}" == "true" || "${SLACK_LAUNCHD_CONFIGURED:-false}" == "true" || "${DISCORD_LAUNCHD_CONFIGURED:-false}" == "true" ]]; then
            AUTOSTART_CONFIGURED=true
        else
            AUTOSTART_CONFIGURED=false
            print_info "Auto-start was not registered. You can still start Nullion manually below."
        fi
    else
        AUTOSTART_CONFIGURED=false
    fi

elif [[ "$PLATFORM" == "linux" ]]; then
    echo "  Nullion can start automatically when you log in."
    if [[ "$TELEGRAM_ENABLED" == "true" ]]; then
        echo "  This will register the web dashboard and your Telegram operator bot."
    else
        echo "  This will register the web dashboard."
    fi
    echo "  This uses systemd user services — no root required."
    echo

    if confirm_yes "Set up auto-start at login?"; then
        # Ensure systemd user session is available
        if ! command_exists systemctl; then
            print_err "systemctl not found. Is systemd running?"
            print_info "Skipping auto-start. You can set this up manually later."
            AUTOSTART_CONFIGURED=false
        else
            mkdir -p "$SYSTEMD_USER_DIR"
            cat > "${SYSTEMD_USER_DIR}/${SYSTEMD_SERVICE}" << UNIT
[Unit]
Description=Nullion Web Dashboard
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${VENV_DIR}/bin/nullion-web --port ${NULLION_WEB_PORT} --checkpoint ${NULLION_INSTALL_DIR}/runtime.db
EnvironmentFile=${NULLION_ENV_FILE}
WorkingDirectory=${NULLION_INSTALL_DIR}
StandardOutput=append:${NULLION_LOG_DIR}/nullion.log
StandardError=append:${NULLION_LOG_DIR}/nullion-error.log
Restart=on-failure
RestartSec=10
StartLimitIntervalSec=120
StartLimitBurst=5

[Install]
WantedBy=default.target
UNIT

            # Enable lingering so service starts at boot even without interactive login
            if command_exists loginctl; then
                loginctl enable-linger "$USER" 2>/dev/null || true
            fi

            systemctl --user daemon-reload
            systemctl --user enable "$SYSTEMD_SERVICE"
            print_ok "Web auto-start registered via systemd user service."

            if [[ "$TELEGRAM_ENABLED" == "true" ]]; then
                write_chat_systemd_unit "${SYSTEMD_USER_DIR}/${TELEGRAM_SYSTEMD_SERVICE}" "Nullion Telegram Operator" "nullion-telegram" "telegram" 5
                systemctl --user daemon-reload
                systemctl --user enable "$TELEGRAM_SYSTEMD_SERVICE"
                systemctl --user restart "$TELEGRAM_SYSTEMD_SERVICE" 2>/dev/null || true
                print_ok "Telegram auto-start registered via systemd user service."
            else
                rm -f "${SYSTEMD_USER_DIR}/${TELEGRAM_SYSTEMD_SERVICE}"
            fi

            if [[ "$SLACK_ENABLED" == "true" ]]; then
                write_chat_systemd_unit "${SYSTEMD_USER_DIR}/${SLACK_SYSTEMD_SERVICE}" "Nullion Slack Adapter" "nullion-slack" "slack" 5
                systemctl --user daemon-reload
                systemctl --user enable "$SLACK_SYSTEMD_SERVICE"
                systemctl --user restart "$SLACK_SYSTEMD_SERVICE" 2>/dev/null || true
                print_ok "Slack auto-start registered via systemd user service."
            else
                rm -f "${SYSTEMD_USER_DIR}/${SLACK_SYSTEMD_SERVICE}"
            fi

            if [[ "$DISCORD_ENABLED" == "true" ]]; then
                write_chat_systemd_unit "${SYSTEMD_USER_DIR}/${DISCORD_SYSTEMD_SERVICE}" "Nullion Discord Adapter" "nullion-discord" "discord" 5
                systemctl --user daemon-reload
                systemctl --user enable "$DISCORD_SYSTEMD_SERVICE"
                systemctl --user restart "$DISCORD_SYSTEMD_SERVICE" 2>/dev/null || true
                print_ok "Discord auto-start registered via systemd user service."
            else
                rm -f "${SYSTEMD_USER_DIR}/${DISCORD_SYSTEMD_SERVICE}"
            fi

            systemctl --user daemon-reload
            print_info "Nullion will start when you log in (lingering enabled)."
            AUTOSTART_STOP_CMD="systemctl --user stop ${SYSTEMD_SERVICE} ${TELEGRAM_SYSTEMD_SERVICE} ${SLACK_SYSTEMD_SERVICE} ${DISCORD_SYSTEMD_SERVICE} && systemctl --user disable ${SYSTEMD_SERVICE} ${TELEGRAM_SYSTEMD_SERVICE} ${SLACK_SYSTEMD_SERVICE} ${DISCORD_SYSTEMD_SERVICE}"
            AUTOSTART_CONFIGURED=true
        fi
    else
        AUTOSTART_CONFIGURED=false
    fi
fi

if [[ "${AUTOSTART_CONFIGURED:-false}" == "false" ]]; then
    print_info "Skipped auto-start. You can start Nullion manually any time:"
    echo
    echo -e "    ${CYAN}source ${NULLION_ENV_FILE} && ${VENV_DIR}/bin/nullion-web --port ${NULLION_WEB_PORT} --checkpoint ${NULLION_INSTALL_DIR}/runtime.db${RESET}"
    if [[ "$TELEGRAM_ENABLED" == "true" ]]; then
        echo
        print_info "Telegram was configured but not registered for auto-start. Start it manually with:"
        echo
        echo -e "    ${CYAN}${VENV_DIR}/bin/nullion-telegram --checkpoint ${NULLION_INSTALL_DIR}/runtime.db --env-file ${NULLION_ENV_FILE}${RESET}"
    fi
    if [[ "$SLACK_ENABLED" == "true" ]]; then
        echo
        print_info "Slack was configured. Start it manually with:"
        echo
        echo -e "    ${CYAN}${VENV_DIR}/bin/nullion-slack --checkpoint ${NULLION_INSTALL_DIR}/runtime.db --env-file ${NULLION_ENV_FILE}${RESET}"
    fi
    if [[ "$DISCORD_ENABLED" == "true" ]]; then
        echo
        print_info "Discord was configured. Start it manually with:"
        echo
        echo -e "    ${CYAN}${VENV_DIR}/bin/nullion-discord --checkpoint ${NULLION_INSTALL_DIR}/runtime.db --env-file ${NULLION_ENV_FILE}${RESET}"
    fi
    echo
fi

# ── Start now ─────────────────────────────────────────────────────────────
echo
print_header "All done!"
echo
print_ok "Nullion v${NULLION_VERSION} is installed."
echo

if [[ "${MACOS_TRAY_CONFIGURED:-false}" == "true" && "$PLATFORM" == "macos" ]]; then
    open_native_webview_now
elif confirm "Open Nullion in your browser now?"; then
    # If autostart is configured on Linux, use systemctl to start
    if [[ "${AUTOSTART_CONFIGURED:-false}" == "true" && "$PLATFORM" == "linux" ]]; then
        systemctl --user start "$SYSTEMD_SERVICE"
        sleep 2
        if systemctl --user is-active --quiet "$SYSTEMD_SERVICE"; then
            print_ok "Nullion is running!"
        else
            print_err "Nullion failed to start. Check the log:"
            echo "    journalctl --user -u $SYSTEMD_SERVICE -n 50"
            echo "    tail -50 $NULLION_LOG_DIR/nullion-error.log"
        fi
    else
        print_info "Starting Nullion..."
        set -a
        # shellcheck source=/dev/null
        source "$NULLION_ENV_FILE"
        set +a
        nohup "$VENV_DIR/bin/nullion-web" --port "${NULLION_WEB_PORT}" --checkpoint "${NULLION_INSTALL_DIR}/runtime.db" \
            >> "$NULLION_LOG_DIR/nullion.log" \
            2>> "$NULLION_LOG_DIR/nullion-error.log" &
        WEB_PID=$!
        sleep 2
        if kill -0 "$WEB_PID" 2>/dev/null; then
            print_ok "Nullion is running (PID $WEB_PID)"
        else
            print_err "Nullion exited unexpectedly. Check the log:"
            echo "    tail -50 $NULLION_LOG_DIR/nullion-error.log"
        fi
    fi
    # Open browser
    echo
    echo -e "  ${BOLD}${GREEN}→  http://localhost:${NULLION_WEB_PORT}${RESET}"
    echo
    if [[ "$PLATFORM" == "macos" ]]; then
        open "http://localhost:${NULLION_WEB_PORT}" 2>/dev/null || true
    else
        xdg-open "http://localhost:${NULLION_WEB_PORT}" 2>/dev/null || true
    fi
else
    echo
    print_info "To start manually:"
    echo -e "    ${CYAN}source ${NULLION_ENV_FILE} && ${VENV_DIR}/bin/nullion-web --port ${NULLION_WEB_PORT} --checkpoint ${NULLION_INSTALL_DIR}/runtime.db${RESET}"
    echo
    echo -e "  Then open:  ${BOLD}${GREEN}http://localhost:${NULLION_WEB_PORT}${RESET}"
    echo
fi

if [[ -n "${BROWSER_EXTRA_NOTE:-}" ]]; then
    echo
    echo -e "${YELLOW}  Browser note:${RESET}"
    echo -e "  $BROWSER_EXTRA_NOTE"
fi

# ── Add venv to PATH in shell profile ────────────────────────────────────────
PATH_LINE="export PATH=\"\$HOME/.nullion/venv/bin:\$PATH\""
SHELL_PROFILE=""
if [[ "$PLATFORM" == "macos" ]]; then
    # zsh is the default on macOS Catalina+
    if [[ -f "$HOME/.zshrc" ]]; then
        SHELL_PROFILE="$HOME/.zshrc"
    elif [[ -f "$HOME/.bash_profile" ]]; then
        SHELL_PROFILE="$HOME/.bash_profile"
    fi
else
    if [[ -f "$HOME/.bashrc" ]]; then
        SHELL_PROFILE="$HOME/.bashrc"
    elif [[ -f "$HOME/.profile" ]]; then
        SHELL_PROFILE="$HOME/.profile"
    fi
fi

if [[ -n "$SHELL_PROFILE" ]]; then
    if ! grep -qF ".nullion/venv/bin" "$SHELL_PROFILE" 2>/dev/null; then
        echo "" >> "$SHELL_PROFILE"
        echo "# Nullion CLI tools" >> "$SHELL_PROFILE"
        echo "$PATH_LINE" >> "$SHELL_PROFILE"
        print_ok "Added nullion to PATH in $SHELL_PROFILE"
        echo -e "  Run ${CYAN}source $SHELL_PROFILE${RESET} or open a new terminal to use ${BOLD}nullion${RESET} directly."
    else
        print_ok "nullion already in PATH ($SHELL_PROFILE)"
    fi
fi

echo
echo -e "  Logs:    ${CYAN}${NULLION_LOG_DIR}/nullion.log${RESET}"
echo -e "  Config:  ${CYAN}${NULLION_ENV_FILE}${RESET}"
if [[ -n "${AUTOSTART_STOP_CMD:-}" ]]; then
    echo -e "  To stop: ${CYAN}${AUTOSTART_STOP_CMD}${RESET}"
fi
echo
