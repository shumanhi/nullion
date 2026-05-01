# Nulliøn

![Nullion local operator console with chat, generated artifacts, approvals, tasks, and control center](https://www.nullion.ai/assets/nullion-web-console-real.png)

A local-first AI operator console with parallel mini-agents, Sentinel approvals,
Doctor health checks, Builder skill learning, plugin integrations, and
Telegram/Slack/Discord operator control. Recent builds add workspace-scoped
approvals and schedules, LangGraph-backed turn routing, platform-aware file and
PDF delivery contracts, validated task planning, cleaner activity traces, and
model fallback configuration.

Online docs: https://www.nullion.ai/docs/

## Install

**Mac or Linux** — open Terminal and paste:

```bash
curl -fsSL "https://raw.githubusercontent.com/shumanhi/nullion/main/install.sh?$(date +%s)" | bash
```

**Windows** — open PowerShell and paste:

```powershell
irm https://raw.githubusercontent.com/shumanhi/nullion/main/install.ps1 | iex
```

The installer sets everything up, walks you through connecting your API keys, and starts Nullion automatically. Takes about a minute.

On macOS and Windows, Nullion also includes an optional tray/menu-bar companion:

```bash
nullion-tray
```

It shows the Nullion logo in the menu bar or system tray and gives you quick access to the Web UI, approvals, logs, config, and restart actions. Clicking "Open Web UI" opens a native webview window; "Open in Browser" is still available from the menu.

To remove the local install later:

```bash
nullion uninstall
```

Use `nullion uninstall --dry-run` to preview the cleanup, or `nullion uninstall --keep-data` to remove services while keeping `~/.nullion`.

## License and security

Nullion is released under the Apache License 2.0. See [LICENSE](LICENSE).

This project is provided as-is, without warranties. You are responsible for
configuring API keys, approvals, third-party integrations, enabled plugins,
file roots, browser permissions, schedules, model providers, and any actions
performed by agents or tools.

Please report vulnerabilities privately through GitHub Security Advisories:
https://github.com/shumanhi/nullion/security/advisories/new

Do not include API keys, bot tokens, OAuth tokens, private file paths, or logs
with secrets in public issues. See [SECURITY.md](SECURITY.md) for details.

<details>
<summary>Already cloned the repo?</summary>

```bash
bash install.sh
```

</details>

## Goals

- Secure by default
- Easy to install and configure
- Pluggable chat adapters — Telegram, Slack, Discord, and Web UI
- Clear everyday wording
- Parallel mini-agent execution through a warm pool
- LangGraph-backed routing for parallel turns and dependent follow-ups
- Contract-based file, PDF, image, and document delivery across web and messaging
- Global Doctor for health checks and safe recovery
- Global Sentinel for approval, boundary policy, and grant management
- Workspace-scoped approvals, crons, grants, and member connections
- Validated planner task cards, optional activity traces, and thinking summaries
- Builder proposals for reusable skills and compacted memory

## Launch feature summary

- **Parallel work** — complex requests can be decomposed into bounded subtasks,
  dispatched to warm mini-agents, and merged by a result aggregator.
- **Two-layer safety** — Sentinel separates tool capability grants from boundary
  policy for domains, file roots, accounts, workspaces, and other scoped
  resources.
- **Workspace ownership** — approvals, scheduled jobs, provider connections,
  delivery targets, memory, and grants stay attached to the initiating
  workspace or operator instead of leaking across users.
- **Visible planning and artifacts** — Verbose modes can show activity traces,
  planner task cards, compact tool outcomes, Builder/Doctor checkpoints, and
  generated image/file artifacts without
  dumping raw tracebacks into user-facing channels.
- **Delivery as a contract** — message-only tasks finish when the reply is
  delivered; attachment tasks finish only when the requested file exists and is
  surfaced as a web download or messaging attachment.
- **Self-healing runtime** — Doctor probes adapters, plugins, scheduler,
  database, browser backend, model client, and warm-pool capacity, then retries
  safe recovery before escalating.
- **Skills that improve with use** — Builder detects repeated workflows and
  proposes versioned skills for review instead of silently mutating prompts.
- **Local-first state** — runtime data, history, memory, skills, schedules,
  grants, and audit records stay under the local Nullion data directory.

## Product philosophy

Nullion is shaped around four layers:

1. **Kernel** — tiny, generic, auditable primitives. Local-first by default. No provider-shaped promises.
2. **Plugins** — optional installable tool groups (`search_plugin`, `email_plugin`, `calendar_plugin`, `browser_plugin`, …). Provider choice stays explicit and replaceable.
3. **Skill packs** — optional reference instructions such as `nullion/web-research` or `google/skills` for product/workflow know-how. They do not grant account access by themselves.
4. **Skills** — reusable procedures for when and how to use tools well. Prefer plugins when present; fall back to generic primitives when absent.
5. **Policy** — approvals, grants, principal isolation, and side-effect control.

### Agnostic design rules

- Core product behavior stays generic, not domain-shaped
- No privileged news/weather/provider-specific chat paths
- No hardcoded vertical workflows in the kernel
- The runtime only claims support for what is actually installed and allowed
- When a plugin is missing, Nullion composes generic primitives rather than pretending the plugin exists
- Core tools remain a universal fallback path; plugins improve efficiency, not basic possibility

See also:
- `docs/plugins.md`
- `docs/skill-packs.md`
- Online docs: https://www.nullion.ai/docs/

## Plugins

Plugins are optional capability packs. Search, browser automation, workspace
files, local media tools, email, calendar, and future integrations are enabled
explicitly and bound to provider adapters deliberately.

Useful commands:

```text
/plugins
/plugins available
/plugin search_plugin
/tools
/verbose status
/thinking status
```

Env-based setup:

```bash
NULLION_ENABLED_PLUGINS=search_plugin,browser_plugin,workspace_plugin,media_plugin
NULLION_PROVIDER_BINDINGS=search_plugin=builtin_search_provider,media_plugin=local_media_provider
NULLION_BROWSER_ENABLED=true
NULLION_BROWSER_BACKEND=auto
NULLION_WORKSPACE_ROOT=/Users/you/Projects
NULLION_ALLOWED_ROOTS=/Users/you/Projects,/Users/you/.nullion/.nullion-artifacts
# Optional override for Deep Agents model selection. When unset, Deep Agents
# uses the same model client Nullion already configured for mini-agents.
# NULLION_DEEP_AGENTS_MODEL=openai:gpt-5.4
# Recommended audio default: installer-managed whisper.cpp + ffmpeg with
# ggml-base.en.bin at ~/.nullion/models/ggml-base.en.bin. Leave the command
# unset unless you need a custom wrapper.
NULLION_IMAGE_OCR_COMMAND='tesseract {input} stdout'
```

See `docs/plugins.md` for the catalog, installer UX, disable/reconfigure
guidance, and plugin-author documentation.

## Skill Packs

Skill packs are optional reference instructions. They help Nullion understand
how to work with a product area, while plugins still control real capabilities
such as browser automation, search, email, calendar, and files.

Installed custom skill packs are available to every workspace when they are
instruction-only. If a pack's `SKILL.md` declares or implies API keys, OAuth,
tokens, credentials, or other account access, Nullion treats it as
auth-required and exposes its provider under Settings -> Users -> Connections
instead of making it silently usable everywhere.

If a native account provider fails during a request, Nullion also surfaces
active external connector providers to the agent. That lets the agent try an
enabled connector skill route before saying account access is unavailable,
without hardcoding any one gateway or service.

Curated skill packs can be enabled during setup or with:

```bash
NULLION_ENABLED_SKILL_PACKS=nullion/web-research,nullion/browser-automation,nullion/files-and-docs,nullion/pdf-documents,nullion/productivity-memory
```

Useful commands:

```text
/skill-packs
/skill-packs available
/skill-pack google/skills
```

See `docs/skill-packs.md` for the Google Skills integration model and authoring
guidance for community skill packs.

## Architecture overview

```
┌─────────────────────────────────────────┐
│              Chat Adapters              │
│  Telegram · Slack · Discord · Web UI   │
└────────────────┬────────────────────────┘
                 │  ChatAdapter interface
                 ▼
┌─────────────────────────────────────────┐
│            chat_backend.py             │
│     (platform-agnostic chat core)      │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│               Kernel                   │
│  runtime · tools · policy · audit      │
└─────────────────────────────────────────┘
```

Each chat adapter is a thin platform-specific shim. The kernel and chat backend never touch platform APIs directly. Adding a new platform means implementing the `ChatAdapter` interface — nothing in the kernel changes.

## Initial focus

Build a minimal kernel with:
- principals
- tasks
- events
- audit logging
- policy/capability checks
- user-facing main assistant

## Chat adapters

Nullion ships with Telegram, Slack, Discord, and Web UI adapters. Telegram remains the reference implementation; Slack and Discord use the same operator/runtime path with platform-specific connection code at the edge.

### Telegram adapter

#### Setup

Create a local env file:

```bash
cp .env.example .env
```

Fill in:

```bash
NULLION_TELEGRAM_BOT_TOKEN=<your-bot-token>
NULLION_TELEGRAM_OPERATOR_CHAT_ID=<your-telegram-user-or-chat-id>
NULLION_TELEGRAM_CHAT_ENABLED=false
```

`NULLION_*` keys are the config surface used by every Nullion service.

#### Run

```bash
nullion-telegram
# or
nullion-operator
```

Optional overrides:

```bash
nullion-telegram --checkpoint runtime.db --env-file .env
```

Runtime state is stored in SQLite by default at `~/.nullion/runtime.db`.
Legacy `runtime-store.json` checkpoints are still readable as an import fallback.
Chat-history fields are encrypted locally; on macOS, setup can store the chat
history data key in Keychain instead of `~/.nullion/chat_history.key`.

```bash
nullion-telegram --help   # inspect all CLI flags
```

Equivalent Python entrypoint:

```bash
python -m nullion.telegram_app
```

### Recovery control plane

Nullion also ships a small out-of-band recovery command:

```bash
nullion-recovery status
nullion-recovery services
nullion-recovery restart telegram
nullion-recovery restart all
nullion-recovery snapshot-config
nullion-recovery restore runtime 0
nullion-recovery restore config latest
```

`nullion-recovery` is intentionally separate from the main web app and chat
adapters. It can restart launchd-managed services, list runtime backups, snapshot
local config, and restore `.env`/credentials snapshots if a bad update or config
save breaks the main app.

Telegram recovery can use a dedicated recovery bot token, or it can fall back to
the normal `NULLION_TELEGRAM_BOT_TOKEN` plus
`NULLION_TELEGRAM_OPERATOR_CHAT_ID`. When it uses the normal bot token, it waits
until `ai.nullion.telegram` is down before polling so the two processes do not
consume the same Telegram updates.

#### Operator commands

- plain text — when `NULLION_TELEGRAM_CHAT_ENABLED=true`, routed to the Nullion Assistant
- `/help` — list available commands
- `/chat <message>` — send a message through the Nullion Assistant
- `/verbose [off|planner|full|status]` — choose activity trace and planner task-card visibility
- `/thinking [on|off|status]` — show or hide provider reasoning summaries separately from final replies
- `/approvals [status=pending|approved|denied|all]` — list approval requests
- `/approve <id>` / `/deny <id>` — approve or deny a request
- `/grants` — list permission grants
- `/revoke-grant <id>` — revoke a grant
- `/proposals` / `/accept-proposal <id>` / `/reject-proposal <id>` — manage Builder skill proposals
- `/skills` / `/skill <id>` / `/skill-history <id>` — browse saved skills
- `/ping` / `/version` / `/health` / `/uptime` / `/status` — liveness and status
- `/backups` / `/restore [latest|<generation>]` — checkpoint management
- `/system-context` / `/codebase` / `/tools` — runtime introspection
- `/plugins` / `/plugins available` / `/plugin <id>` — plugin catalog and setup details

Telegram mention form (`/help@<bot_username>`) is supported in group chats.

### Web UI adapter

```bash
nullion-web              # http://localhost:8742
```

Provides a browser-based chat interface (left panel) and live dashboard (right panel) with approvals, tasks, skills, and health. No Telegram account required.

### Slack adapter

Slack runs in Socket Mode:

```bash
NULLION_SLACK_ENABLED=true
NULLION_SLACK_BOT_TOKEN=xoxb-...
NULLION_SLACK_APP_TOKEN=xapp-...
nullion-slack
```

Authorize Slack users in the Users settings or `~/.nullion/users.json` with `messaging_channel=slack` and their Slack user ID.

### Discord adapter

```bash
NULLION_DISCORD_ENABLED=true
NULLION_DISCORD_BOT_TOKEN=<bot-token>
nullion-discord
```

Authorize Discord users in the Users settings or `~/.nullion/users.json` with `messaging_channel=discord` and their Discord user ID. The Discord bot needs the Message Content intent enabled.

### Multi-user connections

Single-user installs continue using the globally configured provider accounts. Once another user is enabled, member workspaces can declare their own provider connections in Settings -> Users.

The Connections panel lists only providers required by enabled or installed
auth-required skill packs. Instruction-only skills do not appear there because
they do not need account credentials.

For Gmail through Himalaya, install Himalaya on the same computer or server that runs Nullion, then create a Himalaya account profile per person. The installer can enable the email/calendar plugins; workspace connections only store the local profile name, not the Gmail password.

Then add a workspace connection with:

```text
Workspace: workspace_nathan
Provider: Gmail / Google Workspace
Profile: nathan
```

Member tool calls do not fall back to the admin Gmail account by default. If a
member has no workspace connection, Nullion reports that the provider is not
connected for that workspace. An admin may intentionally share one credential
across all workspaces for providers that allow it; the UI requires a
confirmation and records the connection as an admin-shared credential.

### Adapter status

| Platform | Status    | Notes                          |
|----------|-----------|--------------------------------|
| Telegram | Shipped   | Reference implementation       |
| Web UI   | Shipped   | FastAPI + WebSocket at `:8742` |
| Slack    | Shipped   | Socket Mode                    |
| Discord  | Shipped   | `discord.py` bot client        |
| SMS      | Exploring | Via Twilio plugin              |

## Operator access control and audit logging

- `NULLION_TELEGRAM_OPERATOR_CHAT_ID` restricts who can run operator commands on the Telegram adapter.
- Unauthorized attempts return `Unauthorized operator chat.` and emit warning logs.
- Every handled command is logged: `Handled Telegram operator command (chat_id=..., command=..., result=...)`.
- INFO-level logging must be enabled to see startup, clean-stop, and handled-command logs. Unauthorized attempts and failures are logged at warning/error levels.
