# Nullion Documentation

Nullion is a local-first AI operator console. It runs on your machine, connects
to the model provider you choose, and can use approved tools for web research,
local files, browser automation, media processing, scheduled work, and preview
account connectors. Current builds include workspace-scoped approvals and
scheduled jobs, platform-aware delivery routing, validated planner task cards,
thinking-summary controls, LangChain model/tool adapters, LangGraph turn routing
and delivery workflows, Deep Agents mini-agent execution, model fallback
settings, default PDF/document skill packs, and cleaner activity traces.

This docs folder is written for a mixed audience:

- If you are new to Nullion, start with this README, then use
  `docs/plugins.md` and `docs/skill-packs.md` for capability setup.
- If something is not working, use `docs/support.md`.
- If you are configuring capabilities, use `docs/plugins.md` and
  `docs/skill-packs.md`.
- If you are operating Telegram in production, use the runbooks in
  `docs/operations/`.
- If you are contributing to product direction, the dated files in
  `docs/plans/` and `docs/progress/` record design history.

## What ships today

Core interfaces:

- Web UI on localhost, usually `http://localhost:8742`
- Native webview and tray companion through `nullion-tray` / `nullion-webview`
- Telegram operator through `nullion-telegram`
- Slack adapter through `nullion-slack`
- Discord adapter through `nullion-discord`
- Out-of-band recovery through `nullion-recovery`
- CLI helpers through `nullion` and `nullion-cli`

Core systems:

- Sentinel approval, grants, and boundary policy
- Workspace-scoped grants, crons, provider connections, and delivery targets
- Doctor health actions and safe recovery
- Recovery control plane for service restart, config snapshots, runtime restore,
  and Telegram takeover when the normal adapter is down
- Builder skill proposals, learned skills, and memory compaction
- LangChain adapters that expose Nullion tools as structured tools and wrap
  configured model clients as chat models
- Warm mini-agents and Deep Agents for bounded parallel work
- LangGraph-backed turn routing for independent parallel work versus dependent follow-ups
- LangGraph workflows for approval decisions, artifact handling, attachment
  formatting, and platform delivery
- File/PDF delivery contracts across web, Telegram, Slack, and Discord
- Validated DAG planning with optional planner task cards in chat surfaces
- User-facing observability controls for Verbose modes, compact tool outcomes,
  planner task cards, generated artifacts, and reasoning summaries
- Local runtime state, history, schedules, grants, skills, and audit records

Capability status:

- Available: search, web fetch, browser automation, workspace files, local
  media tools, reminders, crons, Telegram, Slack, Discord, Web UI.
- Preview: email search/read and calendar listing through explicit provider
  bindings.
- Planned: account-level messaging plugins. Telegram, Slack, and Discord are
  chat adapters today, not messaging account tools.

## Install

macOS or Linux:

```bash
curl -fsSL "https://raw.githubusercontent.com/shumanhi/nullion/main/install.sh?$(date +%s)" | bash
```

Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/shumanhi/nullion/main/install.ps1 | iex
```

Nullion requires Python 3.11 through 3.13. The installer checks for Python and tries
to install it automatically when it is missing: `winget` on Windows, Homebrew on
macOS, and `apt`, `dnf`, `pacman`, or `zypper` on Linux.

If automatic Python install is blocked, install Python first and rerun the
Nullion installer:

```powershell
# Windows PowerShell
winget install --id Python.Python.3.12 --source winget -e
py -3.12 --version
```

```bash
# macOS
brew install python@3.12
python3.12 --version
```

```bash
# Debian/Ubuntu
sudo apt-get update
sudo apt-get install -y python3.12 python3.12-venv python3.12-dev
python3.12 --version
```

Avoid Python 3.14 for now on Windows; some tray/webview dependencies do not
publish compatible wheels yet.

Already cloned the repo:

```bash
bash install.sh
```

## Everyday commands

```text
/help
/status
/health
/doctor
/approvals
/tools
/verbose
/thinking
/plugins
/skill-packs
/backups
/update
nullion-recovery status
```

The same operator command handler is used by Telegram and the web command path.
Slack and Discord route authorized user messages through the same messaging
runtime.

## Documentation map

- `plugins.md` explains plugins, providers, account connections, and tool
  registration.
- `skill-packs.md` explains reference instruction packs and why they do not
  grant real account access.
- `support.md` gives first-response troubleshooting steps.
- `runtime-persistence.md` covers local runtime checkpoint behavior.
- The online docs at `website/docs/index.html` include the v0.2 delivery,
  routing, and fresh-install notes shown on https://nullion.ai/docs/.
- `operations/recovery-control-plane.md` covers break-glass recovery,
  config snapshots, service restarts, and Telegram takeover mode.
- `operations/telegram-operator-runbook.md` covers Telegram operations.
- `operations/deploy-telegram-operator.md` covers Telegram deployment.
- `philosophy/kernel-plugins-skills-policy.md` explains the kernel/plugin/skill
  separation.

## Safety model in one minute

Nullion separates what a tool can do from where it is allowed to do it.
Approving a tool capability does not automatically approve every domain, file
root, account, or destructive action. Sentinel checks both the tool grant and
the boundary policy before execution.

Skill packs are instructions, not access. A Google skill pack can teach a better
Google Cloud workflow, but Gmail, Calendar, browser, search, and filesystem
access still require the matching plugin/provider setup and approval.
Custom skill packs follow the same rule: instruction-only packs can be used by
all enabled workspaces, while packs that require API keys, OAuth, tokens, or
account credentials must be connected under Settings -> Users -> Connections.
Admins can keep credentials per workspace or deliberately share a supported
admin credential across workspaces.

## Delivery contract

Nullion treats message delivery and file delivery as different completion
contracts. A normal question is complete when the reply is sent. A file request
is complete only when the requested artifact exists and is attached or exposed
as a download. If the user asks for a PDF, a `.txt` or `.html` staging file does
not satisfy the task even if the assistant text says it did.

The same idea is used across web, Telegram, Slack, and Discord:

- deliver files only when a file is requested or produced as a required artifact
- prefer requested formats such as `.pdf`, `.docx`, `.xlsx`, `.csv`, or images
- strip raw local paths from messaging captions when the platform attaches the file
- leave the task active or report failure when a required attachment is missing
