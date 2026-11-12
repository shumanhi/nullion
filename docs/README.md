# Nullion Documentation

Nullion is a local-first AI operator console. It runs on your machine, connects
to the model provider you choose, and can use approved tools for web research,
local files, browser automation, media processing, scheduled work, and preview
account connectors. Current builds include workspace-scoped approvals and
scheduled jobs, platform-aware delivery routing, validated planner task cards,
separate verbose activity controls, LangChain model/tool adapters, LangGraph
turn routing and delivery workflows, Deep Agents mini-agent execution, model
fallback settings, default PDF/document skill packs, Builder-managed capability
dependencies, browser-backed web research fallback, and cleaner activity traces.

This docs folder is written for a mixed audience:

- If you are new to Nullion, start with this README, then use
  `docs/plugins.md` and `docs/skill-packs.md` for capability setup.
- If something is not working, use `docs/support.md`.
- If you are configuring capabilities, use `docs/plugins.md` and
  `docs/skill-packs.md`.
- For commands and setup, use [Commands](commands.md) and
  [Configuration](configuration.md).
- For time-sensitive monitoring, use [Live tracking](live-tracking.md).
- For release highlights, see [v0.10.0 release notes](releases/v0.10.0.md).
- For contributors, use [Architecture](architecture.md) and
  [Testing](e2e-testing.md).

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
- Builder-reviewed capability dependency proposals for local document,
  spreadsheet, PDF, image, presentation, and audio workflows
- LangChain adapters that expose Nullion tools as structured tools and wrap
  configured model clients as chat models
- Warm mini-agents and Deep Agents for bounded parallel work
- LangGraph-backed turn routing for independent parallel work versus dependent follow-ups
- Typed turn-scope policy that hides web, scheduler, connector, and artifact
  tools unless URL, attachment, file-extension, task-frame, or structured-plan
  evidence grants that scope
- LangGraph workflows for approval decisions, artifact handling, attachment
  formatting, and platform delivery
- File/PDF delivery contracts across web, Telegram, Slack, and Discord
- Validated DAG planning with planner task cards in chat surfaces
- User-facing observability controls for verbose activity details, compact tool
  outcomes, planner task cards, generated artifacts, and reasoning summaries
- Per-session stop/cancel handling through `/stop`
- Local runtime state, history, schedules, grants, skills, and audit records

Capability status:

- Available: search, web fetch, browser automation, workspace files, local
  media tools, reminders, crons, temporary live trackers, Telegram, Slack, Discord, Web UI.
- Preview: email search/read and provider-supported calendar reads and writes
  through explicit provider bindings. Calendar writes remain approval-bound.
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
/planner <message>
/stop
/plugins
/skill-packs
/backups
/update
nullion --dashboard
nullion --web
nullion --logs
nullion --errors
nullion --config
nullion --status
nullion --stop
nullion --restart
nullion --model gpt-5.5
nullion --provider openai
nullion update --hash
nullion update --ignore-checks
nullion repair windows-install
nullion tray install
nullion tray start
nullion tray stop
nullion tray status
nullion-web
nullion-webview
nullion-cli --one-shot "Summarize my project"
nullion-cli update --hash
nullion-cli skill-pack enable openclaw/local-skills
nullion-auth --reauth codex
nullion-recovery status
nullion-recovery backups
nullion-recovery serve --port 8020
nullion-recovery telegram
```

The same operator command handler is used by Telegram and the web command path.
Slack and Discord route authorized user messages through the same messaging
runtime.

## Documentation map

| Guide | Covers |
| --- | --- |
| [Commands](commands.md) | Shared chat-command catalog and terminal entry points |
| [Configuration](configuration.md) | Models, adapters, file roots, visibility, data and encryption |
| [Live tracking](live-tracking.md) | Temporary watches versus reminders/crons, reuse, limits, controls and API |
| [Plugins](plugins.md) | Capability registration, provider setup and account connections |
| [Skill packs](skill-packs.md) | Instruction packs, dependencies and authoring |
| [Support](support.md) | Diagnosis, recovery, updates and reporting |
| [Architecture](architecture.md) | Routing, follow-ups, delivery, persistence and source map |
| [Testing](e2e-testing.md) | Repository-local suites, Docker and local operations |
| [Security](../SECURITY.md) | Vulnerability reporting and local-data protection |
| [Browser tasks](../src/nullion/plugins/browser_plugin/README.md) | Low-level sessions and the optional Browser Use worker |

The [online guide](https://www.nullion.ai/docs/) is maintained in the separate
`nullion-website` repository. This app repository owns the guides listed above;
it no longer contains a `website/` directory or the retired operations/plan
documents previously linked here. Documentation follows this source checkout;
an older installed release can have fewer tools or controls.

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

## Planner, activity, and background work

Planner cards and verbose activity are separate. The current runtime keeps
task planner cards enabled; the former planner-feed environment toggles do not
change this build's behavior. With verbose activity off, cards show their
request summary and task rows without tool details. With activity on, they
include compact, deduped tool and mini-agent activity. Final results and
requested artifacts must still be delivered.

Use `/planner <message>` when you want a request to run through the explicit
step-by-step planner path. It is not a visibility toggle; it starts a planned
task using the message that follows the command.

`/verbose` accepts only `on`, `off`, or `status`. It never toggles planner-card
visibility. Terminal success, failure, timeout, and artifact completion still
need a final user-visible result or verified artifact receipt even when planner
or activity cards were already shown.

![Nullion web console showing planner cards, activity, generated artifacts, approvals, schedules, and runtime controls](https://www.nullion.ai/assets/nullion-web-console-real.png)

Independent turns can run without blocking a
new message. Follow-ups wait only when the typed turn dispatcher links them to
an active task frame or prior turn. The dispatcher uses structured runtime
evidence such as task state, attachments, URLs, file extensions, UI actions,
tool schemas, and model-produced structured plans, not prompt-word matching.

If a run should stop, `/stop` cancels active work in the current session and
marks the active task frame cancelled when possible.

## Capability dependencies

Some local artifact workflows need optional Python packages. Settings ->
Learning and planning -> Capability dependencies shows the supported catalog
and creates Builder proposal cards before installing packages. Built-in
catalog entries include pandas, openpyxl, Pillow, pypdf, python-docx,
python-pptx, and soundfile. Custom PyPI or GitHub package requests also create
a reviewable Builder proposal instead of installing silently.

## Temporary watches and recurring work

Use a live tracker for frequent bounded observations, a cron for continuing
recurring work, and a reminder for a known notification time. Live trackers
reuse prepared source state and stop on a match or deadline. The agent asks
numbered questions when it cannot choose confidently. The Web service must
remain running for live checks and alerts. See [Live tracking](live-tracking.md).

Follow-ups can amend a running request. When replying to an earlier message,
keep the reply anchor attached so the runtime can identify the intended task.
A cancellation of a recently created schedule uses its verified receipt; if
several targets could match, Nullion should ask which one to cancel.
