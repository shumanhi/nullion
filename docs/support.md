# Nullion Support Guide

Use this when Nullion is installed but something is confusing, slow, blocked, or
broken.

## Start here

Run these from Telegram or the web command path:

```text
/health
/status
/doctor
/approvals
/tools
/plugins
/verbose status
/thinking status
```

What they tell you:

- `/health` gives the shortest useful diagnosis.
- `/status` shows active missions, approvals, Doctor actions, grants, model
  configuration, and enabled tool categories.
- `/doctor` lists repair actions and health issues.
- `/approvals` shows work paused for your decision.
- `/tools` shows what is actually registered in this runtime.
- `/plugins` shows enabled plugins and setup hints.
- `/verbose` shows the current visibility mode: `off`, `planner`,
  or `full`. `/thinking` shows whether provider reasoning summaries are shown
  separately.

## Common problems

### Web UI will not open

Try:

```bash
nullion-web
nullion --dashboard
open http://localhost:8742
```

If you changed the port, check `NULLION_WEB_PORT`. If another service is using
the port, set a different value and restart.

### Model calls fail

Check Settings -> Model in the web UI, or verify:

```bash
NULLION_MODEL_PROVIDER=openai
NULLION_MODEL=gpt-5.5,gpt-5.4,gpt-5.4-mini
NULLION_REASONING_EFFORT=medium
NULLION_OPENAI_API_KEY=...
```

`NULLION_MODEL` accepts a comma-separated preference list. The runtime uses the
first non-empty entry for chat, while the settings UI can test each configured
model so bad fallback entries are visible before you depend on them.

OpenAI-compatible providers such as OpenRouter, Gemini, Groq, Mistral, DeepSeek,
xAI, Together, and Ollama are routed through the OpenAI-compatible client when
their provider and key/base URL are configured.

Thinking level is also available in the web UI under Settings -> Model -> Chat
model. Supported values are `low`, `medium`, and `high`; providers or models
without explicit reasoning controls ignore the setting.

### Telegram ignores messages

Check:

- `NULLION_TELEGRAM_BOT_TOKEN` is the token from BotFather.
- `NULLION_TELEGRAM_OPERATOR_CHAT_ID` is your numeric chat ID, not your username.
- You started `nullion-telegram`.
- You are messaging the correct bot.

If the chat ID was learned from the wrong account, update
`~/.nullion/.env` and restart the adapter.

### Slack or Discord does not respond

Check:

- `NULLION_SLACK_ENABLED=true` or `NULLION_DISCORD_ENABLED=true`.
- The required bot token variables are present.
- The adapter process is running: `nullion-slack` or `nullion-discord`.
- The sender is authorized in Settings -> Users or `~/.nullion/users.json`.
- Discord has Message Content Intent enabled.
- Slack Socket Mode has an app-level token with `connections:write`.

### A task is stuck

Run:

```text
/status active
/approvals
/doctor
```

Most stuck work is waiting for one of three things: an approval decision, a new
boundary grant, or a health action.

If the delegated plan itself is unclear, set Verbose to Planner or Full in
Settings, then repeat the request.

### A file or PDF was promised but not delivered

Nullion should not mark an attachment task complete until the requested
artifact is attached or exposed as a web download. Check:

- The request explicitly asked for a file, PDF, image, document, spreadsheet,
  or attachment.
- The final artifact extension matches the request. A `.txt` or `.html` file
  is not a valid PDF deliverable.
- In the web UI, the answer shows a download button for the artifact, not only
  a local filesystem path.
- In Telegram, Slack, or Discord, the platform sends a document/image
  attachment when one is expected.
- Verbose Full shows `Preparing artifacts` and `Writing response` after the
  file tool or artifact generation tool completes.

If the file exists locally but no download button appears, restart the web app
and retry once. If it still happens, include the requested format and the
visible artifact path in the bug report.

### File tools cannot see a project

Set one of:

```bash
NULLION_WORKSPACE_ROOT=/Users/you/Projects/my-project
NULLION_ALLOWED_ROOTS=/Users/you/Projects,/Users/you/Documents
```

Restart the runtime after changing allowed roots. Nullion blocks path traversal
outside allowed roots.

### Browser automation fails

Start with:

```bash
NULLION_ENABLED_PLUGINS=browser_plugin
NULLION_BROWSER_ENABLED=true
NULLION_BROWSER_BACKEND=auto
```

Use `auto` first. Switch to `playwright` for a clean headless browser, or `cdp`
when you intentionally want to attach to an existing Chrome/Brave session.

### Email or calendar is unavailable

Email and calendar are preview capabilities. They require both:

- `email_plugin` or `calendar_plugin` in `NULLION_ENABLED_PLUGINS`
- a provider binding such as `email_plugin=google_workspace_provider` or
  `email_plugin=custom_api_provider`
- a matching provider connection in Settings -> Users -> Connections for the
  workspace that made the request, unless an admin intentionally shared a
  supported credential across workspaces

Skill packs alone do not grant account access.

## Updates and rollback

Update safely:

```bash
nullion update
```

Use the repository-head channel when you need a commit that has not been
released yet:

```bash
nullion update --hash
```

Or from chat:

```text
/update
```

Nullion snapshots the install, runs health checks, and rolls back if checks
fail. Runtime state backups can be listed and restored with:

```text
/backups
/restore latest
/restore <generation>
```

On Windows, if an interrupted update leaves `~ullion` pip leftovers, missing
launchers, broken scheduled tasks, or a locked `venv\Scripts\nullion.exe`, run:

```powershell
nullion repair windows-install
```

That command repairs the installed source pointer and recreates Web, Tray, and
Telegram scheduled tasks without rewriting the locked console launcher.

## What to include in a bug report

Include:

- Operating system and install method.
- Output from `/version`.
- Output from `/health`.
- The adapter you used: Web UI, Telegram, Slack, Discord, or CLI.
- The exact action that failed.
- Any relevant logs with API keys, bot tokens, OAuth tokens, and private paths
  removed.
