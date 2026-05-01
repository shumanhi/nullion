# End-to-End Testing Plan

This project already has good fast coverage around installer contracts, config
helpers, FastAPI endpoints, WebSocket behavior, approvals, crons, plugins, and
adapter workflows. The missing layer is a slow, isolated test lane that proves a
fresh install can boot the app and exercise the major user paths through the
real console scripts.

## Test Lanes

Use three lanes instead of one huge brittle test:

1. `pytest`
   Fast unit and workflow tests. These remain the default gate.
2. `NULLION_RUN_E2E=1 pytest tests/e2e`
   Local or CI smoke tests that create an isolated virtualenv, install Nullion,
   start `nullion-web`, and hit real HTTP/WebSocket surfaces.
3. `NULLION_RUN_LIVE_E2E=1 pytest tests/e2e`
   Optional live-provider tests that use real API keys, browser binaries, and
   external services. These should never be required for normal PRs.

## Installation Coverage

The current installer is intentionally interactive and service-manager aware,
so the safest first e2e install test is:

- create a temporary `HOME`
- create a temporary virtualenv
- run `pip install -e <repo>`
- assert all console scripts exist and their `--help` paths work
- launch `nullion-web` from the installed script
- verify `/api/health`, `/api/status`, `/api/config`, and HTTP chat slash
  commands

A full `bash install.sh` e2e should come next, but it needs test hooks first:

- `NULLION_INSTALLER_NONINTERACTIVE=true`
- `NULLION_INSTALLER_SOURCE_DIR=<repo>`
- `NULLION_INSTALLER_SKIP_SERVICES=true`
- `NULLION_INSTALLER_SKIP_OPEN=true`
- `NULLION_INSTALLER_SKIP_PLAYWRIGHT=true`
- `NULLION_INSTALLER_DEFAULTS=minimal`

With those hooks, CI can preseed `~/.nullion/.env`, run the actual installer,
and assert it writes the expected venv, `.env`, secure-storage key, logs
directory, and service files without registering real launchd/systemd services.

## Major Workflows To Cover

Start with these because they match the real product surface:

- First boot: health, version, status, config, session bootstrap.
- Chat: HTTP fallback, WebSocket chat, slash commands, empty-message errors,
  concurrent turns, and conversation reset.
- Approvals: create a pending approval through a fake risky tool, reject it,
  approve it, and verify status/history updates.
- Files and artifacts: upload a file, receive an artifact descriptor, fetch it
  back through `/api/artifacts/{id}`.
- Preferences and profile: save, reload, and verify persistence under the
  isolated data directory.
- Users and connections: save workspace members and a fake env-backed
  connection, then verify env updates and registry persistence.
- Crons/reminders: create, list, update, delete, and verify workspace metadata.
- Doctor and control plane: diagnose, chat-service restart conflict behavior,
  gateway event listing, and safe restart paths with restart functions patched
  or disabled.
- Plugins: workspace plugin against a temp allowed root, browser plugin in
  `auto` or fake backend mode, media command providers using tiny local command
  templates.
- Messaging adapters: use fake Telegram/Slack/Discord send functions and assert
  operator commands, delivery receipts, approval cards, and artifact delivery.

## Browser UI Coverage

After the API smoke lane is stable, add a Playwright lane that opens the live
web app and checks visible UI behavior:

- app shell renders without console errors
- chat input sends `/status` and receives a bot bubble
- Settings opens, edits a preference, saves, closes, and persists after reload
- Upload attaches a file and shows an artifact/download row
- Approvals render action buttons and stale clicks produce the expected copy
- Dashboard status updates after crons/reminders/tasks mutate state

Keep Playwright tests focused on user-visible behavior. API tests should own
the deeper state matrix.

## CI Shape

Recommended commands:

```bash
python -m pytest
NULLION_RUN_E2E=1 python -m pytest tests/e2e -m "not live"
NULLION_RUN_LIVE_E2E=1 python -m pytest tests/e2e -m live
```

The e2e lane should upload server logs from the temp home on failure:

- `~/.nullion/logs/nullion.log`
- `~/.nullion/logs/nullion-error.log`
- captured `nullion-web` stdout/stderr

