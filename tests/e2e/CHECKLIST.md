# Nullion E2E Checklist

This checklist is the working map for the Docker and Doctor e2e suite. The
source of truth for machine-readable status is `nullion_e2e_manifest.py`.

## Runner Isolation

- [x] Docker runner keeps `HOME`, `NULLION_HOME`, and XDG paths inside tmpfs.
- [x] Docker runner mounts the app checkout read-only.
- [x] Docker runner publishes no host service ports.
- [x] GitHub Doctor workflow runs the e2e suite on Linux, macOS, and Windows.
- [x] Coverage report is emitted as `.e2e-artifacts/nullion-e2e-coverage.json`.
- [x] Installer smoke runs `nullion update --hash` against a broken local update target and proves automatic rollback on Linux, Windows, and macOS.
- [x] Windows installer smoke locks `venv\Scripts\nullion.exe` during update and fails on `WinError 32`, `.deleteme`, or process-cannot-access-file pip output.

## Mocked External Calls

- [x] OpenAI-compatible chat API call path is exercised against a fake HTTP API.
- [x] Codex OAuth refresh and Responses API call path are exercised against fakes.
- [x] Anthropic messages serialization is exercised without live Anthropic calls.
- [x] Model-test endpoint handles provider-specific response shapes (tool-calls, empty payloads, missing stop reason) through mock model clients.
- [x] `web_fetch` hits a mocked HTTP origin through the real fetch tool.
- [x] `connector_request` hits a mocked connector gateway with fake credentials.
- [x] OpenRouter web model-test has a fakeable base URL path contract.
- [x] Google Workspace email/calendar behavior has fake provider responses.
- [x] IMAP/SMTP behavior has fake server responses.
- [x] Local media provider behavior has local fake command fixtures.

## Real Workflow Contracts

- [x] Config/provider matrix and memory bucket caps are visible through `/api/config`.
- [x] Model-test endpoint calls fake provider APIs.
- [x] Web model provider saves and model switches are now validated for web and Telegram conversation behavior.
- [x] Model provider switches are now validated for Slack and Discord conversation behavior.
- [x] Media model add/delete/save/default cycles are covered in settings UI and `/api/config`.
- [x] Connections endpoint persists connector config and enables connector tools.
- [x] Profile, preferences, users, cron CRUD, and OAuth disconnect/reauth contracts use isolated state.
- [x] Upload, artifact, chat HTTP, chat websocket, approvals, profile, preferences, and Doctor diagnose paths have e2e coverage.
- [x] Installed updater rollback is covered through the real CLI path with snapshot and restored git commit assertions.
- [x] Windows updater coverage includes the locked `nullion.exe` launcher case that previously broke `nullion update`.
- [x] Seeded legacy SQLite runtime DB update compatibility is covered for updater migrations, memory encryption, runtime reload/checkpoint, and web boot.
- [x] Artifact generation workflows validate multi-format file delivery (`.txt`, `.pdf`, `.xlsx`, `.png`/screenshot-like).
- [x] Multi-step, multi-format websocket artifact delivery is covered in one-turn output fanout.
- [x] Multi-step fetch-to-file workflows (web content into `.txt`+`.pdf`, and embedded-image `.xlsx`) are covered.
- [x] Settings tab matrix saves and reloads values on General, Messaging, Profile, Preferences, Setup, Execution, Doctor, Security, and Builder tabs.
- [x] OAuth reauth SSE stream has fake process stream coverage for output, failure, disconnect, and spawn errors.
- [x] Settings tab actions cover users CRUD/toggles, crons CRUD/toggles, logs source switching, security policy add/remove, builder install, messaging restart, doctor run, and setup restart confirmation.
- [x] Doctor action execution path has browser-e2e coverage for action trigger and feedback.
- [x] Reminder delivery needs fake delivery loop coverage.
- [x] Cron delivery needs fake platform-delivery coverage.
- [x] Builder, memory, task frame, and mini-agent workflows have model/runtime fakes.
- [x] Web chat memory claims are covered end-to-end through Builder capture, runtime storage, and `/api/status` dashboard filtering, including non-English phrasing and runtime-ID noise rejection.

## Platform Surfaces

- [x] Web HTTP surface.
- [x] WebSocket surface.
- [x] Telegram adapter fake.
- [x] Slack adapter fake.
- [x] Discord adapter fake.
