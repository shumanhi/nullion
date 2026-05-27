# Nullion Agent Instructions

## Routing Rules

- Do not infer user intent from hardcoded natural-language words, phrases, regexes, or synonym lists.
- Do not split one user message into multiple tasks based on conjunctions or sentence wording.
- Treat a user message as one request unless there is a structured product signal proving otherwise.
- Allowed signals: explicit UI action, slash/operator command, attachment metadata, file extension, URL/domain, tool result schema, task/frame state, approval state, artifact descriptor, or model-produced structured plan.
- If intent classification is needed, use a model-produced structured output with a schema, then validate it against runtime evidence.
- Prefer Deep Agents, LangGraph, and LangChain for stateful workflows, typed routing, retries, artifact verification, task state, and tool orchestration.
- Deep Agents may receive structured tasks only; do not dispatch them from keyword matching.
- LangGraph nodes should branch on typed state and verified tool/runtime facts, not free-form text phrases.
- LangChain/tool adapters should expose structured tool metadata and outputs so downstream code avoids parsing prose.
- Keep safety/security detectors separate from product routing. Security filters may inspect text, but must not decide task decomposition or artifact delivery.
- Add tests proving equivalent behavior works without English-specific trigger words.
- Activity/status summaries should show that a tool was used, including tools like `list_crons`, without exposing the tool's full output when that output contains internal task prose, paths, files, artifacts, credentials, connector payloads, or other non-deliverable state.
- Durable user preferences that affect safety or consent, such as requiring confirmation before sending emails, must be saved as memory and injected into future turns across supported chat surfaces. Capture them through structured memory extraction or explicit memory tooling, not product-routing keyword checks.
- Any user-facing prompt that asks the user to choose from multiple options or provide one of several possible inputs must present numbered options and must allow a numeric reply such as `1`, `2`, or `3`. Do not require exact text names or internal ids as the only path for a normal user.

## Anti-Use-Case Patching

- Never fix a reported bug by embedding the reporter's concrete prompt wording, nouns, account names, chat ids, repo names, branch names, product names, marketplace names, people names, country/language, screenshots, sample artifacts, URL path shapes, or vendor-specific semantics into Nullion core behavior.
- Treat every concrete report as an example of a product invariant. First state the invariant in implementation notes, then implement that invariant with typed runtime evidence such as tool schemas/results, URL/domain records as opaque references, attachment metadata, artifact descriptors, task/frame state, approval state, stored memory records, or model-produced structured plans.
- Core may preserve opaque structured evidence, like `{"type": "url", "domain": "...", "url": "..."}` or tool names/result status, but core must not interpret provider-specific URL paths, repo semantics, marketplace semantics, order semantics, or connector defaults. Put provider behavior in the connector, adapter, skill pack, or tool metadata that owns it.
- When multiple saved-history, memory, tool, artifact, or URL candidates exist, do not choose by old rank, English wording, or the user's previous example. Use current structured evidence and live tools when available; otherwise ask a brief clarification.
- Search, memory, and recent-context fixes must be language- and user-agnostic. Use corpus statistics, recency, typed evidence, and structured relationships instead of English trigger words, regex synonym lists, or one user's recurring topics.
- Tests for these fixes must use neutral synthetic fixtures and include at least one wording or language variant that does not depend on the original English example. Do not use the reporter's actual accounts, products, stores, repos, branches, or chat ids as the thing that proves core behavior.
- Before handoff for a routing, memory, search, tool-scope, or delivery fix, audit the touched app diff for reporter-specific and vendor-specific strings. Remove them from core behavior, or explicitly justify each remaining provider name as adapter/UI copy, documentation, or test fixture data rather than routing logic.

## Planner Cards And Activity Display

- Planner card visibility is controlled only by the settings-backed planner/task-card setting, such as `NULLION_TASK_PLANNER_FEED_MODE` and the matching UI setting, not by `/verbose`.
- `/verbose` is only `on`, `off`, or `status`; it controls whether activity details are shown. Do not reintroduce planner/full verbose modes or a public task-card command.
- When planner cards are enabled, every planned run should render a planner card on supported surfaces, including Web, Telegram, Slack, and Discord. The card should include `PLANNER`, the mission type, a compact `For: <request summary>` line from typed task-status state, the task count/title, and per-task rows.
- When activity feed is off, show only the planner card top section: header, request summary, task count/title, and task rows. Do not append the `ACTIVITY` section or tool/mini-agent details below it.
- When activity feed is on, show the same planner card top section plus an `ACTIVITY LIVE` section below it with compact, deduped tool/mini-agent activity.
- Task-status delivery must pass and branch on typed state such as `status_kind`, `group_id`, and, where applicable, `include_activity`; clients must not infer planner-card or activity-feed behavior from user prompt prose or stale local UI state.
- Terminal success, failure, timeout, or artifact completion must always deliver a final user-visible result message or verified artifact receipt. Never suppress terminal results just because a planner card or activity card was visible.
- For `TaskDecomposer` and planner-preview latency regressions, debug and fix the active provider/runtime bottleneck first. Do not implement model-specific routing or provider-specific workaround behavior as the primary fix.
- Any decomposer slowness investigation must capture runtime evidence before UI/display patches: `cron planner preview profile` timing logs, `TaskDecomposer` timeout/parse errors, prompt size, tool-count, and `list_crons -> run_cron` startup gap from `runtime.db`.
- Do not assume users have a second provider or a "fast lane" model. The default path must remain reliable when only one provider/model is configured.
- If fallback planner cards are needed, they are resilience-only and must not replace root-cause performance fixes.
- Add regression tests that cover timeout, clarification, and valid multi-task planner outcomes without relying on English trigger words.
- Latency is a release-critical product requirement. Do not fix routing, scheduler, connector, planner, or activity bugs by adding a model/classifier call to every ordinary request. First look for typed runtime signals, cached decisions with correct invalidation, existing task/frame state, tool result schemas, stored job/artifact descriptors, and other cheap structured routes. If a new model decision is unavoidable, gate it behind structured evidence and add tests proving unrelated plain turns stay on the fast path.
- Startup should proactively warm cheap, reusable runtime facts that chat turns commonly need, including tool registry shape, cron metadata, compact installed-skill indexes, system context, and platform chat context. Do not move this work into the hot path for every ordinary chat request unless the cache is missing or invalid.
- Tool scoping must follow this order: use the cached compact installed-skill/capability index for cheap awareness, make a structured scope decision or `request_tool_scope` call only when the visible tools are insufficient, then load exact skill docs or connector context only after that structured scope is selected. Never load full skill-pack text, connector inventories, or scheduler/job details on every simple chat turn.
- Do not hardcode product behavior, URL rewrites, endpoint defaults, workflow assumptions, retries, or output fixes for third-party skill packs, connector providers, external APIs, or vendor-specific routes inside Nullion core. Core may enforce generic safety, schema, permission, boundary, delivery, and tool-contract rules only. If a third-party skill pack or connector returns the wrong default behavior, fix the installed skill pack, connector metadata, adapter configuration, or skill documentation instead of encoding that provider's semantics in Nullion core.

## Repo Boundaries

- This repo, `nullion`, is the app/runtime repo.
- App code changes happen in `/Users/himanc/Projects/nullion` on the user-named branch.
- Do not create or keep test files or test folders in this repo.
- Write tests in `/Users/himanc/Projects/nullion-test`, or the sibling checkout at `../nullion-test`.
- When running tests from `nullion-test`, set `NULLION_APP_REPO=/Users/himanc/Projects/nullion` so tests exercise this checkout.
- Website, marketing, and docs-site work belongs in `/Users/himanc/Projects/nullion-website`, or the sibling checkout at `../nullion-website`, not in this app repo.
- If a change needs app, test, and website updates, edit each repo in its own checkout and preserve unrelated dirty files.

## Pinned Branch Workflow

- Before any code changes, check the local ops pinned branch file at `~/.nullion-ops/pinned-branches.json`.
- If the `app` pin exists, all `nullion` app work must happen on that pinned branch/ref unless the user explicitly asks to use a different branch.
- If the `test` pin exists, all paired `nullion-test` work must happen on that pinned branch/ref unless the user explicitly asks to use a different branch.
- If the current checkout is not on the pinned branch/ref, switch to the pinned branch or create/use a worktree for it before editing.
- Do not create a new app or test branch while a pin exists unless the user asks to create or change the pin.
- If no pin exists for a repo, follow the normal user-named branch and git workflow rules below.
- The local ops dashboard at `/Users/himanc/Projects/nullion-test/scripts/local_ops_dashboard.py` is the source of truth for creating, changing, and clearing pinned branches.

## Bug Tracker Workflow

- The local ops dashboard uses one paired work-branch pin to represent the matching `nullion/<name>` app branch, `nullion-test/<name>` test branch, and QA tracker workbook.
- When the local ops dashboard creates a paired work branch, it appends a shared `--hh-mm-am/pm` suffix to the app branch, test branch, and pinned QA tracker workbook filename.
- The tracker workbook lives in `/Users/himanc/Projects/agents-worksapce/nullion-ops` with the `bug_rtracker_` filename prefix.
- Before fixing a bug on a pinned app branch, open the pinned QA workbook, or that branch's `bug_rtracker_*.xlsx` workbook, and update or add the matching Bug Tracker row.
- When a bug fix is ready for QA, update the workbook's `Status`, `Implementation Notes`, `QA Status`, `QA Checks`, and `Last Updated` fields.
- Do not overwrite or regenerate an existing branch tracker workbook; preserve existing rows and QA notes.

## Prod, Stage, And Dev Safety

- Prod is the release lane. It uses port `8742`, `~/.nullion`, `~/.nullion/runtime.db`, `~/.nullion/.env`, and launchd labels such as `com.nullion.web`, `com.nullion.tray`, and `ai.nullion.telegram`.
- Prod must run the latest released install from the installer-managed source checkout at `~/.nullion/src`, not unreleased branch code, not latest `origin/main`, and not any `/Users/himanc/Projects/...` worktree. Do not repoint prod unless the user explicitly asks for that exact prod repair.
- Stage is the pushed-code lane. It uses port `8753`, `~/.nullion-stage`, `~/.nullion-stage/runtime.db`, `~/.nullion-stage/.env`, and launchd labels such as `com.nullion.stage.web`, `com.nullion.stage.tray`, and `ai.nullion.stage.telegram`.
- Stage is for testing code that has been pushed, such as `origin/main`, before release. Agents must not directly patch, repoint, restart, or mutate stage unless the user explicitly asks for a stage operation.
- Dev is the local-branch lane. It uses port `8752`, `~/.nullion-test`, `~/.nullion-test/runtime.db`, `~/.nullion-test/.env`, and launchd labels such as `com.nullion.test.web`, `com.nullion.test.tray`, and `ai.nullion.test.telegram`.
- Dev is the only lane intended for local branch testing from `/Users/himanc/Projects/nullion`.
- The local ops dashboard lives in `/Users/himanc/Projects/nullion-test/scripts/local_ops_dashboard.py` on port `2020`. It may expose user-clicked controls for dev and stage operations, including copying prod DB to stage. It must not silently mutate prod.
- Treat all prod and stage homes, launchd plists/services, tray/web/Telegram processes, editable-package pointers, config, credentials, runtime DBs, JSON mirrors, and browser profiles as protected running-instance state.
- Read-only inspection is allowed when debugging: logs, process lists, launchd status, package metadata, curl GETs, DB reads, and config reads. Do not write to prod or stage files, call mutating endpoints, restart services, or run `pip install -e` into their venvs without explicit user approval for that exact action.
- If the user explicitly asks to patch or restart prod for testing, restart all relevant prod launchd services and verify the new processes before telling the user to test:
  - `launchctl kickstart -k gui/$(id -u)/com.nullion.web`
  - `launchctl kickstart -k gui/$(id -u)/com.nullion.tray 2>/dev/null || true`
  - `launchctl kickstart -k gui/$(id -u)/ai.nullion.telegram`
  - Verify fresh PIDs with `launchctl print gui/$(id -u)/com.nullion.web` and `launchctl print gui/$(id -u)/ai.nullion.telegram`.
  - Verify the loaded code path with the prod venv, for example `/Users/himanc/.nullion/venv/bin/python -c "import nullion.telegram_app as t; print(t.__file__)"`.
  - Verify the web service responds on prod port `8742`, for example `curl -fsS http://127.0.0.1:8742/api/status`.
- Do not use `/Users/himanc/Test/nullion` for code changes. That folder is not the working app repo.

## Pull Requests

- Use `nullion/` as the branch prefix for PR work.
- Keep PR titles and descriptions brief.
- Do not include local absolute paths, usernames, or machine-specific commands in PR text.
- Do not mention `nullion-test`, paired test branches, test PRs, `Nullion-Test-Ref`, or any test-repo coordination details in Nullion PR titles, PR bodies, PR comments, issue comments, review comments, or other GitHub-visible Nullion repo text.
- Release PR branches must be squashed to a single commit on top of `origin/main` before pushing or opening the PR. Do not push a working-branch commit stack or expose intermediate local commits in GitHub PR history.

## Public Release Copy

- Release notes, update summaries, public comments, PR text, issue comments, review comments, app-facing info, and any user-visible announcement must stay customer-facing.
- Do not mention internal documents, internal process, tests, test repositories, QA trackers, CI, release checks, private verification, branch coordination, or other non-product work in public/user-facing release copy or comments.
- Convert internal work into user-facing outcomes. For example, describe "more reliable updates" instead of internal release checks, test coverage, or private process details.

## Verification Guardrails

- For live behavior reports from prod, stage, dev, Telegram, Web, Slack, Discord, or cron delivery, inspect runtime evidence before guessing or patching: relevant logs, `runtime.db` events/tool timings, process/import path, active lane, provider/connector status, and exact user-visible error text. Do not diagnose from screenshots or assumptions alone when logs/runtime state are available.
- For user-visible delivery bugs, verify with the live API or platform boundary after patching, not only helper tests. Reuse the same prompt shape and surface that failed when the user authorizes it.
- For Telegram-facing changes, do not count `/api/chat`, helper tests, or bot-token-only Bot API sends as full Telegram validation. A bot token can send messages as the bot but cannot impersonate the user's inbound Telegram account, so it does not prove the real inbound user-message path. Full Telegram validation requires a real user-sent Telegram message on the target lane while the agent watches the matching Telegram logs, `runtime.db` turn/tool/artifact records, and user-visible delivery outcome. If a fake `Update` or handler test is used, label it clearly as handler-only coverage and still request/perform live user-message verification before claiming Telegram is tested.
- For tool-scope, connector, media-generation, scheduler, weather, or artifact-delivery regressions, record the exact failed turn before guessing. The failure chain must include the selected surface, conversation id or request id when available, scoped tool list/tool count, tool results, artifacts, and timing.
- Artifact delivery regressions must be tested against prior fixed cases before handoff: generated cron reports, email/send receipts with attachments, platform captions, and internal sidecar/state-file suppression.
- The active pre-commit hook is `.githooks/pre-commit`, selected by `core.hooksPath=.githooks`.
- The app pre-commit hook blocks `test/`, `tests/`, `test_*.py`, and `*_test.py` from this repo.
- The app pre-commit hook runs the private suite in `../nullion-test/tests` with coverage unless `NULLION_SKIP_PRIVATE_TESTS=1` is set.
- Every bug fix must include the right test layers before it is marked ready: a focused regression test for the changed code path and, when the bug affected a user-visible workflow, an E2E test in `nullion-test` proving the real product boundary that failed.
- Integration tests may supplement the E2E, but they do not replace it for user-visible workflow bugs unless an E2E cannot be written.
- If a required E2E cannot be written, record the concrete reason in the QA tracker and final handoff instead of silently skipping it.
- While implementing, run focused tests for the touched path only. Do not run the full private suite, all tests, or broad E2E matrix until all code, test, and tracker updates for the current request are complete and targeted checks are green.
- After all work for the current request is complete, run the broadest relevant local verification once, or report the exact blocker before push, merge, or final handoff.
- Docker, installer, GUI, browser, and end-to-end checks should live in `nullion-test` and run inside containers or CI runners, not directly against the host desktop.
- GitHub Actions Windows and Linux installer checks belong in `nullion-test`.
- New E2E tests must exercise the real product boundary that failed. For UI bugs, start the app server and drive the browser with Playwright; for API/workflow bugs, call the real FastAPI route or runner against a real `PersistentRuntime`/SQLite store; for delivery bugs, verify the actual artifact/upload/receipt bytes or rendered UI state.
- Latency regressions require measured E2E coverage, not only helper assertions. When fixing a latency-sensitive chat, routing, connector, scheduler, planner, or activity path, add or update an E2E that runs multiple representative requests, records elapsed time, asserts an average/p95-style threshold, and simulates the previously slow dependency so the test fails if that dependency re-enters the hot path.
- Plain chat requests must remain on the fast path even when optional providers, connectors, skill packs, crons, or planner features are installed. Do not let the existence of a configured capability alone trigger extra model calls, connector inventory loads, network probes, broad database scans, or task decomposition.
- Tool-scope fixes must preserve low-latency simple chat. Do not make every plain turn load full skill-pack text, connector inventories, or a broad tool classifier. Prefer cached compact capability indexes, startup warmup, and exact-tool loading only after structured scope selection or safe local tool metadata makes it appropriate.
- When a user-visible tool-scope bug is fixed, add or update a live stage chat contract in `nullion-test` that sends real `/api/chat` requests against stage and checks both the visible reply/artifact result and backing runtime tool evidence. The live probe must include a simple-turn latency average plus the affected tool family and at least one adjacent tool family when practical.
- Do not mark a tool-scope or delivery fix ready based only on helper/unit tests. Before handoff, run the focused local tests and, when stage is available or the user asks for live validation, run the opt-in live stage probe with `NULLION_LIVE_STAGE_E2E=1` or the matching script directly.
- Do not add E2E tests that only mock the function under test or assert a helper in isolation. Unit tests may use helpers, but E2E coverage must prove the user-visible path changes state and renders or delivers the expected result.
- E2E assertions should include the visible user outcome and the backing runtime evidence when possible, such as a card disappearing plus the stored action status changing, or a dashboard hiding junk memory plus durable memory still rendering.
- Prefer deterministic local fakes only at external network/provider boundaries. Keep Nullion routing, persistence, status APIs, browser UI, task state, and artifact handling real inside the test.

## Git And CI Workflow

- Never commit directly on `main` without the user's explicit approval for that specific commit.
- Never commit in a prod/main worktree such as `/Users/himanc/Projects/nullion-worktrees/main` without the user's explicit approval for that specific commit.
- If you find uncommitted changes on `main`, stop and report them. Do not stage, amend, squash, reset, or commit them unless the user explicitly asks.
- Agent work should happen on the pinned branch when one exists, otherwise on a user-named feature/bugfix branch, not on `main`.
- Do not push directly to `main`. Work on a `nullion/...` branch and open a pull request.
- Treat every push as a paid CI-triggering action. Do not push, force-push, or rerun remote workflows unless the user explicitly asks for that exact paid action after seeing the local verification result.
- Before any push, run the strongest relevant local verification first, including the private suite from `nullion-test` when app changes can affect it. Report the exact command outcome and remaining risk before asking for push approval.
- Never use `--no-verify`, `NULLION_SKIP_PRIVATE_TESTS=1`, or `NULLION_SKIP_IMPORTANT_CHECKS=1` for a push unless the user explicitly approves bypassing that specific gate.
- If local verification fails because of hooks, environment, repo metadata, missing deps, or test harness issues, fix that local root cause and rerun locally. Do not discover those failures by burning CI runs.
- Do not push while a previous CI run for the same branch is still running unless the user explicitly says to cancel/restart it with a new push.
- Before making changes on an existing branch, fetch/prune remotes and verify the branch still exists upstream and is not already merged into `origin/main`. If the upstream branch is gone, merged, or replaced by a squash merge on `origin/main`, stop using that branch and start any follow-up work from fresh `origin/main`.
- If the shared checkout has unrelated dirty files, create a separate `git worktree` from `origin/main` and make your changes there.
- Keep app changes in `nullion` and tests in `nullion-test`; use separate branches/PRs when both repos need changes.
- After creating a PR, check the PR status checks with GitHub before reporting success.
- If the user already instructed you to merge or ship, wait for required checks to finish and merge only after they pass.
- If any check fails, inspect the failing job logs, report the failure, and fix it before merging.
- For `nullion-test`, the installer checks are `Linux installer Docker` and `Windows installer Docker` in the `Installer Docker` workflow.
- Do not merge a PR with failing, cancelled, or still-running checks unless the user explicitly overrides that specific failure.
