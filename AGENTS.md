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

## Repo Boundaries

- This repo, `nullion`, is the app/runtime repo.
- App code changes happen in `/Users/himanc/Projects/nullion` on the user-named branch.
- Do not create or keep test files or test folders in this repo.
- Write tests in `/Users/himanc/Projects/nullion-test`, or the sibling checkout at `../nullion-test`.
- When running tests from `nullion-test`, set `NULLION_APP_REPO=/Users/himanc/Projects/nullion` so tests exercise this checkout.
- Website, marketing, and docs-site work belongs in `/Users/himanc/Projects/nullion-website`, or the sibling checkout at `../nullion-website`, not in this app repo.
- If a change needs app, test, and website updates, edit each repo in its own checkout and preserve unrelated dirty files.

## Prod, Stage, And Dev Safety

- Prod is the release lane. It uses port `8742`, `~/.nullion`, `~/.nullion/runtime.db`, `~/.nullion/.env`, and launchd labels such as `com.nullion.web`, `com.nullion.tray`, and `ai.nullion.telegram`.
- Prod must run the latest released install from the installer-managed source checkout at `~/.nullion/src`, not unreleased branch code, not latest `origin/main`, and not any `/Users/himanc/Projects/...` worktree. Do not repoint prod unless the user explicitly asks for that exact prod repair.
- Stage is the pushed-code lane. It uses port `8753`, `~/.nullion-stage`, `~/.nullion-stage/runtime.db`, `~/.nullion-stage/.env`, and launchd labels such as `com.nullion.stage.web`, `com.nullion.stage.tray`, and `ai.nullion.stage.telegram`.
- Stage is for testing code that has been pushed, such as `origin/main`, before release. Agents must not directly patch, repoint, restart, or mutate stage unless the user explicitly asks for a stage operation.
- Dev is the local-branch lane. It uses port `8752`, `~/.nullion-test`, `~/.nullion-test/runtime.db`, `~/.nullion-test/.env`, and launchd labels such as `com.nullion.test.web`, `com.nullion.test.tray`, and `ai.nullion.test.telegram`.
- Dev is the only lane intended for local branch testing from `/Users/himanc/Projects/nullion`.
- The local ops dashboard lives in `/Users/himanc/Projects/nullion-test/scripts/local_ops_dashboard.py` on port `8760`. It may expose user-clicked controls for dev and stage operations, including copying prod DB to stage. It must not silently mutate prod.
- Treat all prod and stage homes, launchd plists/services, tray/web/Telegram processes, editable-package pointers, config, credentials, runtime DBs, JSON mirrors, and browser profiles as protected running-instance state.
- Read-only inspection is allowed when debugging: logs, process lists, launchd status, package metadata, curl GETs, DB reads, and config reads. Do not write to prod or stage files, call mutating endpoints, restart services, or run `pip install -e` into their venvs without explicit user approval for that exact action.
- Do not use `/Users/himanc/Test/nullion` for code changes. That folder is not the working app repo.

## Pull Requests

- Use `nullion/` as the branch prefix for PR work.
- Keep PR titles and descriptions brief.
- Do not include local absolute paths, usernames, or machine-specific commands in PR text.

## Verification Guardrails

- The active pre-commit hook is `.githooks/pre-commit`, selected by `core.hooksPath=.githooks`.
- The app pre-commit hook blocks `test/`, `tests/`, `test_*.py`, and `*_test.py` from this repo.
- The app pre-commit hook runs the private suite in `../nullion-test/tests` with coverage unless `NULLION_SKIP_PRIVATE_TESTS=1` is set.
- Docker, installer, GUI, browser, and end-to-end checks should live in `nullion-test` and run inside containers or CI runners, not directly against the host desktop.
- GitHub Actions Windows and Linux installer checks belong in `nullion-test`.
- New E2E tests must exercise the real product boundary that failed. For UI bugs, start the app server and drive the browser with Playwright; for API/workflow bugs, call the real FastAPI route or runner against a real `PersistentRuntime`/SQLite store; for delivery bugs, verify the actual artifact/upload/receipt bytes or rendered UI state.
- Do not add E2E tests that only mock the function under test or assert a helper in isolation. Unit tests may use helpers, but E2E coverage must prove the user-visible path changes state and renders or delivers the expected result.
- E2E assertions should include the visible user outcome and the backing runtime evidence when possible, such as a card disappearing plus the stored action status changing, or a dashboard hiding junk memory plus durable memory still rendering.
- Prefer deterministic local fakes only at external network/provider boundaries. Keep Nullion routing, persistence, status APIs, browser UI, task state, and artifact handling real inside the test.

## Git And CI Workflow

- Never commit directly on `main` without the user's explicit approval for that specific commit.
- Never commit in a prod/main worktree such as `/Users/himanc/Projects/nullion-worktrees/main` without the user's explicit approval for that specific commit.
- If you find uncommitted changes on `main`, stop and report them. Do not stage, amend, squash, reset, or commit them unless the user explicitly asks.
- Agent work should happen on a user-named feature/bugfix branch, not on `main`.
- Do not push directly to `main`. Work on an `agent/...` branch and open a pull request.
- Before making changes on an existing branch, fetch/prune remotes and verify the branch still exists upstream and is not already merged into `origin/main`. If the upstream branch is gone, merged, or replaced by a squash merge on `origin/main`, stop using that branch and start any follow-up work from fresh `origin/main`.
- If the shared checkout has unrelated dirty files, create a separate `git worktree` from `origin/main` and make your changes there.
- Keep app changes in `nullion` and tests in `nullion-test`; use separate branches/PRs when both repos need changes.
- After creating a PR, check the PR status checks with GitHub before reporting success.
- If the user already instructed you to merge or ship, wait for required checks to finish and merge only after they pass.
- If any check fails, inspect the failing job logs, report the failure, and fix it before merging.
- For `nullion-test`, the installer checks are `Linux installer Docker` and `Windows installer Docker` in the `Installer Docker` workflow.
- Do not merge a PR with failing, cancelled, or still-running checks unless the user explicitly overrides that specific failure.
