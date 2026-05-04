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
- Do not create or keep test files or test folders in this repo.
- Write tests in `/Users/himanc/Projects/nullion-test`, or the sibling checkout at `../nullion-test`.
- When running tests from `nullion-test`, set `NULLION_APP_REPO=/Users/himanc/Projects/nullion` so tests exercise this checkout.
- Website, marketing, and docs-site work belongs in `/Users/himanc/Projects/nullion-website`, or the sibling checkout at `../nullion-website`, not in this app repo.
- If a change needs app, test, and website updates, edit each repo in its own checkout and preserve unrelated dirty files.

## Running Instance Safety

- Do not touch the running Nullion instance unless the user explicitly asks for it.
- Treat `~/.nullion`, installed editable-package pointers, launchd services, tray/web/Telegram processes, live config, credentials, and runtime databases as running-instance state.
- Do not restart services, call restart endpoints, run `pip install -e` into the installed Nullion venv, edit `~/.nullion/.env`, mutate encrypted credentials, or repoint the installed package as part of ordinary debugging or code changes.
- Inspecting logs, process lists, package metadata, and config read-only endpoints is allowed when debugging, but do not mutate live state without explicit permission.
- If a fix needs the running instance to pick up code, report the exact command or checkout the user should run rather than applying it yourself.

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

## Git And CI Workflow

- Do not push directly to `main`. Work on an `agent/...` branch and open a pull request.
- Before making changes on an existing branch, fetch/prune remotes and verify the branch still exists upstream and is not already merged into `origin/main`. If the upstream branch is gone, merged, or replaced by a squash merge on `origin/main`, stop using that branch and start any follow-up work from fresh `origin/main`.
- If the shared checkout has unrelated dirty files, create a separate `git worktree` from `origin/main` and make your changes there.
- Keep app changes in `nullion` and tests in `nullion-test`; use separate branches/PRs when both repos need changes.
- After creating a PR, check the PR status checks with GitHub before reporting success.
- If the user already instructed you to merge or ship, wait for required checks to finish and merge only after they pass.
- If any check fails, inspect the failing job logs, report the failure, and fix it before merging.
- For `nullion-test`, the installer checks are `Linux installer Docker` and `Windows installer Docker` in the `Installer Docker` workflow.
- Do not merge a PR with failing, cancelled, or still-running checks unless the user explicitly overrides that specific failure.
