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

## Repo Boundaries

- This repo, `nullion`, is the app/runtime repo.
- Do not create or keep test files or test folders in this repo.
- Write tests in `/Users/himanc/Projects/nullion-test`, or the sibling checkout at `../nullion-test`.
- When running tests from `nullion-test`, set `NULLION_APP_REPO=/Users/himanc/Projects/nullion` so tests exercise this checkout.
- Website, marketing, and docs-site work belongs in `/Users/himanc/Projects/nullion-website`, or the sibling checkout at `../nullion-website`, not in this app repo.
- If a change needs app, test, and website updates, edit each repo in its own checkout and preserve unrelated dirty files.

## Verification Guardrails

- The active pre-commit hook is `.githooks/pre-commit`, selected by `core.hooksPath=.githooks`.
- The app pre-commit hook blocks `test/`, `tests/`, `test_*.py`, and `*_test.py` from this repo.
- The app pre-commit hook runs the private suite in `../nullion-test/tests` with coverage unless `NULLION_SKIP_PRIVATE_TESTS=1` is set.
- Docker, installer, GUI, browser, and end-to-end checks should live in `nullion-test` and run inside containers or CI runners, not directly against the host desktop.
- GitHub Actions Windows and Linux installer checks belong in `nullion-test`.
