# Chat Latency And Context Revert Notes

Created after the `ASK ME A QUESTION` -> `mount averst` regression.

Purpose: document the exact latency/context changes made by Codex so they can be reverted surgically without wiping unrelated dirty work in this branch.

## Do Not Whole-File Revert Blindly

The branch already had unrelated edits in `src/nullion/web_app.py`, `src/nullion/chat_operator.py`, and several `nullion-test` files. If reverting only this latency/context work, do not use whole-file `git restore` unless you have first confirmed those unrelated edits can also be discarded.

## App Repo Changes

Repo: `/Users/himanc/Projects/nullion`

### `src/nullion/web_app.py`

1. Web config classifier opt-in
   - Function: `_handle_web_config_request(...)`
   - Change: structured `config_action` still works, but free-text config classification is skipped unless `NULLION_WEB_CONFIG_TEXT_CLASSIFIER_ENABLED` is enabled.
   - Reason: avoid an extra model call for ordinary chat.
   - Revert: remove the feature-flag guard:
     ```python
     if not _feature_enabled("NULLION_WEB_CONFIG_TEXT_CLASSIFIER_ENABLED", default=False):
         return None
     ```

2. Web ambiguity classifier gating
   - Function: `_web_ambiguity_classifier(...)`
   - Change: the model relationship classifier now runs only when there is structured follow-up evidence or literal structured turn evidence.
   - Reason: avoid classifying casual text like `not much, how about you?` as a task continuation.
   - Revert: restore the previous behavior that allowed the classifier to run from prior assistant prose.

3. Open task-frame evidence helper
   - Function added: `_web_has_open_task_frame(runtime, conversation_id)`
   - Change: checks active task frame status before allowing follow-up classification.
   - Reason: open task state is structured evidence; completed casual chat is not.
   - Revert: remove this helper and remove its call from `_run_turn_sync(...)`.

4. Structured follow-up evidence in Web turn preflight
   - Function: `_run_turn_sync(...)`
   - Change: computes:
     ```python
     structured_followup_evidence = (
         bool(normalized_attachments)
         or _web_has_open_task_frame(runtime, conv_id)
         or _web_dispatch_requires_existing_turn_context(turn_dispatch_decision)
     )
     ```
     and passes it to `_web_ambiguity_classifier(...)`.
   - Reason: keep classifier use tied to structured runtime facts.
   - Revert: pass only `bool(normalized_attachments)` again, or remove the argument if reverting the full gating change.

5. Conversation history decoupled from task continuation
   - Function: `_run_turn_sync(...)`
   - Change: always appends `_web_chat_history_from_store(runtime, conv_id)` to `conversation_history`, even when the new turn is an independent task branch.
   - Reason: fixes the regression where `mount averst` did not know it was answering the previous assistant question.
   - Important: this is the fix for the bug shown in the screenshot.
   - Revert only if you also revert the classifier gating; otherwise normal chat continuity will break again.

6. Web stable context prefix cache
   - Functions added: `_web_stable_context_history_prefix(...)`, `_clear_web_stable_context_cache(...)`, and cache signature helpers near the top of `web_app.py`.
   - Function changed: `_run_turn_sync(...)`
   - Change: caches only the stable rendered Web prompt prefix: tool/capability inventory, runtime config prompt, workspace connection references, enabled skill-pack prompt, installed dependency context, and web research guidance.
   - Not cached: current user text, attachments, chat history, memory, task-frame state, recent tool context, preferences/profile, or artifact context.
   - Invalidation: cache keys include tool registry, model client, settings/env/file signatures, principal id, runtime id, and enabled skill-pack signatures. `_hot_reload_live_config(...)` also clears the cache after settings/config/skill-pack/dependency reloads; connection and user registry saves clear it too.
   - Important: runtime.db is intentionally not part of the file signature because ordinary chat/runtime writes update it every turn. Web config saves clear the cache explicitly when encrypted credentials or model settings change.
   - Reason: avoid rebuilding stable prompt sections every Web turn without changing routing or chat continuity.
   - Revert: replace the `_web_stable_context_history_prefix(...)` call in `_run_turn_sync(...)` with the prior inline system-context rendering block, then remove the cache helpers/constants and invalidation calls.

### `src/nullion/chat_operator.py`

1. Open task-frame status set
   - Constant added: `_OPEN_TASK_FRAME_STATUSES`
   - Reason: same structured-evidence gate for messaging surfaces.
   - Revert: remove if reverting messaging ambiguity gating.

2. Messaging open task-frame helper
   - Function added: `_chat_has_open_task_frame(...)`
   - Reason: same as Web.
   - Revert: remove helper and call site.

3. Messaging ambiguity classifier gating
   - Function: `_chat_ambiguity_classifier(...)`
   - Change: skips model relationship classification unless structured evidence exists.
   - Reason: prevent casual chat from being misclassified as task continuation.
   - Revert: restore prior behavior that allowed prior assistant prose to trigger classifier calls.

4. Messaging preflight structured evidence
   - Function: `_render_chat_turn(...)`
   - Change: computes `conversation_id` once and passes structured evidence based on attachments, ambiguity fallback reason, or open task frame.
   - Reason: same structured-evidence policy as Web.
   - Revert: remove the structured evidence calculation and pass the previous classifier arguments.

5. Conversation context decoupled from task continuation
   - Function: `_should_include_conversation_context(...)`
   - Change: returns `bool(thread)` instead of using `should_include_prior_turn_messages(...)`.
   - Reason: keep normal chat memory while task branch stays independent.
   - Important: this protects Telegram/Slack/Discord-style casual replies too.
   - Revert only if also reverting classifier gating.

## Test Repo Changes

Repo: `/Users/himanc/Projects/nullion-test`

### `tests/test_web_ui_regressions.py`

1. Updated config classifier test
   - Test: `test_web_config_request_uses_structured_action_or_model_plan`
   - Change: asserts free-text config classifier is disabled by default and requires `NULLION_WEB_CONFIG_TEXT_CLASSIFIER_ENABLED=1`.

2. Updated independent-turn context test
   - Test: `test_web_completed_turn_without_structured_signal_keeps_chat_context_without_task_continuation`
   - Change: asserts a turn can have `parent_turn_id is None` while still receiving prior user/assistant chat history.

3. Added screenshot-regression shape
   - Test: `test_web_short_reply_to_assistant_question_keeps_conversational_context`
   - Scenario: `ASK ME A QUESTION` followed by `mount averst`.
   - Assertion: second turn sees the previous assistant question in `conversation_history`, but still does not become a task continuation.

### `tests/test_web_app_helper_coverage.py`

1. Added Web stable context cache coverage
   - Test: `test_web_stable_context_history_prefix_is_cached`
   - Change: verifies stable prompt sections render once, are reused from cache, rebuild when the tool registry signature changes, and rebuild after explicit invalidation.

2. Added runtime.db invalidation guard
   - Test: `test_web_stable_context_file_signature_excludes_runtime_db`
   - Change: verifies ordinary runtime DB writes cannot accidentally make the stable-prefix cache miss every turn.

### `tests/test_language_neutral_routing.py`

1. Updated chat ambiguity classifier test
   - Test: `test_chat_ambiguity_classifier_skips_completed_turn_without_structured_evidence`
   - Change: includes prior assistant message in the test context and still asserts no classifier model call.

### `tests/test_high_coverage_operator_telegram_helpers.py`

1. Added messaging context assertion
   - Test: `test_chat_operator_task_frame_and_activity_helpers`
   - Change: asserts `_should_include_conversation_context(...)` is true even for independent turns when a chat thread exists.

### `tests/e2e/test_language_neutral_routing_contracts.py`

1. Updated structured separate expectations
   - Tests:
     - `test_completed_messaging_turn_structured_separate_stays_independent`
     - `test_web_completed_turn_structured_separate_stays_independent_e2e`
   - Change: structured separate turns must remain independent task branches, but still receive ordinary chat transcript.

2. Existing/added full-context coverage
   - Test: `test_completed_messaging_turn_follow_up_keeps_full_context_across_a_third_message`
   - Purpose: protects multi-turn chat context across messaging path.

## Docs Added

Repo: `/Users/himanc/Projects/nullion`

1. `docs/chat-latency-todo.md`
   - Purpose: future work list for phase timing, prompt/context caching, streaming, and LangGraph preflight.
   - Revert: delete this file if removing all latency documentation.

2. `docs/chat-latency-revert-notes.md`
   - Purpose: this document.

## QA Tracker

External tracker updated:

`/Users/himanc/Projects/agents-worksapce/nullion-ops/bug_rtracker_nullion-paired-2026-05-08--03-11-pm.xlsx`

Row:

`APP-2026-05-09-001`

Changes:

- Added the original latency issue.
- Added the regression note for `ASK ME A QUESTION` -> `mount averst`.
- Added verification commands and status.

This workbook is outside the app repo and is not reverted by git.

## Verification Already Run

From `/Users/himanc/Projects/nullion`:

```bash
python3 -m py_compile src/nullion/web_app.py src/nullion/chat_operator.py
```

From `/Users/himanc/Projects/nullion-test`:

```bash
NULLION_APP_REPO=/Users/himanc/Projects/nullion /Users/himanc/Projects/nullion-test/.venv/bin/pytest \
  tests/test_web_ui_regressions.py::test_web_short_reply_to_assistant_question_keeps_conversational_context \
  tests/test_web_ui_regressions.py::test_web_completed_turn_without_structured_signal_keeps_chat_context_without_task_continuation \
  tests/test_web_ui_regressions.py::test_web_completed_turn_follow_up_uses_structured_recent_context \
  tests/test_language_neutral_routing.py::test_chat_ambiguity_classifier_skips_completed_turn_without_structured_evidence \
  tests/test_high_coverage_operator_telegram_helpers.py::test_chat_operator_task_frame_and_activity_helpers \
  -q
```

Result: `5 passed`.

```bash
NULLION_APP_REPO=/Users/himanc/Projects/nullion /Users/himanc/Projects/nullion-test/.venv/bin/pytest tests/test_language_neutral_routing.py -q
```

Result: `14 passed`.

```bash
NULLION_APP_REPO=/Users/himanc/Projects/nullion ./scripts/e2e-docker.sh \
  tests/e2e/test_language_neutral_routing_contracts.py::test_completed_messaging_turn_structured_separate_stays_independent \
  tests/e2e/test_language_neutral_routing_contracts.py::test_web_completed_turn_structured_separate_stays_independent_e2e \
  tests/e2e/test_language_neutral_routing_contracts.py::test_web_completed_turn_follow_up_uses_structured_recent_context_e2e
```

Result: `3 passed`.

## Suggested Revert Strategy

If reverting everything from this latency/context work:

1. Remove `docs/chat-latency-todo.md`.
2. Remove `docs/chat-latency-revert-notes.md`.
3. In app code, revert only the hunks listed above in:
   - `src/nullion/web_app.py`
   - `src/nullion/chat_operator.py`
4. In test repo, revert only the tests listed above.
5. Do not revert unrelated dirty changes in those files without checking their owning bug tracker rows first.

If keeping the latency classifier gating, keep the conversation-history decoupling too. Removing only the decoupling recreates the normal-chat regression.
