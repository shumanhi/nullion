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
- `/verbose` shows whether activity details are `on` or `off`. Planner cards
  remain enabled for planned work in the current runtime. The old planner-feed
  environment toggles are not active visibility controls in this build.
  `/thinking` shows whether provider reasoning summaries are shown separately.

## Common problems

### A health-alert button says it is resolved

Doctor **Mark resolved** and `/doctor complete` record your decision to close
the alert. Their acknowledgements explicitly identify the Doctor alert and do
not certify that a repair ran. **Dismiss** closes an alert without marking its
underlying issue fixed; **Mark in progress** changes only the alert status.
Sentinel review/resolution notices use the same distinction.

A restart button reports a repair request, followed by a health check or test
message to establish recovery. The legacy Web restart action leaves its alert
in progress. Closed-alert buttons cannot request another restart. Unsupported
commands and manual plugin instructions leave the alert unchanged. The model
connection menu offers **How to stop a request**, which explains `/stop`; it
does not claim to pause chat or disable schedules.

### An attachment error replaces source findings

The current request's validated output requirements take precedence over an
earlier tool-scope format guess. A calendar or other text answer must not acquire
an image requirement merely because an earlier step attempted to create one.
When a requested file fails, grounded findings can still be delivered with a
clear file limitation. The request remains incomplete; this does not certify
missing writes, bypass approval or deliver an unverified file.

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

If the delegated plan itself is unclear, turn on Verbose in Settings, then
repeat the request. Verbose now means activity details on or off; planner-card
cards remain enabled independently of Verbose in the current runtime.

If you want to force an explicit step-by-step plan for one request, use:

```text
/planner <message>
```

That starts a planned task for the message after the command. It does not turn
planner cards on or off.

If a run is still active and you want to stop it, send:

```text
/stop
```

This cancels active work in the current session and marks the current task
frame cancelled when one exists.

### Planner and activity display

- The current runtime keeps task planner cards enabled. Old guides describing
  a Planner-card off switch or `/verbose planner` and `/verbose full` are stale.
- `/verbose` accepts only `on`, `off`, or `status`. Activity off keeps cards
  compact; activity on adds tool/mini-agent details.
- `/planner <message>` starts one planned request; it does not hide/show cards.
- Pending, queued or blocked tasks use `☐`; active tasks use `◐`, completed
  tasks `☑`, failed tasks `✕`, and cancelled tasks `⊘`.
- Terminal replies and requested artifacts must arrive even if a status card
  was already displayed.

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
- Verbose activity shows compact artifact-preparation and response-writing
  activity after the file tool or artifact generation tool completes.

If the file exists locally but no download button appears, inspect the delivery
status before retrying. Repeating the whole request can create another artifact.
Include the requested format, artifact name, and redacted delivery evidence in
a report; keep private paths and content out of public issues.

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
NULLION_BROWSER_PREFERRED=brave
NULLION_BROWSER_CDP_URL=http://127.0.0.1:9222
```

Use `auto` first. It attaches to a reachable local CDP browser, otherwise it
launches a visible browser and falls back to headless Chromium only when no
local browser is available. In Settings, choose Browser mode separately from
Visible browser app so Brave, Chrome, and Edge are explicit choices.

Use `cdp` only when you intentionally want to attach to an existing browser
already running with remote debugging enabled. The default CDP endpoint is
`http://127.0.0.1:9222`; this avoids `localhost` resolving to `::1` on macOS.

Use `playwright` for a clean headless Chromium session.

If Open agent browser says it cannot connect to an existing browser on
`http://127.0.0.1:9222`, either switch Browser mode to Auto visible browser or
start Brave, Chrome, or Edge with remote debugging on port 9222.

### Web research feels slower than search

Nullion now prefers API-backed search only when the configured provider has
usable credentials. Without a Brave, Google Custom Search, or Perplexity key,
public web research falls back to browser navigation, page extraction instead of registering a direct fetch/search tool that may fail.

Check:

```text
/plugins
/tools
```

For faster source discovery, configure one of:

```bash
NULLION_PROVIDER_BINDINGS=search_plugin=brave_search_provider
NULLION_BRAVE_SEARCH_API_KEY=...
```

```bash
NULLION_PROVIDER_BINDINGS=search_plugin=google_custom_search_provider
NULLION_GOOGLE_SEARCH_API_KEY=...
NULLION_GOOGLE_SEARCH_CX=...
```

```bash
NULLION_PROVIDER_BINDINGS=search_plugin=perplexity_search_provider
NULLION_PERPLEXITY_API_KEY=...
```

### Email or calendar is unavailable

Email and calendar are preview capabilities. They require all of:

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

## Live trackers and scheduled work

### I asked for a short watch, but it became a recurring job

Use an explicit event, condition and end time, such as “watch this source every
15 seconds for two hours, then stop.” Nullion should use a live tracker and ask
numbered questions when the lifetime or schedule is unclear. Check **Settings →
Crons → Live trackers** for the actual state. No dedicated `/track` command is
required. See [Live tracking](live-tracking.md).

### A live tracker stopped without a match

Check whether it expired, was stopped, or paused because the source could not
be read. No-match observations are successful checks; an unreadable or missing
field is unknown. Verify the website login, page, permissions and model access
(if semantic evaluation is used) before resuming. Resume cannot extend an
expired deadline. The Web service must be running even for Telegram alerts.

### Checks are late or queued

Live checks and scheduled assistant jobs have separate parallel limits under
Crons. A source can take longer than the requested interval; raising capacity
does not speed up an individual slow source or bypass its rate limits. Inspect
the last-check time and count before assuming the job has stopped. A browser
restart can invalidate a prepared tab even though its tracker record survives.

### Doctor reported repeated empty checks

For recurring match-only jobs, Doctor defaults to one notice after 12
consecutive successful no-match runs, or a separate notice for repeated failed
runs. It does not claim these are the same state. Configure monitoring and the
streak threshold per cron. No daily tracker-summary schedule is required.

### A follow-up seems to start unrelated work

Reply to the relevant result or confirmation, name the target if multiple jobs
exist, and inspect `/status active`. Follow-ups received during a run should
amend the linked request before its final answer is published. Report the
original message, reply target and subsequent message with timestamps, surface,
and redacted runtime evidence. Do not assume a Web pass proves Telegram's
reply-to path; both adapters need their own acceptance checks.

### Local TCP connection exhaustion

`Cannot assign requested address`, `EADDRNOTAVAIL`, or macOS `Errno 49` across
both localhost and external services can indicate exhausted local TCP source
ports. Check socket states and owning processes before changing credentials.
On macOS, `netstat -anv -p tcp` shows connections and process ids;
`sysctl net.inet.ip.portrange.first net.inet.ip.portrange.last` shows the automatic
port range. Count unique local ports as well as connections: two socket records
do not necessarily consume two different ports.

The tray uses a persistent local connection, with bounded idle connections and
backoff on health failures. A long custom polling interval or a custom server
idle timeout shorter than the polling gap will reconnect. Provider streams that
end early or fail also close their sockets safely; pooling cannot force reuse
of an incomplete response or fix an operating-system retention problem.

Save a timestamped socket snapshot before restarting services. If `TIME_WAIT`
entries persist much longer than the configured TCP lifetime, collect repeated
snapshots and OS uptime to distinguish ongoing churn from abnormal retention.
Clearing pressure with a reboot does not establish its kernel cause or prove
recurrence is fixed. Do not disable schedules, rotate
working credentials, or change OS TCP settings as an automatic response.

### An infographic or report fails with an incomplete tool call

An incomplete provider response or malformed tool-argument JSON is a protocol
error, rather than a failed renderer or a user planning error. Nullion returns a
structured correction to the existing workflow before running the call or asking
the action reviewer to assess it. Completed source findings remain available.
A locally rendered image does not require a separate AI image-generation service.
When investigating, inspect model completion status/reason and the failed tool's
validation category. Argument length, digest and decoder offset are recorded;
raw provider argument bodies are not retained in the diagnostics.

If tracker creation loops between prepared argument aliases and physical browser
handles, inspect `conversation.action_review` concern scopes and the native
prepared-binding evidence. Exact arguments already validated by the adapter must
remain unchanged; physical handles describe output state, not replacement input.
An `advisory` review record preserves a mechanical suggestion without vetoing
that native proof. It does not bypass admission or consent. A genuine preflight
question stays terminal for the current turn and is offered as numbered choices.

If a reply includes directions to edit or omit its own draft, inspect the turn's
`conversation.completion_review` event and its public terminal reply. Review
diagnostics belong in runtime events and recovery context. The public boundary
uses certified findings and typed outcome notices, including for cron delivery
and reviewed file captions. Preserve useful partial facts while investigating;
do not remove the approval, source-binding or artifact-validation checks.

Public limitation rows must cite current source or operation receipts. Failed
operations can explain a limitation, but cannot prove a fact, successful action,
or delivered file. Source network exceptions carry typed categories; raw error
payloads and private reviewer prose remain internal. Tracker expiry delivery
uses its durable chat receipt even when the best-effort runtime-event checkpoint
is temporarily blocked by SQLite contention.

### Database handles or source failures misreported as network outages

Short-lived SQLite connections close after each transaction, including failed
setup and commit paths. Repeated scheduler or approval lookups should not grow
open database handles until garbage collection runs. For a configured database
path containing `#`, `?`, `%` or non-ASCII characters, read-only lookups must open
the literal path; those characters are not URI fragments or query parameters.
These changes do not alter schedule frequency, ownership, consent or retry limits.

An HTTP rejection is a response from the server. Inspect the structured
`status_code` and `reason=http_error` before diagnosing connectivity. A wrapped
socket timeout is recorded as `handler_timeout`; disk or permission failures are
separate source failures. An HTTP response alone does not establish that an
account is disconnected or that reconnecting it is the correct action.

If conversation analysis returns “Conversation analysis could not be completed,”
check the corresponding exception type in the application log and the chat
history storage location. A failed history-store open must return a controlled
error; it must not start an unrelated workflow or expose private exception text.

### A useful answer ends with an unrelated browser or network failure

Inspect the current turn's failed receipts, successful alternatives and
`conversation.completion_review` records. `blocking_failure_bindings` records
the reviewer's proposed failure-to-requirement links; the runtime validates
receipt status, ownership, recovery and eligibility before selecting an
operational notice. Tool-name overlap alone does not establish that relationship.
An earlier detail extractor can
fail while a later text reader succeeds. Remaining comparison uncertainty or
missing post-action readback must not be presented as a whole-browser outage.
Verified findings remain available and the request stays incomplete where
appropriate. Actual access, authorization and delivery failures remain distinct.

The older fulfillment fallback and execution status also consume request-bound
`execution_review` evidence. Check whether its final-text/evidence binding still
matches before treating a reconnect notice or partial status as current. A
same-named successful tool alone cannot establish that another subject recovered.
Certified findings should remain visible alongside a current reconnect notice;
verified completion should not be downgraded solely by a historical attempt.

A file-generation timeout does not establish missing credentials. Provider setup
guidance requires an explicit typed configuration failure. Keep raw exception
text, local paths, tokens and suggested install commands out of public failure
copy; inspect the private receipt/log for diagnostics. Missing files and required
actions remain incomplete even when a content review says the answer is ready.

For repeatedly partial scheduled work, inspect `prepared_report_content_review`,
`partial_report_retained`, `completion_continuation_started` and
`recovered_report_content_review` preflight records. They distinguish retained
files from completed research and record selected recovery tools, remaining
producer allowance and unmet execution state. A continuation should target known
unread records or cursor gaps, apply action-review corrections, and honor the
saved task's permitted data alternatives and current execution clock. Historical
task context does not establish current obligations. Do not rewrite a user's
saved task or its date rule without authorization.

When a scheduled run exhausts its execution budget with known work remaining,
Doctor offers numbered choices for more budget or denial. Approving continues
the saved run in the background, including its evidence, prepared files and
successful action receipts. It does not restart the lookup or change the recurring
schedule. A newer run or a changed task, workspace, or destination supersedes the
older continuation. A report-writing or review timeout by itself is not evidence
that more research budget is needed. Check `scheduled_budget_approval_created` or
`scheduled_budget_checkpoint_failed` to distinguish a persisted choice from a
checkpoint failure. No usable checkpoint means no continuation button is offered.

If a partial browser answer contains location lists, page controls or unrelated
metadata, inspect the source-answer selection's `answer_status` and each point's
`request_relation`, plus the preserved composed answer. Background context is
not a qualifying result. A reviewer outage should preserve authored wording and
layout, rather than introduce a generic findings header and raw excerpts.

An alert-only completed negative check should finish silently with verified
completion, rather than send a failure or restart a search to compose a message.
Silence with failed/unknown checks is still incomplete. For action review pointer
errors, check `review_protocol_errors`: one correction may inspect the retained
root or an existing field within the same deadline. Repeated invalid requests
must still withhold the action; correcting JSON never grants account permission.

For a prepared report with an unresponsive chat summary, inspect `artifact_summary_revision_started`, final-repair diagnostics and the following completion review. Evidence-complete content and incomplete presentation are separate states; a rejected summary must not restart research or invent additional attachments. `execution_satisfied` is runtime-grounded coverage, not publication permission. If summary composition remains unavailable, preserve the selected report with a summary-specific notice. Internal recovery instructions remain in diagnostics, never in public captions.

Terminal attachment checks distinguish the selected file from research completion. A partial report remains attachable only when the runtime's report receipt matches the original request and selected file bytes, and its prepared answer still matches the current tool receipts. New source reads, unfinished source tools, a missing companion format or missing embedded report images must not turn that bound, valid attachment into a missing file. Native files of every format share this receipt; HTML is one producer, not a separate delivery policy. These exceptions do not permit early completion, an email send, changed bytes, unselected files or a successful research claim. The report writer also rejects an unchanged draft when the owning review explicitly marks a contradiction with current evidence. Missing coverage alone is not a contradiction and must not erase useful partial findings. A provider's self-declared success cannot clear a contradiction; its existing correction attempt must change the rejected report or remain a reported composition failure. Review rows carry the optional typed `draft_contradicted` flag for an actual conflict with cited evidence; `draft_covered=false` alone means incomplete coverage. A contradiction cannot simultaneously be marked covered. This distinction keeps helpful partial findings available without restoring an answer that the evidence contradicts.

Carryover inspection previews share a bounded allowance across all retained windows rather than filling the packet with the first sources. The writer carries at most 9,000 characters and the completion review at most half the fresh-inspection allowance. Exact offsets, source sizes and incomplete flags remain visible; full authorized evidence stays inspectable in the same request.


If an availability answer stops after one source, inspect the discovery ledger and `conversation.completion_review.search_coverage`. Check the plan against the original request, relevant pending alternatives, date/location/format constraint evidence, and source observation receipts. A successful navigation or one empty catalog is insufficient for broader absence. Distinct query/fragment URLs remain distinct; only explicit adapter canonical metadata can bind equivalent documents. Inspect `search_coverage_errors` and `search_coverage_incomplete` for the exact failed contract. Blocked alternatives should produce useful scoped findings with specific limits.

For a scheduled continuation marked successful, verify a matching final review receipt and `cron_source_verification_satisfied`, rather than treating producer success or completed calendar reads as proof of a finished report. Calendar discrepancies and current-account status still require semantic reconciliation; this receipt check alone does not prove those judgments correct.

Scheduled chat reports should lead with actionable findings and keep requested sender/subject/date details in a compact supporting line or clause. Completed and informational findings should be grouped briefly unless a detailed audit was requested. Original execution and recovery share this guidance. It adds no model call, word-limit gate or automatic rewriting/truncation of delivered facts; attachments retain their requested detail. A later authorization must be reconciled with an earlier notice without being mistaken for proof of settlement.

For an overexplained report, inspect the typed content, not only its layout label: `report_detail_level=compact` does not make prose in required fields optional. Keep amounts, deadlines, action exceptions and material uncertainty in item summaries or required details. Put report-wide verification explanations and methodology in root `extra_details`; the shared renderer includes those only in detailed mode. Use one common calendar/action result and reserve the footer for an essential final action or exception. Preserve the brief status, such as an unconfirmed payment posting, without repeating an explanation of what each email can prove.

Also inspect the saved task's reporting instructions. A legacy request to include every sender, subject, received date and calendar disposition can legitimately produce an audit-style message despite a compact renderer. When the user changes that preference, update the saved output instructions while retaining execution, permissions, search coverage, scheduling and conditional-action rules. The concise triage fixture in `tests/fixtures/concise_email_triage_task.txt` separates these concerns. A scripted provider test proves transport and configuration propagation; it does not prove a real model will write briefly. Validate writing changes with a bounded real-provider replay of already-collected facts, without re-running account tools, and record that separately from live workflow acceptance.

Audit adjacent saved jobs for stale assumptions as well as runtime code. Historical dates and balances embedded in a task must not establish current unpaid obligations. For a stated local wall-clock schedule, store the intended IANA timezone and local cron hour, and check winter and summer next-fire times. A one-time departure saved as an annual cron needs explicit correction when its date has passed. Apply authorized repairs through the existing job API, preserve unrelated fields and jobs, and retain disabled job history. A writing-only replay does not close full workflow acceptance: an authorized production-worker run must separately establish actual source/action work and delivery, without starting another scheduler or bypassing execution leases. Native client rendering still requires direct client evidence.

When a platform accepts a file but its response times out, the file may arrive later. Nullion stops automatic replay and reports delivery as unconfirmed instead of saying the file was definitely not delivered. Check the destination before requesting another upload. In a multi-file batch, confirmed files, the uncertain file and unattempted files are recorded separately. Scheduled and interactive Telegram uploads share the configured media read/write timeouts. This does not change tracking schedules or rerun the underlying research.

After scheduled report recovery changes the source evidence, selected file bytes or final reply, completion must be reviewed against that updated state. An unavailable helper preserves the usable answer and file, with availability kept in diagnostics. It cannot establish that source checks failed or turn an otherwise delivered report into a failed-task alert. Actual source failures and producer-declared partial work remain visible.

An attached report and its chat summary have different content scope within the same shared presentation contract. Keep detailed sections, source inventories and methods in the file; chat should highlight useful findings, changes and material unavailable results unless the user explicitly requests the full report in chat too. A structured report envelope followed by explicit `MEDIA:` descriptors must still pass through the common renderer, preserving those descriptors and the selected file bytes. Unknown trailing prose is retained, not silently discarded. An empty media descriptor is invalid and must not crash parsing.

The shared `report_delivery_context.delivery_role` makes that scope explicit from existing artifact contracts: `artifact_summary` accompanies a required or selected file, while `standalone_report` carries the report in chat. Scheduled producers, ordinary outcome review, delegated completion and grouped summaries consume the same content policy. A routine artifact summary normally aims for 50–100 words, with essential exceptions and explicit chat-detail requests allowed to expand it; this is writing guidance, never a truncation or publication gate. The role does not establish that a file was created, verified or delivered.

Completion review inspection, protocol correction and transient timeout recovery share one elapsed allowance. Inspect `budget_seconds`, `timeout_seconds`, `deadline_exhausted` and `model_attempts` on the review event. A new attempt receives only the remaining time; inspection must not reset the deadline. This budget belongs to the optional review and does not impose a new tool-call or research limit. A review outage remains an internal diagnostic and preserves usable output without certifying unfinished source work.

For repeated whole reports, inspect the producer draft and the helper's supported findings separately. A rewritten report is a replacement candidate, not extra facts to concatenate with an existing answer. `draft_relation` distinguishes `replacement`, `additional` and `already_covered`; absent metadata defaults to replacement, and a whole structured report is never appended as an addition. A usable primary report retains its facts; only explicitly classified narrow additions and material limitations may supplement it. Grounded complete corrections still use the existing replacement proof. Empty or invalid primary drafts may still fall back to certified partial findings. This handoff is shared by interactive and scheduled delivery, without semantic text matching or another model call.

Calendar creation preflight receives adapter-owned identities, summaries and times from retained calendar reads, including events beyond the generic sampled preview and nearby dates. This context establishes only observed records, not exhaustive absence or permission to write. Connector calendar listings expose continuation tokens and exact next-page requests; follow those receipts before an absence claim. A selected-match preview identifies when more matches exist than the preview shows. Inspection and duplicate checks must compare the underlying obligation rather than assuming different reminder dates imply different obligations. Report any already-created duplicate separately; a code repair does not delete an account record.

### Outcome helper versus execution failure

Check `outcome_review_role`, `outcome_review_status` and the actual producer, source and delivery receipts separately. A helper timeout, invalid response or editorial disagreement is not an execution failure and should not appear as a review warning in the user's answer. `conversation.completion_review` retains the diagnostic details. A grounded correction can replace a weak composition; otherwise the primary useful answer remains available.

A helper can propose scoped improvements using retained work within the producer's existing progress-based budget. A grouped partial grade alone cannot restart execution: an `outcome_assistance_plan` must name owned task IDs and permitted tools and pass the executor's principal/receipt checks. Approval, source selection, actual missing actions, invalid files and upload acknowledgements remain independent. For delegated timeouts, inspect the producer status and retained output before attributing failure to the editor.


Image generation after browser research must distinguish factual page observations from source-image assets. A numeric/text browser result does not impose a source-photograph requirement. The provenance guard still applies to failed source-image collection and structured image-asset candidates; it must not reject a generated chart merely because a browser read happened earlier in the same request.

When a named account lookup asks for the same source again, inspect both the original and compacted `request_tool_scope` results. Source resolution needs the adapter IDs, read-tool names, permission metadata, inventory kind and recovery instructions. Compaction must preserve these fields without treating a supported-service catalogue as a list of connected accounts or granting account access. The model still selects the exact read scope; vague requests and requests to choose before reading still require a numbered choice.

Compact tool awareness and full tool inventories share the same named-source read policy. An unspecified hypothetical account is not a competing connected source. Progress callbacks likewise do not opt a user into activity details: final replies and background status cards on Web, Telegram, Slack and Discord use the selected chat's Verbose setting. Planner headers and final results remain visible independently of activity details. Modern session-setting lookups use their index rather than copying the conversation history.
