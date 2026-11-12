# Testing

Tests live alongside the implementation in `tests/`, with real product workflow
coverage in `tests/e2e/`. A fresh clone contains the complete released suite.
No second checkout, private-repository key, or live-provider credentials are
required for ordinary pull requests, including contributions from forks.

Shared fallback/status regressions are covered by
`test_shared_failure_outcomes.py` and
`tests/e2e/test_shared_failure_outcome_delivery.py`. These exercise receipt
invalidation, missing-action/file enforcement, current versus historical
connector blockers, preserved findings, Web reply/history rendering, scheduled
delivery/status and native image-provider timeout delivery through the chat API.
Only external model/source/provider responses are scripted; routing, review,
persistence, permissions, fulfillment and presentation remain real. These local
checks do not claim live provider judgment or live stage acceptance.

Scripted providers identify auxiliary scope requests from the structured packet
through `tests.workflow_provider_protocol.is_tool_scope_request`, rather than
an output-token limit. Changing a configured token allowance must not turn a
scope reply into an execution reply or invalidate fast-path call-count checks.

Tracker deadline coverage includes positive finite windows, explicit future
timestamps with offsets, mandatory end-time validation, in-place amendments,
duplicate requests, paused-state preservation, and stale in-flight checks. The
Web boundary tests use real tracker tools and SQLite. The opt-in live Web case
`test_live_stage_web_any_finite_window_and_same_tracker_amendments` creates a
14-day tracker, applies an exact deadline, then requests three hours in Spanish;
it checks one saved identity and no source re-preparation during amendments.

## Local commands

```bash
python -m pip install -e '.[test]'
./scripts/test-docker.sh
./scripts/e2e-docker.sh
```

Unit tests require at least 70% overall coverage. Both Docker commands mount the
checkout read-only, copy it into the container, and use temporary homes and
databases. E2E tests refuse to run on the host. Screenshots and workflow receipts
are exported to the ignored `.e2e-artifacts/` directory.

Pass pytest arguments to either runner for focused work:

```bash
./scripts/test-docker.sh tests/test_messaging_adapters_workflow.py
./scripts/e2e-docker.sh tests/e2e/test_web_surface_contracts.py
```

Focused unit runs still enforce overall coverage; use `python -m pytest` directly
with a temporary HOME for quick unit-only feedback. Browser, installer and E2E
checks must run in containers or CI runners. Live-stage probes are opt-in and
require separately authorized services and credentials.

Scheduled-workflow regressions include large hydrated-record batches followed
by late duplicate checks, conditional writes retained through recovery and normal
approval, source-selection consent retained in outcome review, and paginated
inventories with version invalidation. These exercise the graph, SQLite and real
runner/API boundaries; only external model/network responses are deterministic.
Monitor coverage also checks native task-specific delivery history in both the
requirements and outcome-review calls, including first runs and prior receipts
in two languages. Persisted-history checks cover unrelated chat traffic, fresh
uncheckpointed receipts, and unavailable databases without inventing an empty
history.
Scheduled profile cases use real persisted workspace memory, assert that it
reaches the producer and both review roles and report recovery, and exclude a
foreign workspace's profile.
Requested local-path checks follow exact tool-backed executable paths through
the real Web route, stored turn, Telegram formatting, and common Slack/Discord
transport preparation. They cover English and Japanese, typed local scope,
failed or unrelated evidence, later scope revocation, and request isolation.
Retained-evidence inspection tests cover two user languages, an exact full-body
window, and mailbox-scale execution with 96 long source bodies, 30 discovery
calls and four calendar checks. The scale cases assert one delivered report,
exact retained-window handoff to review, no repeated source IO, bounded model
context volume and local orchestration latency. Work-limit cases stop inside
large batches, preserve usage across resume/chat retry, prevent a scheduled
producer restart and deliver the concrete failure once. Adjacent cases cover
fresh reads, write invalidation, failures and the unchanged plain-chat fast path.
Additional inspection tests cover an exact full-body
window, one source read across two reviews, and rejection of foreign invocation
ids, missing pointers and invalid windows. Protocol-recovery cases correct an
oversized window, foreign reference or missing pointer within three model calls
while executing the source once, in two languages. Cron API tests preserve UTC, Tokyo and
New York schedule timezones and reject invalid timezone updates atomically.

For authorized saved-task diagnosis, `test_live_stage_web_cron_recovery.py` reads
the explicitly supplied `NULLION_LIVE_CRON_REPLAY_FILE`, creates disabled stage
clones with the original schedule, and starts each through the Web composer.
It checks terminal fulfillment, source verification and durable delivery evidence.
Its 1,200-second observation window is not a product latency guarantee; elapsed
time is retained in each result. A still-running task is preserved for diagnosis
rather than deleted to make fixture cleanup appear successful. The foreground
Web catalogue retains its separate latency assertions.

The default E2E image is built from `docker/e2e.Dockerfile`. To reuse an already
built image, set `NULLION_E2E_IMAGE` and `NULLION_E2E_SKIP_BUILD=1`; this only skips
the image build and still installs and runs the current checkout.

## CI

The PR workflow checks out the requested commit once. Eight disjoint unit shards
run with temporary homes, then combine coverage at the same 70% threshold.
A separate Docker job runs the complete E2E directory. Linux, Windows and macOS
installer jobs use the installer harnesses from this same commit. The macOS
installer uses the supported `macos-15` runner; retired runner labels must be
updated along with the workflow contract checks.
Every workflow has read-only repository permissions and ordinary fork PRs do not
receive secrets. Runtime/provider failures are simulated at external boundaries;
Nullion routes, state stores, rendered UI, artifacts and delivery remain real.

```bash
./scripts/installer-docker-linux.sh
# Windows CI runner:
# .\scripts\installer-docker-windows.ps1
# macOS CI runner:
# ./scripts/installer-macos.sh
```

Do not run installer or managed-service operations against an existing personal
installation. CI installer checks create disposable homes and work directories.
Do not include private logs, runtime databases, credentials, or personal output
in fixtures. Use fictional identities and example paths.

## Local operations dashboard

The dashboard, test runners, and app all come from this repository. A single
work-branch pin selects code and tests together; there is no second repository,
private access key, or paired test branch. E2E jobs execute the selected branch's
own `scripts/e2e-docker.sh` and `tests/`, rather than mixing it with another checkout.

Run `python3 scripts/local_ops_dashboard.py` to serve the dashboard on loopback
port 2020. On macOS, `python3 scripts/local_ops_dashboard.py --install-launchagent`
installs its login service from the current checkout, backs up the previous
LaunchAgent, and restores it if loading fails. This changes only the dashboard
service. Keep its source checkout available; branch cleanup protects it.

Instance configuration, databases, and logs remain in their runtime homes;
these are data directories, not separate Git repositories. Installing the
dashboard does not install, restart, or change the app's prod/stage/dev services.
The former standalone production-switching test launcher is retired; use the
explicit lane controls in this dashboard for runtime operations.

## Live-tracker and browser acceptance

Focused coverage for temporary watches, repeated source reads and delivery:

```bash
./scripts/e2e-docker.sh tests/test_live_tracking.py tests/e2e/test_live_tracker_workflow.py
```

The stateful-browser case uses real Chromium and a local changing source. It
checks one page load across several observations, a later match, and a delivered
alert without restarting the agent. External provider/model fakes do not replace
Nullion's routing, browser tools, SQLite state, worker or delivery boundary.

`tests/e2e/test_live_stage_snapshot_tracker.py` covers a real Web-created semantic
comparison tracker against the disposable source fixture. It verifies a successful
baseline, no alert for clock-only changes, and a visible delivered alert after an
existing total changes. `test_semantic_tracker_snapshot_workflow.py` exercises the
actual Web route, browser and SQLite worker with a deterministic provider: a
clipped source is rejected, a complete read repairs setup, and comparison plus
delivery survive worker recreation. Unit cases cover multilingual conditions,
failed reads preserving history, source-binding changes and resumed comparisons.

`tests/e2e/test_live_stage_live_tracking.py` is an opt-in real-provider Web UI
probe. Enable it only for an authorized stage instance with
`NULLION_LIVE_STAGE_E2E=1`. `NULLION_LIVE_BASE_URL` must target stage port 8753;
`NULLION_LIVE_RUNTIME_DB` points at its database for read-only evidence. Run it
inside the E2E container with a stage-only connection and export its screenshot
and source/delivery records. Never redirect this probe to a personal production
instance. It creates bounded disposable tracker records.

The stage runner installs the candidate's declared app and test dependencies,
uses a native read-only evidence bridge, and exports browser screenshots and
runtime receipts. For shared reasoning, generated-file bytes and plain-turn
latency, run:

Live runs default to one sample per scenario. `NULLION_LIVE_REPEAT` explicitly requests more samples; dedicated latency cases retain their own multi-sample assertions. Default scenario identifiers are unique and explicit selections are deduplicated. The API catalogue test stops on its first failure before spending more provider quota; resume only the failed and unrun scenarios after diagnosis. The Docker launcher forwards the named live-fixture controls and still restricts its service connection to stage port 8753. Repeated Web prompts bind evidence to the new turn identity rather than reusing an earlier receipt. Native evidence reads close their SQLite connections and can supply review events without relying on Docker Desktop's live SQLite file view.

The live runner also stops the session when fresh runtime evidence reports a terminal model quota or authentication failure, even when pytest is configured to continue ordinary assertion failures. It checks the native read-only bridge between cases and during long Web/cron waits; this makes no additional model request. The exported `provider-blocked.json` contains only the failure category, HTTP status, safe provider error codes and observation time. Existing historical warnings and user-quoted error text cannot trigger this guard. Remaining cases are unattempted, not passes. Restore the provider allowance or authentication before resuming only failed and unrun cases.

The scheduler enable/follow-up probes name disposable task IDs, use a schedule that cannot become due during the replay, and verify unrelated saved states. Cleanup writes only states actually changed unexpectedly, then fails the probe; it never rewrites every saved job as routine cleanup.

```bash
NULLION_APP_REPO="$PWD" NULLION_LIVE_STAGE_E2E=1 \
  scripts/live-stage-web-docker.sh tests/e2e/test_live_stage_web_reasoning.py
```

The controlled article fixture in `tests/e2e/fixtures/live_news_server.py` supports
new-URL versus edited-headline checks and saved-region repair. Run it in a
disposable container and provide its reachable origin as
`NULLION_LIVE_NEWS_SOURCE` to the stage runner. Both the browser service and the
E2E container must reach that same source. Without an explicit fixture origin,
those two acceptance cases are skipped; that is not live proof of those paths.
Stop the disposable fixture after the replay. Never replace a real stage source
or change stage permissions to make a probe pass.

An actual Telegram app reply is separate acceptance. A fake Update or bot-token
send proves a handler/transport path, not the user's inbound reply-to action.
Record blocked surfaces honestly; do not label them live-tested.

For an explicitly authorized saved-task replay, supply a local JSON file through
`NULLION_LIVE_CRON_REPLAY_FILE` and run
`tests/e2e/test_live_stage_web_cron_recovery.py` with the stage runner. The file
maps the selected scenario keys to `task`, `schedule`, `schedule_timezone` and
`artifact_delivery_options`. The probe preserves schedule/timezone metadata in
owned, disabled copies, invokes them once through Web chat,
and checks durable execution diagnostics and delivery. Saved task instructions
may authorize connected-account actions; choose the replay file deliberately and
retain the normal approval boundaries. Results go to the owned Web conversation.
Completed owned schedules are removed; interrupted runs remain available for
diagnosis and must be stopped before cleanup. Temporary read failures in the
native evidence bridge are retried without resubmitting the request.
Match the production agent profile when reproducing a production incident; the
private diagnostics record configured and effective check reasoning effort.
The Web probe compares the final message body to its durable chat-history receipt,
separately from typed activity cards and background-conversation broadcasts.

Scheduled recovery coverage includes long account histories with a later
calendar match omitted from the compact preview. The real scheduled runner and
persistence path must inspect that retained match and deliver one rendered
report, with exactly one calendar read and no duplicate action. Unit coverage
also checks evidence-budget bounds, every retained check identity, invalid
pointers, foreign references, and repeated-inspection termination.

Recovery scenarios also cover omitted facts in an already-researched report,
reuse of the current draft and exact reviewer feedback, a missing source found
after a report-only correction, repeated draft failure without restarting reads,
blocked sources, numbered clarifications, verifier outages, and approval-required
checkpoint writes within disabled saved jobs. Raw CDP tests inject an old
external load event and a delayed real HTTP document, follow a redirect, and
check fragment navigation without waiting for another document load.
Native-calendar coverage includes empty and nonempty returned counts, malformed
provider payload rejection, and verified no-match results through the real
adapter, scheduled runner, and Web history boundary for manual and automatic runs.
Recovery also asserts that the material requirement planner runs once across
composition, review and execution continuation. Handoff unit checks cover changed
request/context invalidation, additional proof adapters, explicit saved delivery
history, absence of runtime objects, and the plain-turn fast path.

## Keeping documentation current

When changing a command, route, setting or tool contract, check its references
in `README.md`, `docs/`, `SECURITY.md`, and plugin-local guides. Commands come
from `operator_commands.py`, HTTP routes from `web_app.py`, dependency and
entry-point claims from `pyproject.toml` and installers, and capability status
from registered provider/tool metadata. The online guide is in the separate
website repository; changes here do not publish it.

Validate local links, section anchors, code examples and advertised enum values.
Preview changed website sections at desktop and narrow widths. Keep examples
fictional and distinguish an available source capability from a guarantee that
any third-party site or account can be accessed.

Alert-delivery recovery cases run the real scheduled runner, SQLite terminal
records and platform formatter with only external provider IO scripted. Japanese
and Spanish requests cover truncated, malformed and explicitly incomplete model
outputs, a second invalid decision and provider exceptions. They assert one
source collection, one bounded protocol correction, one delivery on success,
no matching alert on failure and the persisted final failure category. Exhausted
report composition must not resume source execution without a validated work gap.

Retained-evidence checks include an action derived from a long source body older
than sixteen subsequent results, through the real scheduler and file mutation.
Only one source collection and one write are permitted. A separate case forces
an inspected completion review to return truncated JSON, then verifies that its
format correction reuses the inspection and delivers once without producer replay.
Both cases run with Japanese and Spanish requests.

Alert decision coverage also binds the full-evidence reviewer to actual delivery,
including conditions based on verified absence. Changed report text, evidence,
request, recipient, clock, history, preferences, scope and forged receipt objects
must invalidate reuse. Recovery cases make the composer invent extra work, identify
real missing work, and encounter an unavailable verifier: the independent contract
must decide whether execution resumes, and unavailable verification must stop it.

The scheduled-alert Web case starts the real FastAPI app and scheduler, persists
the source review through SQLite, and checks chat history for exactly one matching
report or no delivery for a verified negative. Each case isolates both the runtime
and chat-history store. Repeated delivery formatting is checked with non-Latin and
symbol-only job names so adapter wrapping cannot duplicate the scheduled header.

Review-protocol regressions require a malformed response after retained-evidence
inspection to receive concrete validation feedback and a larger truncation budget,
then deliver once from the same source collection. Unit cases validate the actual
JSON schema, conditional alert decision pairs, explicitly incomplete responses,
and malformed transport metadata. Negative composer fixtures return genuinely
invalid protocol output; a composer's pessimistic completion flag alone is not
an authoritative failure when independent evidence proves the requested outcome.

Saved no-tool regressions exercise the real runner with a producer that first
asks an unsupported source question. A selected source must be read once and
the report delivered; an unresolved selection must read nothing, show numbered
choices, and remain unfinished. Scheduled reasoned answers without tools and
ordinary single-call chat remain covered separately. These cases use Japanese
and Spanish requests with the same structured provider/connection evidence.

Saved-source setup regression cases simulate a six-second provider response
against the real runner, without sleeping or replacing the resolver. Requested
conditional writes must remain available behind the normal approval boundary;
existing events must not be duplicated. Timeout, malformed and interrupted
setup responses must start no source work, make no account changes, and deliver
one specific failure notice. Tests also validate the emitted JSON schema and
check that exception payloads never enter diagnostics.

Browser amendment tests wait for actual history hydration before counting the
initial bot rows. Delayed-history cases must distinguish the initial greeting
from a duplicated terminal answer while still proving one source read and one
corrected result. Long-running saved-task probes retain the owned job and record
its ID if the renderer or observation window fails before a terminal run event;
fixture cleanup must not turn that diagnostic failure into a user cancellation.

Retained-evidence regressions validate the provider response against the actual
schema supplied in that request, including the inspection alternative. Both
completion and action review must allow a bounded inspection before it is used
and require a normal decision afterward; recorded source work and writes must
not be repeated to recover missing preview fields.

`test_retained_inspection_handoff.py` exercises the real scheduled runner,
graph, SQLite checkpoint and Telegram delivery pipeline with fakes only at
external model/source/transport boundaries. Spanish and Japanese cases inspect
a field omitted from a long preview, carry it from reviewer to composer or
composer to reviewer, correct the report and deliver exactly once with one
source read. A still-wrong revision must remain rejected despite its preserved
source windows. Unit cases in `test_retained_review_inspections.py` invalidate
changed tool output/status/identity, discard foreign or corrupt descriptors,
rebind visible aliases, enforce shared budgets and hash each inspected source
only once per reconstruction. These local cases do not establish live stage
acceptance; live stage tests remain opt-in.

Pagination coverage tests exercise real native adapter calls through the Web
route and verify the review packet, stored tool receipts and visible result.
Unit cases also cover missing initial/middle pages, cycles, conflicting receipts,
failed calls, duplicate pages and unrelated scopes. A terminal page alone must
never establish a complete chain. Failure-delivery cases retain the reviewer
authored confirmed findings without publishing the rejected draft, and keep
incomplete conditional monitors silent.

`test_progressing_completion_recovery.py` runs the real graph and SQLite store
against an external-provider fixture that submits a premature draft, then reads
the remaining selected records. Spanish and Japanese cases verify the final
answer, completed evidence and durable review/budget events. Adjacent cases
prove duplicate calls remain bounded and progress cannot expand the turn budget
after its existing one-time recovery allowance. Unit cases cover failed, denied, negative, unrelated and repeated
receipts plus the no-scan ordinary chat path.

`test_scheduled_constraint_evidence.py` exercises a five-requirement batch report
through the scheduled runner, SQLite events, and Telegram transport boundary.
It verifies shared source proof across execution constraints, findings, action
checks and report delivery, with a tools-disabled wording correction and no
repeated source work. Reused proof for independent facts and unknown inspection
citations cannot certify completion: the corrected report remains available with
internal helper availability diagnostics. Japanese and Spanish prompts exercise the same typed path.

`test_review_availability_delivery.py` covers unavailable requirement planning,
review timeouts, invalid JSON and invalid citations through the real graph,
scheduled runner, SQLite audit, Telegram transport adapter and a local Web UI
driven with Playwright. It verifies one visible answer without reviewer-status boilerplate, unknown completion,
no duplicate source work, no duplicate wrapper review, and preservation of successful
findings/files when a sibling fails. Failed/denied required sources remain negative
cases; an uncited critique cannot suppress a primary result. External providers/account adapters are
fakes; these are not live stage or native Telegram acceptance.

`test_profile_clarification_continuation.py` drives real Telegram ingress,
conversation persistence, tool scoping, the action preflight and outcome graph.
Scripted external providers reproduce a missing profile default and a linked
follow-up that produces only a promise. The final result must contain a completed
regional lookup backed by stored tool receipts. Cases cover current-location
overrides, cancellation and explanation-only replies, plus a non-English
continuation. Ordinary-turn parent lookup and cross-workspace rejection are
covered by `test_profile_pending_context.py`. These container checks do not call
real account providers or lift the user's hold on live stage acceptance.
The file cases also reject corrupt selected files and dependent reports whose
structured prerequisites failed, are missing, or form a cycle. Transitive
dependencies cannot bypass this boundary. `test_delegated_report_completion.py`
distinguishes a genuine rejected source from an unavailable review through the
real Deep Agents runner and Telegram upload adapter, including internal group
helper status and unchanged delivery-failure behavior.

Live stage acceptance for this change is held by the user. When resumed, replay a
normal report and a report with a deliberately unavailable review provider on the
stage Web UI and real inbound stage Telegram app, then verify the single answer
without reviewer boilerplate and the durable helper/transport states. Exercise a genuine missing-action or
missing-file case alongside it. Do not inject failures by changing production
credentials, configuration or runtime data.

For model calibration, annotate sanitized observed traces with `case_id`,
`expected: {correction_needed: bool, must_block: bool}`,
`review: {disposition: string, error: string|null}`, and
`publication: delivered|warned|blocked`, then run
`python scripts/evaluate_completion_reviews.py labelled-traces.jsonl`.
Include both successful and defective cases. This offline command measures supplied
labels; it does not run or certify a model. Scripted E2E coverage is separate.

`test_report_only_completion_revision.py` exercises the full scheduled runner,
SQLite audit, Telegram formatter and transport boundary. It covers a rejected
text deliverable whose source read succeeded, proves the producer does not
restart for a tools-disabled wording correction, and checks exactly one final
delivery. A deliberately incorrect second draft must remain rejected.

`test_telegram_pending_send_and_cron_recovery.py` uses the real messaging runner
and SQLite store with delayed Telegram SDK acknowledgements. English, Japanese,
and Spanish replies must leave one visible message and a durable delivery receipt;
failed final edits withdraw the draft before fallback delivery. The scheduler
case kills a real isolated worker, restarts the app, checks that its dead-owned
lease is reclaimed, and verifies one interruption notice through Web history and
status APIs. These are container boundary tests, not native Telegram app or live
stage acceptance. Live stage replays remain separately opt-in.

`test_report_findings_survive_review.py` runs the real graph, scheduled workflow,
SQLite delivery history and Telegram formatting/transport boundary with scripted
external sources and provider responses. Two languages and two source families
cover a successfully corrected report, exhausted presentation correction with
item details retained, and unknown finding citations rejected. The tests assert
one source read, the actual delivered text and persisted history, and honest
completion state. These fixtures verify runtime contracts; they do not measure
real model judgement or replace separately authorized live acceptance.

`test_tracker_setup_recovery.py` replays a missing selector, a truncated 40/197
collection, a reviewer revision that leaves the tracker unregistered, selection
of a witnessed complete DOM region, an invalid action-review response, and
successful tracker creation through the real Web chat route.
Japanese and Spanish fixtures verify a 15-minute interval, four-hour window,
durable baseline, no early due claim, and one exact-link alert after a new record
appears. Advancing the SQLite claim clock exercises the later poll without a
15-minute wall-clock wait. Polling must reuse the prepared plan rather than call
the chat agent again. Browser/source and model boundaries are local fixtures;
live stage and real-provider acceptance remain separately opt-in.

`test_report_content_outcome.py` creates real scheduled jobs through FastAPI and
runs the actual scheduler, SQLite persistence, fallback document producer and Web
chat storage. Japanese and Spanish cases distinguish complete, partial and absent
completion metadata, verify one delivered file containing source-backed values,
and assert `ok` versus `partial_success` in the persisted job. One source read and
one generation call are allowed; no additional grader or source replay is used.
Unit cases cover malformed/foreign/duplicate metadata, changed requests and
files, CSS-only delivery normalization, pending approvals, cancellation, failed
delivery and verified-match alert boundaries. External providers are local fakes;
these tests do not establish real-model judgement accuracy or live acceptance.

`test_http_connection_reuse.py` measures accepted sockets at a real Uvicorn
server using the production keepalive setting, real health/status routes and
SQLite runtime. It covers repeated tray polls, an idle gap beyond the previous
five-second timeout, instance isolation, overlapping refreshes, server retirement
and restart, pool shutdown, and an ambiguous restart POST without replay. Model
stream cases use a local provider boundary, check model-variant reuse for fully
consumed bodies and preserve early-terminal response behavior. Unit coverage
also simulates source-port exhaustion and verifies bounded health-check backoff,
thread-safe pool ownership, current auth headers and per-request deadlines.
These checks run in Docker; they neither call paid providers nor run live stage
workflows, and they do not simulate the macOS kernel's socket-retention behavior.

## Health alert acknowledgements and delivery requirements

`test_health_action_acknowledgements.py` drives the real Telegram callback adapter
against a local Bot API HTTP boundary, confirms the edited card bytes, and reloads
the SQLite status. It also exercises the real Doctor/Sentinel FastAPI routes and
the Web UI resolution button. Cases distinguish manual resolution, dismissal,
in-progress status, restart requests, stale buttons, unsupported commands and
manual-only plugin guidance. No host restart is performed.

`test_request_delivery_requirement_handoff.py` uses the real chat routes and shared
messaging runner, a deterministic external calendar/model/renderer, and actual
Telegram HTTP delivery. English, Japanese and Arabic cases reject an unrelated
image attempt, retain the current empty output contract, and preserve confirmed
calendar findings when a requested PDF cannot be generated. The checks assert
both user-visible text and backing tool/persistence evidence. Unit cases cover
request/evidence invalidation, untrusted output payloads, compaction round trips,
pre-scoped tools and missing write receipts. These are local container tests; live
stage and native Telegram acceptance remain separate and must be reported honestly.

## Generated artifact protocol recovery

`test_generated_artifact_protocol_recovery.py` exercises the real streaming
model adapter, chat runner, SQLite store, permission/boundary enforcement,
action reviewer, local file writer and subprocess renderer. Only the external
model and calendar are deterministic fixtures. Truncated JSON, a stream ending
without a completed call, and missing schema arguments must yield a structured
correction before action review or execution. The repaired workflow must reuse
one calendar read and produce an actual one-page PNG or PDF. Web cases download
the file through FastAPI; Telegram cases verify the multipart Bot API upload
bytes. Pixel checks and PDF page/image inspection reject a text-only fallback.
The cases cover English, Japanese and Arabic. They are local Docker acceptance,
not a substitute for live stage Web UI or real inbound Telegram app validation.

`test_tool_argument_protocol_recovery.py` also checks the framework handoff:
LangChain retains malformed/incomplete calls as invalid calls, and a real
LangGraph `ToolNode` must not execute a tool's optional default arguments for
those rejected calls.

The generated-artifact protocol recovery contract includes production-style
768-token fast-profile limits at the local HTTP/SSE model boundary. Rendering
must recover the configured workflow budget and deliver actual PNG/PDF bytes
through Web download and Telegram multipart upload, in multiple languages.
Retained-evidence tests also cover early archival below the aggregate threshold,
exact inspection across report correction, fresh parallel batches, protocol
receipt identity, and runtime-owned checkpoint replacement. These deterministic
container checks do not establish live provider reasoning quality; live stage
acceptance remains a separate opt-in run.

Reviewer publication coverage exercises real Web chat, Chromium rendering and
reload, Telegram HTTP delivery, SQLite history, native file creation and uploaded
report bytes with scripted external review responses. Multilingual editing
instructions must stay internal. The contracts also cover incomplete requirement
matrices, missing required execution, numbered clarification/approval, partial
report details, valid/blocked file delivery and both sides of UTC midnight.
These local provider-boundary checks do not establish live model judgement
accuracy; opt-in live stage checks remain separate.

Limitation-role contracts prove that a failed/denied receipt can explain a public
limitation while the same receipt cannot certify source facts or completion.
Tracker expiry acceptance also holds a real competing SQLite write transaction
and verifies browser delivery plus the numbered extension reply.

The storage-lifetime contracts retain real SQLite connection objects to verify
closure without garbage collection, and exercise commit, rollback, early return,
setup failure and deferred-constraint commit failure. The scheduler API is polled
repeatedly with GC disabled; the Linux container checks descriptor growth plus
average/worst-request latency. Personal tasks are saved, restarted and recalled
through the existing messaging/Web boundaries with literal special-character
database paths. Conversation analysis uses a real unreadable database location
and must return its controlled HTTP error. Source-fetch cases inject failures
only at the external transport/model boundary and verify the public HTTP status,
private-error suppression and backing failed tool receipt through `/api/chat`.
The real connection-test API also verifies response closure after HTTP rejection
and error-body read failures; adjacent connector, media and loading-screen units
exercise the same ownership rule.
These are local container contracts, not live stage or native Telegram acceptance.

Failure-attribution contracts exercise real browser extraction from a controlled
HTTP page, an HTTP source adapter, outcome review, SQLite history, Web UI/reload
and Telegram HTTP delivery. Spanish and Japanese cases cover a failed extraction
followed by useful source facts, a genuinely blocked second subject, an entirely
failed source path, explicit public limitations, and an update without readback.
Unit coverage checks failed-receipt bindings, alias resolution, invalid and
recovered references, consent preservation and recomputation on continuation.
External model/source responses alone are scripted; no live account is needed.

`tests/e2e/test_live_stage_failure_attribution.py` is separate opt-in Web UI
acceptance with a real stage model. Besides `NULLION_LIVE_STAGE_E2E=1`, it requires
an explicit `NULLION_LIVE_FALLBACK_SOURCE_URL` and
`NULLION_LIVE_FALLBACK_EXPECTED_TEXT`. Supply an authorized controlled page with
an empty `#empty` region and a substantive `#observations` region containing the
expected literal text. It checks the initial fallback and a follow-up through
the real UI and backing tool receipts. Do not enable it while live stage runs
are held. Local contracts do not establish real-provider judgment accuracy or
native Telegram app acceptance.

When native Telegram live validation is authorized again, send that controlled
source request and its follow-up to the stage bot from the Telegram app. Verify
one terminal delivery for each request, the confirmed source text and actual
remaining uncertainty, plus matching current-turn failed/successful receipts in
the stage runtime. A recovered extraction must not become a blanket outage.
The local Telegram HTTP contract and the Web live scenario do not replace that
native app check; it remains pending while live stage runs are held.

`tests/e2e/test_cron_completion_execution_recovery.py` exercises the real
scheduled runner, graph, SQLite diagnostics and platform upload boundary with
external model/source fakes. It covers native and generated partial HTML,
finishing missing records in the same run, a productive interrupted continuation,
persistent outages, unchanged partial fallback bytes, one selected report file,
verified negative browser/native monitor checks, inline requirement correction,
and action-review pointer correction before one actual write. Assert research
completion and uploaded content, not merely a nonempty failure message.
The cancellation case stops at the producer/report-owner handoff and asserts
zero subsequent model calls while preserving the cancelled terminal outcome.

Focused unit coverage lives in `tests/test_scheduled_completion_execution.py`,
`tests/test_progressing_recovery_budget.py` and
`tests/test_action_review_protocol_repair.py`. It includes cross-format partial
version guards, schema/receipt ownership, missing cursor pages, blocked actions,
unavailable tools/reviewers and fixed producer budgets. These local contracts do
not prove the judgment or availability of a live third-party source. Keep opt-in
live stage probes disabled while the user has held them.

`tests/e2e/test_artifact_summary_revision.py` covers native prepared HTML and source-recovered HTML with a rejected final chat summary, through the real scheduled runner, SQLite review records and Telegram upload bytes. Japanese and Spanish requests verify one bounded tools-disabled revision, fresh review, no repeated source reads or duplicate reports, and report preservation during composer outages. `tests/test_artifact_summary_coverage.py` checks evidence-only coverage against unknown citations, mismatched contracts, failed requirements, blocked/approval/input states and retry tools, plus suppression of private correction prose in captions. The upload contracts also include an internal history dataset, changed-file invalidation during composition, and cancellation before and during summary correction with no later calls or delivery. Live stage remains opt-in and must stay disabled while held.

Search coverage contracts live in `tests/test_search_coverage.py` and `tests/e2e/test_search_coverage_boundary.py`. They exercise Japanese/Spanish requests, primary-empty/alternate-positive results, scoped negatives, early positive completion, incorrect filters, correction of an incorrectly narrowed scope, blocked alternatives with useful partial answers, durable candidate metadata, and measured five-request plain-chat latency with a forbidden slow discovery dependency. The E2E uses the actual graph, SQLite, FastAPI Web route and scheduled Telegram delivery boundary; only external providers and sources are scripted. `tests/test_cron_continuation_review.py` reproduces a resumed producer returning success without an independent receipt, including failure that must retain supported findings.

`tests/test_bounded_comparison.py` and `tests/e2e/test_bounded_comparison_boundary.py` distinguish a scoped recommendation with incidental pending listings from an unresolved ranking contender. The graph/SQLite, Web route and Telegram HTTP delivery tests prove an observed recommendation can finish with thirteen unverified discoveries retained, a credible contender receives a targeted source read, scope-only wording repair performs no further source reads, and blocked contenders keep useful facts with a specific limitation. Platform tests verify one delivered reply and persisted history. Japanese/Spanish prompts share the same typed path. Explicit exhaustive comparisons still inspect the discovered sources; unit cases also cover missing selected-source proof, requested subjects, discovery truncation, incorrect filters and legacy negative/comprehensive coverage. Provider calls and external source adapters are deterministic; no live stage/provider calls are used by these tests.

`tests/e2e/test_live_stage_search_coverage.py` is opt-in through `NULLION_LIVE_STAGE_E2E=1` and `NULLION_LIVE_COVERAGE_CASE`, a JSON file containing `prompt`, `expected_source_url`, and `expected_text` for controlled public fixtures. It submits through stage Web and checks both the visible alternate match and backing source/review evidence. Do not run it while stage acceptance is held. These local tests do not establish a real model's judgment accuracy or current commercial inventory.

Controlled coverage pages are provided under `tests/fixtures/search_coverage/`: the discovery page links to an empty primary catalog and an independently stocked format-specific catalog. When live stage is authorized, serve those pages through the permitted test-fixture origin and generate the live case JSON with that origin, the requested fixture constraints, alternate URL and literal listing ID `COVERAGE-IMAX-007`. Do not treat these pages as real commercial inventory.

Scheduled chat presentation uses the same guidance in the original producer and interruption recovery. `tests/test_scheduled_chat_presentation.py` checks that requested provenance and detailed formats remain intact. `tests/e2e/test_scheduled_chat_presentation_delivery.py` exercises the real scheduled runner, SQLite and Telegram delivery with Japanese/Spanish requests and a provider interruption: compact source metadata, reconciliation uncertainty, one delivery, and no extra presentation model pass. The external model is scripted; these tests establish instruction propagation and delivery preservation, not subjective real-model writing quality. Live stage acceptance remains a separate opt-in check.

`tests/e2e/test_saved_job_configuration_repairs.py` exercises actual job creation and scoped updates through FastAPI/SQLite, using Japanese and Spanish labels. It checks current-evidence task updates, disabling an expired annual alert without deleting its history, and local market-alert next-fire times in winter and summer. Unrelated settings and jobs must remain unchanged. This is configuration persistence and scheduling coverage; it does not execute real account searches or prove report writing quality.

`tests/e2e/test_review_report_repetition.py` drives the actual runner, scheduled graph, SQLite and Telegram transport boundary with a producer report, a reworded full helper report, and a narrow limitation. Japanese and Spanish variants require one report and one limitation on the wire, with no repeated research. `tests/test_review_assistance.py` covers legacy and structured replacements, explicit additions, covered facts, empty drafts and idempotent handoff. `tests/e2e/test_calendar_nearby_duplicate_review.py` uses the real connector adapter and action preflight: an existing reminder beyond the first three events, on a nearby date, must be visible before the proposed write. Only external provider/model boundaries are faked; no calendar POST may occur. `tests/test_calendar_review_evidence.py` checks opaque continuation replay, provider isolation, malformed pages, preview counts and complete cursor chains.

`tests/e2e/test_scheduled_review_outage.py` also reproduces an evidence inspection followed by a provider timeout. Its model-boundary clock consumes the passed timeout and requires subsequent attempts to stay within the original total allowance. The real scheduled runner, SQLite, shared report renderer and Telegram upload path must retain one concise caption and the exact HTML bytes, without a second research run or a failed-task notice. Unit cases in `test_compact_report_contract.py` cover report envelopes with attachment descriptors across HTML, PDF, Office, image, data and archive extensions, compact/detailed preference preservation, unknown trailing text, and empty media markers. The model and network remain simulated in these boundary tests; a separately recorded real-provider writing replay checks the supplied report's summary quality without repeating live research or delivery.

Attachment acknowledgement regressions are covered by `tests/test_delivery_acknowledgement.py`, `tests/e2e/test_delivery_acknowledgement_boundary.py`, and `tests/e2e/test_adjacent_attachment_acknowledgement.py`. They distinguish server acceptance from an SDK response, inspect actual multipart bytes and persistent receipts, and cover HTML/PDF/Office/image/archive/text data, delayed success, accepted-but-lost acknowledgements, safe pre-connect/rate-limit retries, explicit rejection, acknowledged prefixes in multi-file batches, Telegram scheduled/operator/reply/callback paths, Slack/Discord, and SQLite outbox recovery after an interrupted send. The external HTTP transport or SDK is simulated; no production account is contacted. The cron transport test enters the real delivery graph at its completed-execution boundary; `tests/e2e/test_changed_report_review_boundary.py` separately drives the real producer and native file handlers through research recovery, unchanged-file/new-evidence recovery, reviewer outage and unavailable evidence, asserting final upload bytes and persisted completion status in Japanese and Spanish.

These focused Docker checks do not replace user-authorized live stage acceptance. A lost remote acknowledgement cannot be reconstructed by a boolean send probe; do not claim exactly-once delivery or successful completion from a pending/unknown receipt. Hold broad/live runs when requested.

For focused Docker runs, `scripts/pytest_target_order.py` groups the initial explicit path-selector block by directory metadata. This avoids duplicate directory collector identities in recent pytest versions when parent/unit and child/E2E files are interleaved. Option/value pairs, node-id suffixes, duplicates and in-directory order are preserved; invalid/plugin selectors are left for pytest to validate. Normalization failure aborts the runner rather than falling back to an unrequested full collection. `tests/test_pytest_target_order.py` checks selector/option preservation, and `tests/test_pytest_fixture_scope.py` checks actual fixture resolution when mixed E2E cases are selected.

Adjacent notification acknowledgement coverage lives in
`tests/e2e/test_adjacent_notification_acknowledgement.py`. It exercises real
platform adapters and SQLite owners for final text replies, stream edits,
live-tracker alerts, Doctor notices, reminders and outbox failure notices.
External transports simulate remote acceptance followed by response loss;
assertions check accepted messages, persisted unknown status and no automatic
replay after reopening the store. Separate cases preserve plain-format fallback
after explicit rejection and acknowledged alerts after a local history failure.
Telegram coverage additionally exercises ordinary final replies and pending
stream creation through real SDK requests: unknown acknowledgements survive
finalization without another message, while typed rejections retain fallback.

Search-validation regressions include paraphrased constraint descriptions with
stable ids, discovered URLs followed by filtered source and booking pages,
structured collections, multi-page evidence, earlier selected reads and bounded
discovery inventories. Positive find-any outcomes retain ancillary diagnostics
without inventing unfinished work. Wrong filters, unsupported matches, broader
negative results and incomplete comparisons remain incomplete. Invalid review
bindings preserve usable findings with a review warning and do not trigger
another source read. These contracts use local provider/network fakes through
the real orchestrator, Web API, SQLite and scheduled delivery boundaries; live
stage remains opt-in.

Source-answer failure recovery is covered by
`tests/test_source_answer_failure_recovery.py` and
`tests/e2e/test_source_answer_failure_recovery_boundary.py`. The E2E drives the
real Web route, LangGraph, SQLite, source renderer and Telegram SDK with external
model/source/HTTP fakes. Japanese and Spanish cases cover model timeouts,
transport errors, repeated malformed selections, absent selections and independent
semantic rejection. They assert selected findings survive, unselected inventory
and controls stay hidden, research runs once, and exhausted preparation does not
start another review. These controlled checks do not establish live-provider
latency or replace an authorized live-stage replay.

`tests/e2e/test_selected_request_objective_boundary.py` exercises Web and real
Telegram ingress with persisted parent turns and platform delivery receipts.
It deliberately changes quoted Markdown to platform display text, interleaves
an unrelated report, and uses two-step numbered clarifications. Japanese,
Spanish and French height/weight requests first receive a wrong roster answer;
the real review/execution graph must fetch the missing properties, publish
them, and record a complete verdict without an unrelated source-discovery
warning. Raw adapter results and captured source-answer composition are both
covered. External model, source and send responses are controlled; these tests
do not establish real-provider reasoning quality. `tests/test_request_lineage.py`
and the source-attachment regressions cover reset, cancellation, cross-workspace,
cycle, bounded-context and in-flight input protections. Existing measured plain
chat contracts continue to guard the unrelated-request fast path.

`tests/e2e/test_plan_revision_recovery_boundary.py` replays a persisted numeric
height-request follow-up through Web and Telegram after a browser proposal is
rejected by action preflight. The corrected proposal must execute once and
deliver the requested properties with source evidence. Additional real graph
and SQLite cases cover other action tools, approval suspension/resume,
delegated execution, unchanged rejected proposals, productive scope recovery,
and a scope loop that must terminate without repeated completion reviews.
Provider/source/platform responses remain controlled; no paid live stage probe
is implied. `tests/test_plan_revision_recovery.py` covers malformed contracts,
non-English diagnostics, proof receipts, checkpoint restoration, and unresolved
execution failures. These are focused local regressions, not live-provider
acceptance evidence.

The productive-continuation cases in
`tests/e2e/test_cron_completion_execution_recovery.py` also guard the report
boundary after multiple provider interruptions: completed research and an
updated file must reach independent final review, rather than being stranded
behind a byte-matching partial/unknown receipt.

`tests/e2e/test_structured_review_layout_boundary.py` runs independently reviewed
partial comparisons through the real Web API, narrow Web DOM and Telegram HTTP
transport. Each comparison unit must remain one item with its related values,
source link and uncertainty. Section headings, item boundaries, inline emphasis
and limitations must survive SQLite persistence and browser reload without raw
Markdown or redundant generic disclaimers. Adjacent Web cases cover indented
details, paragraph continuations, nested items and literal prose. External
provider responses are controlled; this does not run a paid live stage probe.
`tests/test_structured_review_layout.py` covers legacy/prose preservation,
malformed display metadata, evidence-reference resolution, numbered choices,
review outages and operational notices that must remain visible.

Structured source coverage regressions also live in `tests/test_declared_source_coverage.py` and the structured-adapter cases in `tests/e2e/test_bounded_comparison_boundary.py`. They cover nested declared records with no text, parent-bound evidence aliases, empty/failed records, identity-only compaction, credentials in URLs and linked-target separation. The graph and real Web route must retain verified prices and actionable limitations. `tests/e2e/test_live_stage_bounded_comparison.py` uses the disposable `live_source_server.py` fixture and a real stage Web/provider request to exercise a scoped recommendation. `live_cdp_service.py` supplies an isolated container browser at the configured stage-only endpoint when no external stage browser is running; it never mounts a host browser profile.

The live stage audit runs sequentially and stops on the first failed case. Successful plain/weather preambles with identical arguments are reused within one runner process for at most 30 minutes; affected workflow requests are always executed. A new runner or candidate revision starts fresh baseline measurements. Keep successful case node IDs when resuming an interrupted audit so that provider-funded cases are not repeated unnecessarily. Saved-task replay datasets may be explicitly bounded for the audit; record that scope rather than representing them as full mailbox or full research replays.

### Shared presentation and live-condition amendments

`test_shared_presentation_delivery.py` saves style settings through FastAPI and
checks ordinary chat, scheduled runs, Doctor notices and Slack/Discord upload
bytes. It asserts unchanged facts, artifact bytes, numeric choices and the
ordinary-turn latency/call-count budget. `test_live_tracker_verified_change_delivery.py`
checks saved emoji and Markdown settings through real SQLite, evaluation, Web
history and Telegram delivery. `test_live_tracker_condition_amendment.py` starts
with an insufficient list-only answer, exercises reviewer recovery through the
real chat route, verifies the saved matcher receipt, then polls new data and
delivers only qualifying changes. It also checks original deadlines, snapshots,
invalid amendments and original-chat ownership. External providers and platform
network clients are the only scripted boundaries.

`test_live_stage_web_tracker_updates.py` adds a real-provider Web acceptance for
an existing tracker's condition amendment followed by continuing delivery. It
checks a Spanish amendment, unchanged identity/cadence/deadline, a fresh baseline,
suppression of unrelated new articles, two qualifying changes, no duplicate on
an unchanged poll, and one visible copy of each alert after reload. The tracker
stays active between matches and is stopped by fixture cleanup. The disposable
news source namespaces these changes by observation owner; the Docker network
check `test_live_news_fixture_isolation.py` verifies isolation without model calls.
Run this case with `NULLION_LIVE_STAGE_E2E=1` and `NULLION_LIVE_NEWS_SOURCE` using
`scripts/live-stage-web-docker.sh`; it must never target production.

`test_live_stage_web_snapshot_tracker_ignores_clock_and_alerts_on_changed_total`
checks ongoing tracking of existing values against the disposable comparison
source. It requires no initial or clock-only alert, one alert for each of two
successive vote-count changes, no duplicate on an unchanged poll, and an active
tracker after both matches. The source is refreshed in its existing worker tab.
`test_live_source_fixture.py` verifies the source's clock and two independent
changes over HTTP, including isolation from a second observation owner.

Native stage evidence GETs share `scripts/live_stage_http.py`. Transport errors
and 502/503/504 responses receive a bounded retry; authentication, authorization,
invalid requests and malformed JSON fail immediately. The helper refuses
mutations and never resubmits the user request. Provider-guard subprocess E2E
checks cover a transient first read followed by a real SQLite quota receipt,
proving later paid scenarios still stop. The continuing-tracker acceptance also
records setup/amendment latency and bounds unnecessary completion-review rounds.

The continuing-alert replay also requires each matched headline and its verified
direct link to survive generic evaluator summaries. `complete_verified_findings`
in the shared message layer fills missing context from exact current evidence;
it does not issue another model call or select other collection rows. Unit cases
cover multilingual summaries, unsafe citation URLs, field/whole-record pointers
and idempotence. `test_generic_semantic_match_delivers_verified_record_and_link_once`
checks the real SQLite worker, notice delivery and Web history boundary.

For an interrupted receipt-continuity replay, `NULLION_LIVE_RECEIPT_REPLAY_ID`
selects its owned `web:live-ui-audit:` conversation. The probe verifies the saved
image summary, draft, exact form URL and empty fields, then submits only the
remaining follow-up through Web. Navigating to a verified form and clicking its
link are equivalent; the test requires the resulting form state and zero actual
submissions. Do not repeat already completed provider-funded setup to repair a
harness assertion. Native evidence outages must not prevent cleanup of an exact
owned tracker through the stage API; foreign names/conversations stay unchanged.

Requested in-chat questions, wording and response structure use the existing
`reasoned_answer` proof domain; live facts and external actions retain tool-bound
proof. `test_response_content_requirement.py` checks both domains in one real
FastAPI turn with Japanese/Spanish requests. The opt-in Web counterpart
`test_live_stage_web_response_proof.py` checks real forecasts, visible questions,
complete reviews and latency. The in-flight WebSocket probe records per-request
latencies separately from total workflow duration, so its single-request limit
is not applied to the sum of three requests. `test_live_active_probe_timing.py`
checks this distinction without provider calls. When coordinating the entire
live audit, run identical API/Web catalogue prompts once through Web, and retain
API-only scenarios separately; document that selection explicitly.

Stage file-lookup checks use an owned temporary directory beneath the running
stage workspace, mounted separately into the Docker probe. They never write
fixtures into a container-only imitation of the stage home. The launcher removes
only that owned directory after collecting evidence. Existing-file listing and
requested-file download checks share this setup.

`test_file_lookup_completion.py` covers scoped negative filename evidence,
non-English answers, and rejected unrequested writes against the real API or
LangGraph/SQLite runner. `test_live_stage_web_file_lookup.py` checks the real
stage Web result and backing tool evidence. A complete filename scan can prove
absence within its declared roots and exclusions; denied, interrupted, truncated,
or bounded content-prefix scans cannot prove general absence. Missing output
files still require successful generation and delivery evidence.

Completed runtime reviews also bind the exact request, response, tool evidence,
and file versions before optional common retries are considered. Changed
requests, changed evidence or files, incomplete reviews, and tool-output lookalike
receipts retain the recovery path. Rejected plans are reconciled with the cached
material contract before the missing-tool gate; this does not add a planner call
to ordinary requests. Recovery may omit an unrequested action instead of forcing
another attempt with the same tool. The API regression includes a rejected write
and verifies no execution, one review, and preservation of the actual lookup answer.

Advertised artifact download URLs must return the actual bytes. Live checks fail
on HTTP errors even if a matching local file exists. Exact existing-file delivery
compares the downloaded SHA-256 with the source fixture; inspection tools alone
are not failures and do not prove delivery.

Batch scheduler mutations declare item collections with exact task IDs, allowing
separate requested actions to cite separate runtime-bound records. Batch starts
expose only successfully started items as action evidence; failed or missing
items cannot borrow a successful sibling's receipt. The Japanese/Spanish API
regression verifies two selected tasks change, another remains unchanged, and
completion needs one planner and one review without task-list rereads. The real
stage bulk-enable contract retains its existing latency limit.

Paid API scenarios can run individually through
`test_live_stage_api_scenarios.py`, with an explicit `NULLION_LIVE_SCENARIOS`
selection. Empty selection never expands to the full paid catalogue. This keeps
separate receipts and lets fail-fast runs resume without repeating successful
workflows. The fresh scheduler follow-up requires its own fulfilled runtime turn
and a read-only inventory check; tool use alone is insufficient. It measures the
slower individual request separately from the full two-turn workflow.

Screenshot follow-ups require `NULLION_LIVE_CAPTURE_SOURCE`, pointing to the
owned `/cart/` fixture in `live_source_server.py`. A URL query parameter on
`example.com` does not create a cart. The capture must appear in the API's actual
artifact receipt and download as a nonuniform image. A disk-only artifact cannot
replace a missing delivery receipt.

`test_scheduler_inventory_version_recovery.py` changes the selected task through
an independent SQLite-backed writer between inventory reads. The real chat API
must retain the failed version receipt, pass corrected arguments through compact
context, and reread the same source successfully. Japanese and Spanish requests
share that path. Unit coverage also verifies ID lookup, scope-preserving recovery
arguments, and retention of failure/permission controls without verbose task prose.

Source artifact handoff coverage: `tests/e2e/test_source_artifact_contract_delivery.py` drives the real chat API, Chromium, saved browser-session reuse, SQLite receipts, and downloaded PNG bytes with Japanese and Arabic requests. It also checks that an incidental research capture stays private even if an earlier tool scope overstates delivery. `tests/test_requested_source_artifacts.py` covers independent deliverable contracts, eligible producer metadata, allowed roots, current file identity, format constraints, internal roles, and ambiguous version selection.

The stage Web wrapper mounts its owned ephemeral fixture directory at the same absolute path inside the container and on stage. Both artifact and archive-upload fixture roots point there. Archive and planner probes reject missing or mismatched shared roots before invoking a provider; `tests/test_live_stage_fixture_paths.py` covers that quota guard.

Archive prerequisite coverage: `tests/e2e/test_archive_prerequisite_evidence.py` uses the real API, archive extractor, workbook generator, SQLite state, and downloaded workbook bytes. It requires complete outcome review while keeping an intermediate manifest out of delivery. Archive and retry-guard probes additionally require fulfilled backing source work. Artifact-package acceptance independently checks required files, contents and rendering; an optional review outage is recorded separately, while a known material failure still fails acceptance.

`tests/e2e/test_browser_retained_source_recovery.py` covers browser-to-PDF recovery after a rejected browser-report claim: the real chat route and persistent runtime inspect an earlier retained capture, create one PDF, and verify its downloaded price bytes without a second browser request or guessed file paths. The external browser navigation and provider are scripted; the native evidence collector, retained inspector, PDF generator, routing, review binding, and persistence remain real. `tests/test_browser_retained_documents.py` checks the compact index and rejected-claim boundary.

`tests/e2e/test_generated_artifact_inspection_delivery.py` also exercises abbreviated image, PDF, and CSV inspection paths across three languages. It requires recovery to an exact retained path, one final file set with verified download bytes, and no spurious access approval. Unit coverage in `tests/test_artifact_read_references.py` checks additional formats and ensures a deliberate different-path retry retains normal access policy.

The live flight correction contract permits a numbered clarification when relative dates are ambiguous. It resolves that clarification and requires a fulfilled current turn and one downloadable PDF; keeping the original comparison must preserve its exact bytes without repeating research or generation. Its longer HTTP observer budget does not relax the separate latency assertion or production limits, and a disconnected observer stops only its owned conversation. Status GETs and artifact downloads use the same explicitly configured authority header; transient read retries never replay a chat submission.

`tests/e2e/test_scheduler_inventory_family_discovery.py` starts with an empty live-tracker inventory while the named record exists as a cron or reminder. Multilingual API and rendered Web tests require the actual related inventory read, a fulfilled status answer, and unchanged saved state. Representative plain turns measure average/max latency and require one provider call each. `tests/test_scheduler_inventory_scope.py` covers metadata-based visibility, read-only scope retention, later explicitly planned mutations, and reminder workspace boundaries.

Reusable-tool workflow coverage independently requires the pre-save catalog
search, durable save, and exact-revision run through the real API/Web, catalog
SQLite store, terminal boundary, and outcome reviewer. It rejects false partial
results, duplicate operations, and extra review retries before reusing the saved
revision in a fresh runtime. The live counterpart checks `response_fulfilled`
for creation and reuse, alongside the actual revision and computed results.
Unit cases distinguish untested-definition metadata from current operation
failure and validate empty catalog search receipts.

Internal working-file tests cover deliverable, action, and execution-only constraint
contracts in three languages. The real DOCX route must complete review once,
deliver only the final document, verify its downloaded contents, and preserve
that selection on history reload. Artifact-binding unit cases cover nine file
formats and reject external-fact use, public constraints, selected-file
substitution for internal prerequisites, ineligible or missing/failed producers,
and unbound record claims.

`test_background_setup_source_coverage.py` uses the real chat route, rendered Web,
native tracker tools, SQLite state and a controlled HTTP source. Japanese and
Spanish setup requests must complete once despite incidental source discovery.
A mixed immediate-lookup request retains a real coverage assessment, and omitting
that assessment must remain incomplete. A separate ordinary turn still uses one
provider call. The live finite-window and numbered-clarification probes also
require a fulfilled setup result alongside the actual persisted configuration.

Compact report contracts are covered by `tests/test_compact_report_contract.py`
and `tests/e2e/test_compact_report_delivery_contract.py`. Providers return both a
verbose legacy fallback and a typed report; tests assert the actual scheduled,
recovery, delegated and grouped delivery uses the shared compact renderer, retains
all required findings, and preserves readable paragraphs. A malformed layout must
retain useful content without failing completion. A measured multilingual plain
chat route remains one model call with no report-rendering work. The opt-in
`tests/e2e/test_live_stage_web_compact_report.py` uses a real stage provider,
owned fixture records, a disabled one-shot job and rendered Web evidence; it never
reads personal accounts or creates calendar events.

`test_source_group_repair.py` and `test_source_group_repair_boundary.py` cover
same-capture title/terms grouping, missing-title repair, foreign-language requests,
independent snapshot protection, retained limitations and failed-repair partial
publication through real chat/persistence and Telegram HTTP delivery. The opt-in
`test_live_stage_web_source_groups.py` checks a rental comparison and an adjacent
product lookup against disposable pages from `fixtures/source_group_server.py`.
It asserts rendered Web outcomes and backing selection/review evidence without
performing real reservations or purchases.

`test_reviewed_presentation_correction.py` rejects incomplete, malformed or
unbound replacement answers. `test_reviewed_presentation_correction_boundary.py`
drives the real chat graph through FastAPI, rendered Web UI/history and Telegram
SDK HTTP bytes with Japanese and Spanish requests. It checks compact output,
retained amounts and uncertainty, durable completion state, one source read and
one review with no producer retry. External model/source boundaries are faked;
routing, review validation, persistence and rendering remain real.
The source-group boundary also exercises a reviewer-authored correction after
literal selection repair, and the artifact-summary boundary covers a
delivery-owned report correction without a separate composer or new file.
Unknown evidence cannot certify a rewrite; amended requests, evidence, output
and changed artifact bytes invalidate the cached result.
The source-group boundary additionally scripts the empty-prerequisite contract
seen in live acceptance, corrects only its evidence path, and verifies one source
capture, two planning attempts and one final review. Both the schema and runtime
parser must reject the invalid contract.

`test_reviewed_shopping_presentation.py` covers catalog-shaped complete and
partial comparisons in Japanese and Spanish across Web API, rendered Web/history
and Telegram SDK HTTP delivery. It checks all three options and prices, one
recommendation, omitted optional catalogs, visible genuine limitations and the
saved emoji policy, with one source read and one review. Its provider validates
the emitted review schema and proves legacy prose is not an allowed report
finding format there. `test_review_report_layout_contract.py` checks citation
rules still apply, legacy compatibility never changes completion, draft detail
does not override preferences, and quoted assistant content retains the linked
user request. Ordinary turns gain no formatting model call.

`test_live_stage_web_review_layout.py` is an opt-in publication replay of an
actual model review of retained production evidence. Run it with
`NULLION_LIVE_STAGE_E2E=1` and `NULLION_LIVE_REVIEW_LAYOUT_REPLAY=1` through
`scripts/live-stage-web-docker.sh`. It checks the rendered answer and durable
history after reload without repeating source research. Its historical source
snapshot contains omitted observations, so it verifies a correctly presented
partial answer; it does not certify a fresh, complete shopping comparison.

The source-answer recovery boundary also reproduces a rejected role selection
followed by a valid long quoted field, and two invalid composition attempts
followed by an independently grounded shared report. It checks one source read, one outcome review for cosmetic recovery, concise
final facts, actual saved fulfillment and Telegram HTTP delivery. A genuinely
unmet callable fact can still use the existing bounded execution recovery and
subsequent review. The profile/clarification boundary includes a selected partial
execution without saved clarification metadata: an acknowledgement must recover
the actual lookup rather than closing the unfinished goal. Cancellation and
explanation-only cases retain zero lookup calls.

`test_live_stage_web_source_recovery.py` publishes the bounded real-model
retained-source composition result through stage's actual history API and Web
UI, then reloads it. The input is an explicitly fictional three-product fixture;
this verifies composition and publication, not current merchant prices or a
fresh browser search. It uses `NULLION_LIVE_REVIEW_LAYOUT_REPLAY=1` with the stage
Web wrapper and requires a successful saved real-model result first.

`test_native_report_correction_delivery.py` covers a native record report whose
reviewer authors a corrected report with `source_presentation_valid=null` and no
browser selection. The real scheduler, graph, SQLite and Telegram boundary must
publish that correction once with one enclosing title. Japanese and Spanish
requests cover compact and detailed preferences, requested provenance, material
uncertainty and aggregate calendar outcomes. The ordinary Web/Telegram report
correction boundary also uses null when there is no source selection. Unit tests
retain rejection of an explicit false verdict and an unverified real selection.

`test_live_stage_web_native_report.py` replays the controlled native-record
review result through stage Web history and rendering, checking one delivery
title, compact text and mandatory facts after reload. Its inputs are fictional
retained records; it performs no mailbox search, calendar write or fresh
production job execution.

Source-answer recovery boundary tests also assert that reviewer-rewritten full
and partial answers retain both selected public source URLs through the real
chat route, SQLite history and Telegram upload boundary, excluding unrelated
inventory URLs. Shopping report fixtures use typed source URLs rather than
embedding links in labels. Unit coverage checks bound-reference isolation,
deduplication, compact/detailed output, unsafe targets and cosmetic fallback.

`test_partial_review_and_inspection_delivery.py` covers a real FastAPI chat and
SQLite history retaining a reviewed partial answer instead of starting an
adapter retry that would time out. The same answer reaches the Telegram upload
boundary with its link and incomplete status intact. Japanese/Spanish requests
exercise the request-bound receipt policy. A second case requests eight retained
inspection calls in one response, verifies the shared view budget, then retrieves
an exact field outside that view without another source read or data loss.

Native report delivery also covers the `triage_compact_fixture` failure shape:
six obligations/notices, repeated metadata and routine calendar results, two
uncertain outcomes, a conflicting amount and an unconfirmed event end time.
Compact output retains the decision facts and source links once; detailed
output retains the optional notification wording.

Scheduled review outages are covered by `test_scheduled_review_outage.py`: the
real runner, SQLite runtime, and Telegram document boundary preserve HTML bytes
and an unconfirmed verdict without repeating completed reads or regrading an
unchanged report. `test_multi_source_review_budget_workflow.py` checks that a
corrected caption and its separately verified attachment reach the Web API once,
including under source-capture budget pressure. `test_tracker_status_delivery.py`
checks the persisted final-review cause, independent delivery receipt, and one
Doctor notice through the real status worker and Web history route. These cases
use deterministic providers at the external boundary and consume no model quota.

Outcome-assistance coverage in `test_review_helper_delivery.py` exercises real runner/SQLite, FastAPI history, Telegram SDK HTTP delivery, delegated context publication and grouped artifact acknowledgements. Timeout, invalid-editor and cosmetic-rejection fixtures preserve the usable primary answer without review warnings or repeated reads. A slow editor exercises the delegated outer deadline. Scheduled report coverage feeds its actual acknowledged result into tracker health to prove helper unavailability cannot create a twelve-run Doctor failure streak. Separate cases retain permission, required-action and corrupt-file failures, plus successful grounded corrections and scoped execution-improvement plans. These controller fixtures do not measure model judgment accuracy.


### Partial report attachment and inventory carryover regressions

`test_scheduled_budget_native_attachment.py` covers Japanese and Spanish saved tasks paused for budget, approved through FastAPI and resumed from SQLite. It verifies one actual Telegram upload of the selected bytes when the external reviewer times out across HTML, PDF, spreadsheet, CSV, image, document, archive and audio files, including missing requested embedded images in supported report formats. Incomplete content remains partial; no replacement file or completion claim is manufactured. `test_native_artifact_preparation_versions.py` additionally rejects changed bytes and keeps early completion stricter than terminal partial delivery.

`test_cron_inventory_latency_workflow.py` exercises real chat and inventory routes, SQLite and the independent review packet. All observed task identities, scheduler-kind scope and source-page coverage must survive the detailed-row sample. The live Web inventory case checks visible names and its existing latency threshold; it must not pass by silently omitting later tasks or increasing the deadline.

`test_background_status_chat_visibility.py` starts the Slack and Discord adapters with real SQLite session preferences and the shared card renderer. Only the external SDK endpoints are captured. A chat setting opposite to the global setting must control activity details, preserve the planner and deliver the terminal answer. `test_verbose_final_delivery.py` covers the corresponding real Web UI and reload boundary.

Saved-report live assertions compare the caption separately from `MEDIA:` transport directives and bind the selected native paths to history attachment descriptors and actual download bytes. HTML may have both an Open link and a Download link; each selected download must appear once. `test_live_completed_report_receipt_matches_caption_and_download_bytes` can inspect an already completed test-owned report without repeating model calls, research or delivery.

Portable tracker freshness tests omit `source_refresh` and serve static HTML without an automatic update loop. Newly published records must be observed on the next poll and delivered once in the real Web UI. Explicit `reuse` fixtures stream updates into their existing DOM and must retain that behavior. Unit cases also cover older saved recipes without a refresh-policy field. The opt-in new-article acceptance can use `NULLION_LIVE_OBSERVATION_CDP_URL` to verify nonce-owned physical browser targets across polls, independently of background JavaScript timer throttling.

`test_live_tracker_multiple_sources.py` prepares two independent streaming pages through native tools, persists the plan in SQLite and checks the real worker and Web alert. Both pages must remain correct after multiple polls, without reopening either page, before a combined condition can alert once. The opt-in live two-source contract first publishes on only the second feed and requires no false match, then publishes on both and verifies one visible notice and stable source targets.

The controlled news fixture uses server-sent events for explicit streaming-page
reuse. Its source pushes are covered by an actual HTTP stream test; live tests
must not assume that background browser timers keep a page current. Static-page
freshness coverage uses the normal navigation refresh mode instead.

`test_live_tracker_native_preflight.py` drives the actual Web chat route, native browser reads, prepared argument bindings, tracker admission and SQLite review records. A mechanical alias/default suggestion must not block valid creation or repeat producer reads. An explicit constraint must instead preserve the numbered question and create no tracker. `test_preflight_step_scope_boundary.py` also verifies that native mechanical authority cannot bypass an approval-required tool. Shared completion tests bind `awaiting_input` to the exact current question and invalidate it when that state changes.

`test_named_existing_file_delivery.py` checks real chat/API downloads after
editing an existing file outside managed artifact storage. The original edit,
one exported filename and exact download bytes must agree. It includes Unicode,
spaces and command entry. `test_named_artifact_exports.py` covers independent
named versions across eight formats, complete attachment paths and delegated
relative HTML/image links while excluding unselected sibling files. Live
transformation chains stop at an unfulfilled intermediate artifact before
spending provider calls on later transformations.

`test_native_worksheet_counts.py` exercises a real chat request, native count
failure/correction, multi-sheet XLSX bytes and the saved execution receipt.
Individual worksheet counts coexist with a strict workbook-total count.
`test_retained_followup_boundary.py` covers completed-parent read reuse and a
subsequent fresh read through chat routing and SQLite. Its source is a controlled
HTTP boundary; Nullion's routing, persistence, scope and review remain real.
It sends follow-ups without repeating the source URL, exercises generic scope
recovery, and verifies that only an explicit fresh check reads the source again.

HTTP and WebSocket commands use the ordinary shared result payload. Native
attachment tests exercise both entry forms on both transports. Static-command
latency checks require zero model calls and multiple bounded samples, while the
plain-chat latency contract still simulates a slow optional selector. The live
command case verifies the complete Unicode filename, CSV records, one visible
download and persisted attachment metadata across reload.

Inline SVG PDF coverage: `test_inline_svg_pdf_workflow.py` drives the real chat
API, runtime store, scope expansion, native PDF creation, and download route
with a scripted external model. English and Japanese requests must each deliver
one PDF in one creation call with the exact four-color illustration pixels.
Adjacent cases cover document-level CSS, explicit SVG namespaces, and rejected
external dependencies without network fetches or fabricated replacements.
`test_inline_svg_media.py` covers exact Unicode/multiline source spans and
empty, malformed, or definition-only markup. Existing SVG-file tests continue
to exercise PDF and Office downloads.

The opt-in captured-package Web replay in
`test_live_stage_web_saved_report_rendering.py` verifies previously generated
files and renders each PDF page with the test-only PDFium dependency. It makes
no new model request and keeps the original latency verdict unchanged. Optional
review outages are recorded separately from actual download/content results.
The original saved flyer HTML is a checked-in fixture, so the exact inline
illustration rejection remains reproducible without another provider request.
The same E2E module also covers English and Japanese HTML creation through the
real API and download route, comparing unchanged bytes and the browser-rendered
illustration pixels. HTML creation and final fulfillment share structural SVG
checks with the PDF renderer's external-resource policy.
Rendered PDF regression coverage also retains a responsive poster fixture with
an additional image. It checks the one-page result's bounded text and actual
rasterized color regions, including the authored dark background behind white
text. This catches clipping and contrast failures that raw PDF text extraction
misses. A separate two-PDF chat case verifies an illustrated poster plus a plain
two-page memo, with no forced image-generation retry for the memo.

SVG artifact cases cover inline markup and base64/percent-encoded image data
URIs, Unicode, local XLink definitions, and HTML character references. Every
supported form is checked through actual API/download bytes and rendered image
pixels. Malformed, empty, and externally dependent SVG documents must fail
without external fetches. Failure diagnostics contain counts instead of source
HTML. Shared activity tests cover native file/PDF errors, connector output,
scheduler metadata, and non-English private payloads. The Web verbose E2E checks
that private file paths/names stay out of the visible and reloaded response,
while the successful read remains in the persistent execution record.

Live provider evidence checks run synchronously before/after paid cases and in a
background read while a Web request is active. The same authenticated GET may
retry a transient transport outage for up to 30 seconds; chat submissions are
never retried. In-flight polling keeps browser response capture responsive and
propagates an exhausted transport error to owned-request cleanup. Boundary tests
retain terminal quota/auth stopping and a nine-second outage after the first
request, proving a recovered evidence read cannot start a second paid case.
Pending evidence reads are drained at case boundaries before another paid case
can start. A delayed transport or quota result cannot leak into the next case or
permit it to run without the synchronous provider preflight.

`test_optional_artifact_media_recovery.py` exercises the real chat API, persistent
runtime, native creators, artifact selection and download. English/Japanese cases
repair failed optional artwork in PDF/DOCX/PPTX/XLSX without another generation
loop. An archive containing valid and corrupt PNG files plus a text file must
import every inventory row literally and deliver only the requested workbook.
Unit cases retain explicitly required visual checks, reject provider-payload
provenance spoofing, and preserve native requirement metadata through review.
Delegated coverage runs the actual Deep Agents graph with native spreadsheet
creation, then passes the result through aggregation and the Telegram upload
boundary. Only external model/SDK boundaries are scripted. It verifies one
workbook's uploaded bytes and successful outcome after an optional image failure,
including English/Japanese requests and preserved runtime-owned media metadata.
Live archive acceptance compares the actual downloaded workbook row count with the
native extraction receipt, excludes metadata sidecars, and checks follow-up
CSV extension totals against that same archive rather than a stale inventory.

`test_archive_followup_source_history.py` uses real API turns, SQLite history,
native ZIP extraction, history compaction, and a downloaded CSV. It asserts that
111 source files retain their exact extension totals despite unrelated workspace
files, without rescanning or reextracting them. Both English and Japanese follow-ups
exercise the same source contract, with request timings and bounded call counts.

Real-provider latency acceptance records modest overruns as advisories when all
functional checks pass. The advisory window is at most 25 percent and at most
30 seconds above a target. Larger overruns are recorded as attention items for context-based assessment;
hangs, response timeouts, missing results and explicit deadline violations still
fail. Deterministic local performance checks remain strict. The same
assessment is shared by the catalogue, specialized probes and Web matrix.
Captured successful functional evidence may be reassessed without new provider
calls when only this acceptance policy changed. Original failure records remain.

Inventory acceptance verifies complete numbered native identities. A concise
whole-field prefix is permitted only when it identifies that exact row uniquely;
missing, duplicated, swapped or ambiguous identities fail. Historical tracker
render replays bind the saved turn by id and compare visible rendered content,
not Markdown source text against an HTML element. Broad account follow-ups may
complete a date-window clarification, but still require actual account reads
and must not continue the prior browser task.
Timing checks run after functional and artifact-byte verification so a diagnostic
timing target cannot prevent inspecting the actual delivered files.

Action-receipt presentation coverage verifies actual tracker changes, subsequent
polling, persisted condition revisions, and exact approved response copy through
the real chat API. English-style authored copy is exercised with Japanese and
Spanish requests. Review outages retain native acknowledgements; changed native
facts, requests, reply text, or invocation identities invalidate presentation
annotations. Serialization and subsequent requirement recording preserve the
runtime-owned annotation, while provider-payload lookalikes cannot supply it.

`test_restored_reply_identity.py` seeds replies through the real history route and
checks active and archived history in a running Web UI in Japanese and Spanish.
Reloaded replies retain their native runtime turn identity and completed-turn
registration, so terminal replay can reuse the visible reply. Archived history
uses the same restore renderer as the active conversation.

`test_browser_worker_terminal_handoff.py` drives real chat, native browser-tool
invocation, a child process, HTTP source capture and SQLite. Its external SDK fake
refuses asynchronous shutdown after the actual worker publishes validated source
evidence. Japanese and Spanish requests must receive that evidence once, retain
partial/audit flags and leave no worker or scratch directory. Separate SDK tests
run the installed Browser Use version against Chromium for normal step exhaustion
and interrupted reads; deadline units cover cancellation-resistant operations and
owner cancellation without paid model calls.

The SDK origin-following E2E covers a normal browser, an existing browser with 55
untouched pages, and an unresponsive CDP network endpoint. It verifies native
source observations in the fallback browser without changing the configured
endpoint. The terminal-handoff chat E2E covers both successful partial reads and
SDK exceptions while the SDK refuses shutdown. Confirmation-choice stage checks
cover English, Spanish and Japanese without creating a task before consent.

Review scope recovery includes Spanish and Japanese real Web-route/SQLite cases
where the model initially expands the plan to unrelated contextual metadata.
The independent typed scope mismatch must replace that plan, remove the extra
requirement and finish from the original source evidence without another read.
The live artist-research replay checks the same requested outcome against real
provider/source evidence. Scheduled coverage recovery accepts either an inner
producer repair or a scheduler continuation, but requires the actual interruption,
all required source reads, a complete native review and one final delivery.

Browser report publication tests include real child processes that hang during
shutdown or exit with code7 immediately after publishing success/error reports.
Japanese and Spanish real chat-route/SQLite cases require retained observed values
or the exact worker error, one browser invocation and cleaned-up processes.
Exited-worker unit cases additionally prove cancellation wins over late publication
and that exited Windows PIDs are never signalled.

The public-research follow-up scope guard does not require the optional helper
to finish. It requires the final native captured source record, its visible direct
link, no email/calendar tool attempts and no recorded material failure. A helper
outage remains an explicit attention result and does not certify request completion.
A saved-conversation Web replay verifies that exact delivered reply, source links
and turn identity across reload without repeating paid research. The price/stock
and legal-research completion checks retain their stricter outcome assertions.


The SDK blocker evidence boundary exercises a real disabled choice in Chromium,
then checks its captured quote/control state in the Web API and SQLite history.
Japanese and Spanish variants must retain the exact requested date without
reporting inventory success. Rental and flight stage contracts require a completed
request-owned review; price-related text alone cannot pass an unfinished lookup.
Read timeout contracts exercise the actual Web route/store and a healthy second
source after the first invocation times out; recovery scope receipt checks also
cover invocation-scoped versus tool-wide failures.


Terminal package regressions verify exact HTTP download bytes for four distinct
requested formats after an unexecuted duplicate proposal and a reviewer outage.
Same-format drafts and prior excluded/revised candidates retain their selection
requirements. The children’s live package accepts permitted native producer
fallbacks and additionally inspects actual Word page-number fields and embedded
media. Browser blocker tests also exercise an over-expanded either/or plan being
replaced from typed scope feedback without repeating the browser lookup.

Shared secret-link delivery coverage compares WebSocket text, the HTTP result,
and durable chat history. Secret query values must be masked consistently while
Markdown link delimiters and non-secret query parameters remain intact. Separate
punctuation cases ensure this URL handling cannot expose part of an ordinary
unquoted password. Use the same presentation policy before platform rendering;
do not repair only the history or Web adapter.

Keep real browser acceptance serialized with memory-intensive local suites.
A Chromium crash accompanied by a Docker OOM event is an infrastructure failure,
not a passing workflow or evidence of a provider outage. Preserve the failed
run and replay it after removing the resource contention. Cleanup may close
owned test contexts and tasks, never unrelated or unowned browser tabs.

Recovery-focus contracts retain each selected reader's declared, callable READ
setup tools. A narrowed review must not strand the agent on one page. Real
Web/store cases cover opening another source after a focused read request.

Native tracker replay covers a results page whose heading ID changes on each
load. Adapter metadata identifies readiness checks already enforced inside the
read; replay validates these prerequisites without running stale duplicate
waits ahead of the repairable source handler. One bounded repair uses the real
page snapshot, retains the source/projection/heading constraint, and commits
the replacement with a correctly bound comparison snapshot. The next real
value change must produce a notice while the tracker remains active.

A rejected overwrite of a current native artifact package enters the existing
selection and outcome-review path once. This does not certify the files from
format presence or suppress missing content/actions. Boundary tests require
unchanged download bytes and one rejected proposal, including review outages.
Complex package probes allow a twelve-minute observation window and still
record latency advice; this does not change product execution budgets.

Shared-media contracts generate distinct native DOCX and PPTX files from one
downloaded image, then download both through the real Web API and inspect the
embedded image bytes. Reusing supporting media must not count as reusing a
deliverable; reusing the same primary file still fails. Adapter-declared
`source_record_collections[].observed_url_fields` identify actual fetched URLs,
including binary downloads. Rendered crops use the native screenshot's page
receipt, never a requested URL or a linked image as proof of observation.
The DOCX case also requests native page numbering and checks the real footer
PAGE field in the downloaded ZIP alongside its media. The review packet must
contain the independently inspected field count. Unit variants cover default
and disabled numbering, body text that merely says "Page 1", and rejected
unsupported formats or non-boolean arguments.

PDF content checks use Poppler's logical text extraction when available in the
Docker runner, while retaining structural, image and nonempty-content checks.
Synthetic glyph-tracking cases cover English and Spanish text plus fused-word
negative controls: styling must not invent spaces, and normalization must not
accept missing word boundaries. The E2E image installs `poppler-utils`.
For a validation-only correction, an explicit retained-report replay may reload
the original real stage conversation and recheck every original contract and
download hash without issuing model or source requests. Check the actual rendered
artifact link targets and their served bytes; the desktop Save control uses a
local-save route, not a Playwright browser-download event. Preserve the original
failed run and record this separately; it is not a fresh research run.

Review correction schema tests compare accepted and rejected source-presentation
verdicts against the runtime parser with and without a source selection. The
source-answer recovery E2E validates the actual review response schema while
repairing an invalid original role, then checks the corrected Web and Telegram
output and stored outcome without repeating source work. Invalid or incomplete
corrections retain their existing rejection and partial-publication behavior.

`test_scheduler_pending_input_contract.py` exercises the real Web route and
SQLite scheduler: the full request waits on its actual confirmation, stores no
job before the user chooses, creates one paused job after a numeric confirmation,
and creates nothing or opens no approval when the user chooses to decline.
Japanese and Spanish requests share the typed path. Unit cases distinguish a
missing user-controlled input from an unsupported source fact or action claim.

Readiness-repair budget tests use multiple source pages in one session through
the Web API and SQLite. Different-source failures must permit the healthy
source read; repeated failures on the same source must still stop. Both cases
include Japanese and Spanish requests without keyword routing.

The live email-to-carrier contract completes a runtime-owned numbered source selection when exactly one offered source exposes `email_read`, then requires the actual email read and browser work in the selected follow-up. A source-choice question alone cannot pass this boundary; no order identifier or carrier result is fabricated. Stage image-generation acceptance uses a configured provider and verifies the delivered binary bytes; a disabled or unavailable provider remains an explicit environment blocker.

`test_media_settings_live_registry.py` starts with image generation disabled, saves configuration through the real FastAPI route, generates and downloads actual PNG bytes through chat, changes the generator, and disables it again. Japanese/Spanish requests share the same registry path. Independent messaging registries exercise the shared Telegram/Slack/Discord settings reload. Only the external image generator is a local command fixture; routing, registration, persistence, file validation and delivery remain real.

`test_image_provider_failure_contract.py` covers configured commands and models rejecting generation with HTTP 401, 402, 429 and 503. The real tool registry must retain failure status and invocation scope, preserve typed retryability, and create no SVG/PNG substitute. The settings/chat boundary also rejects a configured command after successful generation and asserts that no placeholder is delivered or certified complete.

`test_artifact_provider_failure_presentation.py` verifies typed billing failures across image, document, PDF, spreadsheet, presentation, HTML and file producers, including stored mapping receipts. The shared delivery boundary retains the current HTTP failure, excludes private provider payloads and keeps missing-attachment status. The live `NULLION_LIVE_IMAGE_REJECTION_STATUS` check requires the observed provider status in the visible Web answer, as well as failed tool evidence and no placeholder artifact.

The disposable public-source CDP fixture defaults to headed Chromium on Xvfb inside Docker. `--headless` is an explicit comparison option. Web UI tests may still use headless Chromium; that does not establish public-site accessibility in the normal browser mode. A headless-only page error must be compared with headed mode before declaring the source inaccessible. `test_live_cdp_fixture_mode.py` checks the effective browser mode and rendered PNG/DOM in both modes. No host profile, production browser setting or private session is changed by this fixture.

The real CNN setup observer allows 240 seconds for source inspection and a genuine creation-confirmation exchange. The earlier 120-second observer could cancel a valid confirmation before it reached the test. This does not change production timeouts, tracker duration, positive-match evidence, duplicate-alert assertions, or cleanup.
