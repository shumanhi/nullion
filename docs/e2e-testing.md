# Testing

The primary agent owns the answer; there is no independent AI quality gate.
`test_producer_only_completion.py` exercises Web API, Telegram upload bytes,
scheduled runner delivery, approval resume and delegated execution with real
routing, SQLite, native tools and transport code. Only external model/network
boundaries are scripted. The provider rejects any quality-grader invocation.
Japanese and Spanish requests prove this does not depend on English triggers.
Missing files still fail native delivery checks, while usable partial source
answers remain visible. `test_producer_completion.py` covers legacy paused-state
migration without changing historical data or dropping native media receipts.
Live stage Web UI and real inbound Telegram validation remain separate acceptance
steps; deterministic Docker coverage is not a claim that either was exercised.

Tests live alongside the implementation in `tests/`, with real product workflow
coverage in `tests/e2e/`. A fresh clone contains the complete released suite.
No second checkout, private-repository key, or live-provider credentials are
required for ordinary pull requests, including contributions from forks.

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

Focused unit runs still enforce overall coverage. For a narrow Docker-only check
while implementing, use `NULLION_TEST_COVERAGE=0 ./scripts/e2e-docker.sh tests/test_producer_completion.py`;
run the full unit command with coverage before release. Browser, installer and E2E
checks run in containers or CI runners. Live-stage probes are opt-in and
require separately authorized services and credentials.

Browser history checks wait for the typed terminal-reply save acknowledgement
before asserting persistence or reloading the page. Visible streamed text is an
intermediate state, so matching text alone is not a durable completion receipt.
Keep reload assertions and the stored reply's identity, content and count checks.

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

`test_scheduled_constraint_evidence.py` runs a native batch read, calendar
check and report through the scheduled runner, SQLite and Telegram. The primary
agent authors the answer from its tool results; no independent citation grader
or wording-repair verdict is required. Japanese and Spanish requests use the
same path.

`test_telegram_pending_send_and_cron_recovery.py` uses the real messaging runner
and SQLite store with delayed Telegram SDK acknowledgements. English, Japanese,
and Spanish replies must leave one visible message and a durable delivery receipt;
failed final edits withdraw the draft before fallback delivery. The scheduler
case kills a real isolated worker, restarts the app, checks that its dead-owned
lease is reclaimed, and verifies one interruption notice through Web history and
status APIs. These are container boundary tests, not native Telegram app or live
stage acceptance. Live stage replays remain separately opt-in.

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

Primary-agent source selection uses the source receipts and tool scope it already owns. There is no independent search-coverage grader, correction loop or completion receipt requirement. Tests for a fabricated primary answer being repaired by a reviewer are retired. Keep native browser quote/schema validation, source ownership, read-only permissions, file versions, approval resumptions and actual delivery checks; their failure receipts remain visible without an AI grade.

`tests/test_bounded_comparison.py` and `tests/e2e/test_bounded_comparison_boundary.py` distinguish a scoped recommendation with incidental pending listings from an unresolved ranking contender. The graph/SQLite, Web route and Telegram HTTP delivery tests prove an observed recommendation can finish with thirteen unverified discoveries retained, a credible contender receives a targeted source read, scope-only wording repair performs no further source reads, and blocked contenders keep useful facts with a specific limitation. Platform tests verify one delivered reply and persisted history. Japanese/Spanish prompts share the same typed path. Explicit exhaustive comparisons still inspect the discovered sources; unit cases also cover missing selected-source proof, requested subjects, discovery truncation, incorrect filters and legacy negative/comprehensive coverage. Provider calls and external source adapters are deterministic; no live stage/provider calls are used by these tests.

Controlled coverage pages are provided under `tests/fixtures/search_coverage/`: the discovery page links to an empty primary catalog and an independently stocked format-specific catalog. When live stage is authorized, serve those pages through the permitted test-fixture origin and generate the live case JSON with that origin, the requested fixture constraints, alternate URL and literal listing ID `COVERAGE-IMAX-007`. Do not treat these pages as real commercial inventory.

Scheduled chat presentation uses the same guidance in the original producer and interruption recovery. `tests/test_scheduled_chat_presentation.py` checks that requested provenance and detailed formats remain intact. `tests/e2e/test_scheduled_chat_presentation_delivery.py` exercises the real scheduled runner, SQLite and Telegram delivery with Japanese/Spanish requests and a provider interruption: compact source metadata, reconciliation uncertainty, one delivery, and no extra presentation model pass. The external model is scripted; these tests establish instruction propagation and delivery preservation, not subjective real-model writing quality. Live stage acceptance remains a separate opt-in check.

`tests/e2e/test_saved_job_configuration_repairs.py` exercises actual job creation and scoped updates through FastAPI/SQLite, using Japanese and Spanish labels. It checks current-evidence task updates, disabling an expired annual alert without deleting its history, and local market-alert next-fire times in winter and summer. Unrelated settings and jobs must remain unchanged. This is configuration persistence and scheduling coverage; it does not execute real account searches or prove report writing quality.

`tests/e2e/test_scheduled_review_outage.py` also reproduces an evidence inspection followed by a provider timeout. Its model-boundary clock consumes the passed timeout and requires subsequent attempts to stay within the original total allowance. The real scheduled runner, SQLite, shared report renderer and Telegram upload path must retain one concise caption and the exact HTML bytes, without a second research run or a failed-task notice. Unit cases in `test_compact_report_contract.py` cover report envelopes with attachment descriptors across HTML, PDF, Office, image, data and archive extensions, compact/detailed preference preservation, unknown trailing text, and empty media markers. The model and network remain simulated in these boundary tests; a separately recorded real-provider writing replay checks the supplied report's summary quality without repeating live research or delivery.

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

Structured source coverage regressions also live in `tests/test_declared_source_coverage.py` and the structured-adapter cases in `tests/e2e/test_bounded_comparison_boundary.py`. They cover nested declared records with no text, parent-bound evidence aliases, empty/failed records, identity-only compaction, credentials in URLs and linked-target separation. The graph and real Web route must retain verified prices and actionable limitations. `tests/e2e/test_live_stage_bounded_comparison.py` uses the disposable `live_source_server.py` fixture and a real stage Web/provider request to exercise a scoped recommendation. `live_cdp_service.py` supplies an isolated container browser at the configured stage-only endpoint when no external stage browser is running; it never mounts a host browser profile.

The live stage audit runs sequentially and stops on the first failed case. Successful plain/weather preambles with identical arguments are reused within one runner process for at most 30 minutes; affected workflow requests are always executed. A new runner or candidate revision starts fresh baseline measurements. Keep successful case node IDs when resuming an interrupted audit so that provider-funded cases are not repeated unnecessarily. Saved-task replay datasets may be explicitly bounded for the audit; record that scope rather than representing them as full mailbox or full research replays.

### Shared presentation and live-condition amendments

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

Advertised artifact download URLs must return the actual bytes. Live checks fail
on HTTP errors even if a matching local file exists. Exact existing-file delivery
compares the downloaded SHA-256 with the source fixture; inspection tools alone
are not failures and do not prove delivery.

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

`tests/e2e/test_generated_artifact_inspection_delivery.py` also exercises abbreviated image, PDF, and CSV inspection paths across three languages. It requires recovery to an exact retained path, one final file set with verified download bytes, and no spurious access approval. Unit coverage in `tests/test_artifact_read_references.py` checks additional formats and ensures a deliberate different-path retry retains normal access policy.

The live flight correction contract permits a numbered clarification when relative dates are ambiguous. It resolves that clarification and requires a fulfilled current turn and one downloadable PDF; keeping the original comparison must preserve its exact bytes without repeating research or generation. Its longer HTTP observer budget does not relax the separate latency assertion or production limits, and a disconnected observer stops only its owned conversation. Status GETs and artifact downloads use the same explicitly configured authority header; transient read retries never replay a chat submission.

`tests/e2e/test_scheduler_inventory_family_discovery.py` starts with an empty live-tracker inventory while the named record exists as a cron or reminder. Multilingual API and rendered Web tests require the actual related inventory read, a fulfilled status answer, and unchanged saved state. Representative plain turns measure average/max latency and require one provider call each. `tests/test_scheduler_inventory_scope.py` covers metadata-based visibility, read-only scope retention, later explicitly planned mutations, and reminder workspace boundaries.

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

`test_live_stage_web_source_recovery.py` publishes the bounded real-model
retained-source composition result through stage's actual history API and Web
UI, then reloads it. The input is an explicitly fictional three-product fixture;
this verifies composition and publication, not current merchant prices or a
fresh browser search. It uses `NULLION_LIVE_REVIEW_LAYOUT_REPLAY=1` with the stage
Web wrapper and requires a successful saved real-model result first.

Native report delivery also covers the `triage_compact_fixture` failure shape:
six obligations/notices, repeated metadata and routine calendar results, two
uncertain outcomes, a conflicting amount and an unconfirmed event end time.
Compact output retains the decision facts and source links once; detailed
output retains the optional notification wording.


### Partial report attachment and inventory carryover regressions

`test_background_status_chat_visibility.py` starts the Slack and Discord adapters with real SQLite session preferences and the shared card renderer. Only the external SDK endpoints are captured. A chat setting opposite to the global setting must control activity details, preserve the planner and deliver the terminal answer. `test_verbose_final_delivery.py` covers the corresponding real Web UI and reload boundary.

Saved-report live assertions compare the caption separately from `MEDIA:` transport directives and bind the selected native paths to history attachment descriptors and actual download bytes. HTML may have both an Open link and a Download link; each selected download must appear once. `test_live_completed_report_receipt_matches_caption_and_download_bytes` can inspect an already completed test-owned report without repeating model calls, research or delivery.

Portable tracker freshness tests omit `source_refresh` and serve static HTML without an automatic update loop. Newly published records must be observed on the next poll and delivered once in the real Web UI. Explicit `reuse` fixtures stream updates into their existing DOM and must retain that behavior. Unit cases also cover older saved recipes without a refresh-policy field. The opt-in new-article acceptance can use `NULLION_LIVE_OBSERVATION_CDP_URL` to verify nonce-owned physical browser targets across polls, independently of background JavaScript timer throttling.

`test_live_tracker_multiple_sources.py` prepares two independent streaming pages through native tools, persists the plan in SQLite and checks the real worker and Web alert. Both pages must remain correct after multiple polls, without reopening either page, before a combined condition can alert once. The opt-in live two-source contract first publishes on only the second feed and requires no false match, then publishes on both and verifies one visible notice and stable source targets.

The controlled news fixture uses server-sent events for explicit streaming-page
reuse. Its source pushes are covered by an actual HTTP stream test; live tests
must not assume that background browser timers keep a page current. Static-page
freshness coverage uses the normal navigation refresh mode instead.

`test_named_existing_file_delivery.py` checks real chat/API downloads after
editing an existing file outside managed artifact storage. The original edit,
one exported filename and exact download bytes must agree. It includes Unicode,
spaces and command entry. `test_named_artifact_exports.py` covers independent
named versions across eight formats, complete attachment paths and delegated
relative HTML/image links while excluding unselected sibling files. Live
transformation chains stop at an unfulfilled intermediate artifact before
spending provider calls on later transformations.

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

Native tracker replay covers a results page whose heading ID changes on each
load. Adapter metadata identifies readiness checks already enforced inside the
read; replay validates these prerequisites without running stale duplicate
waits ahead of the repairable source handler. One bounded repair uses the real
page snapshot, retains the source/projection/heading constraint, and commits
the replacement with a correctly bound comparison snapshot. The next real
value change must produce a notice while the tracker remains active.

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

Native approval and source-selection workflows retain numbered user choices and persistent pending state. Reviewer-invented clarification questions and review-driven repair windows are removed with the quality layer; the primary agent and existing typed task/approval state own those decisions.

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


Reviewer-only fixtures that deliberately supplied an incorrect primary answer
and expected a second AI to repair, grade or force source presentation are
retired with that feature. Native permission, cancellation, pending-context,
file version, compaction, formatting and transport tests remain. Existing
partial answers stay visible even when another connector needs reconnection;
native outcome state still records that blocker.

`test_live_stage_web_producer_completion.py` covers real stage Web PDF download
and native account-source consent with no independent quality-review events.
`test_live_stage_web_producer_generation.py` extends this to final DOCX selection,
a complete twenty-row XLSX, CSV generation during dashboard polling, an existing
file lookup follow-up, multilingual plain-turn latency, read-only scheduler
inventory and weather. These opt-in tests use the real configured provider and
browser UI, retain SQLite evidence, and inspect actual downloaded file bytes.
They do not establish Telegram app coverage; that requires the unlocked native
app and a real inbound user-message path.

`test_terminal_reply_history.py` drives the real Web composer with a scripted
external provider and native CSV writer. It holds startup history loading, sends
a real user message, checks one terminal attachment after caption revisions and
runtime synchronization, and reloads the page. Unit cases exercise both arrival
orders, concurrent independent store connections, separate message parts and
identical replies belonging to different requests or conversations.
