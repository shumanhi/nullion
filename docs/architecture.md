# Runtime architecture

## One assistant across every client

All clients use one execution pipeline and one presentation layer. A request
submitted through Web, Telegram, Slack, Discord or a future client must preserve
the same goal and follow the same execution policy when its normalized context,
permissions and runtime capabilities are equivalent.

Ingress adapters translate transport input into a common request contract. They
preserve reply identity, attachments, explicit actions and consent. The shared
runtime owns context and memory, reasoning, planning when needed, tool scope and
selection policy, execution, delegation, retries, recovery, approvals,
cancellation, task state and final outcome selection. A client cannot substitute
its own strategy prompt, execution loop or fallback agent. Required execution
policy belongs at the shared runtime boundary with typed evidence; advice in a
tool description alone does not enforce it.

The runtime produces one common message/result contract. One presentation layer
applies saved preferences; client output adapters render supported markup,
escape, split, upload and transport that result, and return delivery
acknowledgements. They cannot add requirements, discard usable findings, change
the requested work or rerun it. Capability differences are explicit normalized
facts with shared fallback/choice handling, rather than implicit client-specific
assistant policies.

Cross-client regression checks replay equivalent normalized requests and context
with controlled model/tool decisions through the actual affected boundaries.
They compare execution policy, action receipts, state changes and semantic
outcomes, then check transport rendering separately. Independent live model runs
may vary, but must obey the same policy. A successful Web replay alone does not
establish successful Telegram, Slack or Discord behavior or delivery.

Code Mode separates native execution from output selection. An invalid JSON
pointer in a program's output projection retains its successful value and a
bounded native evidence view; it cannot turn a completed read or write into a
new execution obligation. The producer may use that evidence immediately or
select saved values in the same interpreter without repeating native effects.
Observed output contracts describe exact retained field paths and types, not
invented aliases or guarantees about future responses. Ordinary plain turns do
not load these contracts or acquire another model decision.

Retained-evidence correction exposes copyable request-owned inspection requests
and bounded field indexes. Unknown references are never guessed or rebound to
another request. A failed pointer may name its valid parent for inspection, but
that parent does not certify the missing field. Programs can handle a failed
read locally, correct just that lookup and reuse successful windows. Existing
inspection budgets, consent, approval, uncertain-write and cancellation rules
remain authoritative across every client and continuation.

Code Mode defaults to a cumulative 4,000,000-byte native transfer allowance per
program and a 64,000,000-byte interpreter allocation budget. Operators can set
`NULLION_CODE_MODE_MAX_TRANSFER_BYTES` (64,000–64,000,000) and
`NULLION_CODE_MODE_MAX_MEMORY_BYTES` (8,000,000–512,000,000). Invalid settings use
the defaults. The budgets are independent; serialized byte size does not predict
the allocation cost of structured data. A request-owned session retains its
configured limits through continuations and approval resumes. Plain chat does
not load the interpreter or add a model call.
The serialized checkpoint allowance scales with the configured memory budget
(one quarter of it, with a 6,000,000-byte floor), so raising transfer capacity
does not leave the old fixed checkpoint allowance as the next failure point.

Large schema-declared source observations cross into the interpreter as an
explicitly partial view prioritizing accepted facts over optional page captures.
The host retains the exact receipt and an owned inspection reference. Opaque
outputs are never silently reinterpreted. If the remaining transfer allowance
cannot accommodate a result, native work stays completed and the program stops
before another effect. The final model view retains bounded source findings
even without a Python return value, with omission counts and exact references.

Native tool objectives reuse verified selected request lineage, amendments and
the owned question giving a numeric reply its meaning. Prior assistant text is
reference data, and cannot grant action authority or revive a replaced request.
An explicitly selected adaptive interaction continues after a successful native
preflight within the same tool invocation; a page read alone cannot finish the
requested form interaction. Read-only requests and pending source-access choices
retain their existing lightweight path. Browser workers also receive the runtime's accepted date/time context; model
arguments cannot replace that clock. Ingress, native policies and approval
boundaries continue to own source access and side effects.

Source compaction allocates space to accepted findings and their exact provenance
before navigation inventories, model usage counters and other optional diagnostics.
Nested schema-declared source values are projected before the Python return guard
can discard them. The return allowance is 64,000 bytes per typed source
bundle, capped at 256,000 bytes; it uses actual structured results, not prompt
keywords or an extra classifier. Native receipts, VM variables, approvals and
artifact bytes remain unchanged. The inference and stored-history boundaries use
the same projection. Multi-source Code Mode views receive 16,000 characters per
bundle, capped at 48,000 and bounded by the configured model-context ceiling.
Omitted rows remain explicitly counted and inspectable by their owned receipt.

A browser worker's validated `next_action=report` is a terminal handoff for both
answered and useful partial results. A partial handoff remains partial and does
not claim full user-request completion. Pending required controls and rejected
observations still prevent termination. The parent retains responsibility for
coverage and any further source work; the child does not need another model turn
just to issue a redundant `done` action.

## Primary-agent completion

Foreground persistence uses scoped durable writes where the storage contract is
defined. Reminder creation, edits, cancellation and failed-delivery rollback
commit the reminder and its one-off scheduled task together. A revision check
rejects concurrent changes before publishing success in memory. Native reminder
ownership markers also preserve deletion and delivery claims against older full
snapshots; other processes refresh just this inventory before listing or polling.
These commits do not wait for the Python/full-snapshot file locks used while
encoding history and making backups. SQLite still serializes the short writes.

The checkpoint audit also covers operator/button cancellation, display-setting
events, memory-use reinforcement, tracker-notice history and generic messaging
finalization. Display settings use a durable conversation write; secondary
history/usage work uses the existing scoped receipt and coalesced background
snapshot owner. Approval and permission transitions retain their synchronous
durability boundary. Pending approval notifications commit only the exact
approval and suspended continuation in one transaction before delivery; Web's
turn binding uses the same commit. Full history snapshots run through the
background coordinator. Failed or timed-out terminal operations retain captured
streams and explicit incomplete-output metadata without claiming success.
Inspection guidance prefers bounded reads and avoids cleanup mutations. On
POSIX, terminal commands own a separate process session; timeout/interruption
kills that session's pipeline descendants and bounds output draining.
Legacy primary state changes in missions, Doctor/admin
operations and approval workflows still use full checkpoints; backup, restore,
update/restart and shutdown snapshots also remain. This is not a claim that all
checkpoint call sites have been converted to incremental writes.

The primary agent owns reasoning, tool use and the final answer. Nullion does not
run a second AI requirements planner, action reviewer, completion grader or
post-execution quality reviewer. Delegated producers follow the same rule.
Grouped results can be composed into one answer, but the composer cannot grade
completed tasks or dispatch another execution cycle.

The normal producer uses the shared reasoning instructions to compare the
request with its evidence, correct its own plan and explain specific limitations.
Usable primary output and valid selected files remain deliverable when other
work is unfinished. No reviewer verdict is required to publish them.

Native runtime checks remain authoritative: principal and workspace ownership,
tool registration and schemas, permissions, consent, approvals, cancellation,
required action receipts, source selection, artifact integrity and final-version
selection, byte-bound content receipts, and actual delivery acknowledgements.
An unavailable local production method whose registered formats do not support
the selected output does not remain an action obligation after an alternative
producer has supplied the complete native file contract. Completion verifies
current producer-owned files, required formats and counts, embedded media and
task-frame obligations before retiring that method. Account actions, consent,
explicit tool obligations and compatible producers remain required. Shared
artifact guidance selects producers by format and asks the primary producer to
measure text, wrap it within panels, and inspect final rendered pixels for
clipping or overlap; metadata alone cannot establish visual quality.
An accepted tool call is not a delivered file. An incomplete producer receipt
remains incomplete; a successful upload does not certify its research. Native failures
retain the authored findings and add a specific missing-receipt notice, rather
than replacing the whole answer. A scheduled run that exhausts its allowance
with a required file still missing can request budget from that native contract,
without a reviewer inventing more work. Approval snapshots retain native media
requirements and recovery receipts while discarding obsolete review authority.

A terminal checkpoint binds the chosen answer to the request, tool evidence and
current file bytes so an adapter cannot restart finished work. Approval and
budget resumptions discard obsolete quality-review state while retaining native
receipts, tool evidence and selected files. Requested source-file delivery uses
validated structured scope and current file descriptors, not an extra AI plan.
Conditional alert matching remains the task's business logic; it is not a
quality-review pass and continues to distinguish a negative check from failure.

Web terminal history saves carry a typed terminal-reply kind and runtime turn
identity. Runtime import and browser persistence reconcile that one logical reply
atomically even if its rendered caption differs. Other status messages, parts,
follow-ups and conversations keep separate receipts. A real composer send enables
history persistence immediately rather than waiting for startup restoration.

The chat surfaces share routing, tool policy, persistence, and result
presentation. Their transport and rendering code differs, so a passing Web
workflow alone does not prove Telegram, Slack, or Discord delivery.

Telegram persists attachment input identity before dispatch admission and model
work. This receipt is not a completed turn or permission to replay an action.
A reply to a user's media message resolves that exact message's saved input,
or its embedded Telegram media reference when the earlier run was interrupted.
The quoted caption and image anchor context together; unrelated earlier topics
cannot replace the selected media. Bot output files retain the existing receipt
continuation path. Shared image handling derives supported image MIME types from
their bytes, including previously saved uploads with inaccurate transport hints.

Before a planned runtime restart, inspect foreground requests as well as cron
leases. An attachment input receipt without a terminal outcome can represent
work still in admission; an empty cron lease list alone does not prove the
service is idle. Interrupted input remains recoverable without silently replaying
side effects.

## Shared presentation contract

Web, Telegram, Slack and Discord retain their transport renderers for supported
markup, escaping, platform limits and uploads. They do not define independent
workflow style policies. The shared delivery preparation also accepts a typed
message directly. Tests change saved preferences through the real preferences
API, then check chat, scheduler, Doctor and live-tracker delivery, including
platform upload bytes and numeric choices.

Health-alert status updates and repair requests have separate receipts. Manual
Doctor/Sentinel resolution changes the alert record and explicitly carries
`repair_verified=false`. A requested restart stays unverified until health
evidence confirms recovery; guidance and unsupported commands leave the alert
unchanged. Repeated buttons on closed alerts cannot replay a restart.

The tray owns a lazy synchronous HTTP pool for its instance and serializes
overlapping health/status refreshes. Normal polling reuses a connection across
idle gaps: client expiry is 60 seconds and the standard web launcher uses a
75-second server keepalive timeout. Health failures back off up to a minute.
Connection establishment failures and ambiguous POST response failures remain
distinct; an uncertain restart is not replayed through the CLI.

Codex model variants share a lazy, thread-safe connection-pool owner, rather
than constructing an HTTP client per model call. Request headers and remaining
deadline stay request-local. Closing or discarding the final owner releases
its sockets; ending an incomplete stream still closes that response promptly.
Pool ownership introduces no model calls, routing decisions, or schedule changes.

## Request flow

An amendment received while a task is running updates the linked request.
Publication checks prevent an older response from being delivered after a newer
amendment. A status question does not authorize changing a saved job. A short
cancellation can use the recent verified job receipt; ambiguous candidates need
clarification rather than a guessed deletion.

Unquoted, vague follow-ups do not amend the only running request by default.
The shared admission graph uses its existing structured relationship decision
to ask which request is meant, with numbered running/recent request choices.
No cancellation token or publication gate is revoked until the user selects a
validated target. Choices retain the original message, survive restart, expire,
and cannot cross conversations, resets or explicit replies to another message.
Numeric selections resolve locally; plain turns add no relationship model call
or history scan. A selected choice is durably checkpointed even without tools.

Native approval pauses execution, not presentation of completed deliverables.
The shared chat handoff retains current-turn tool receipts and selected files
with the pending action. Telegram renders the existing confirmation card with
the message and attachment; Web renders the same draft and downloadable file.
Sending remains pending and the request is not recorded as fulfilled.
An exact source quote in the selected request ancestry can keep account tools
visible across clarification answers. Account writes can use this continuation
only when their registered metadata requires native approval. This restores the
draft review handoff; it grants no permission and does not enable unguarded
writes, unrelated-history sources or cancelled request ancestry.

Shell producers declare `terminal_exec.output_paths` for generated files. Exact
paths are validated and snapshotted before execution, then staged into the
principal's artifact storage only when created or changed. This avoids relying
on a bounded home-directory scan or parsing stdout as proof of file delivery.
Existing files, escaped roots, missing outputs and unchanged files are not
promoted by a declaration. Dedicated artifact producers remain preferred.

## Three kinds of background work

| Work | Runtime path | State |
| --- | --- | --- |
| Conversation task | Turn orchestration and optional scoped mini-agents | Task/frame/turn state |
| Recurring scheduled task | Cron scheduler and full saved task execution | Recurring job, run, delivery and Doctor records |
| Temporary live tracker | Prepared observation → typed/semantic comparison → bounded outbox | Probe, baseline, deadline, lease, outcome and delivery state |

Live trackers do not create synthetic cron jobs. The Web service owns their
worker; adapter-created trackers share the same instance database. Structured
polls do not call the agent model. Optional semantic evaluation has no tools
and requires evidence from the current observations. See
[Live tracking](live-tracking.md) for limits and failure behavior.
New agent/API trackers default to `stop_after_match=false`: each new matching
check gets its own durable outbox identity and the tracker stays active until
the deadline. Snapshot/structured conditions re-arm against the previous check,
while record-based monitors retain durable identity deduplication. Explicit and
legacy one-shot jobs retain their first-match behavior. Expiration renewal and
three-failure pauses remain independent of match delivery.

## Browser state

The terminal preparation phase says "Preparing your response" for text and file
results alike. A phase name inherited from artifact preparation does not prove
that this request produced a file. Actual producer operations may still show
file-specific progress from their registered metadata.

Foreground progress uses one request-local fact collector in the shared chat
pipeline. Only typed phase transitions, registered tool receipts and validated
native batch counts update it. It never parses prompts/source prose, generates
percentages, or claims that a completed read means the whole request is done.
The collector builds the existing `UserMessage` contract and uses its shared
presentation preferences. Web replaces its status area; messaging adapters edit
one delayed, coalesced notice. Stop updates when the result is ready and remove
the notice when delivery completes, keeping existing progress visible during uploads.
Web's HTTP transport polls only the current request's in-memory snapshot; it
does not resubmit work, load the dashboard or repeat execution. WebSocket
progress and these snapshots receive the same shared content. Verbose
tool logs remain optional. Fast replies need no extra notice, progress delivery
cannot block final delivery, and cancellation/supersession closes the publisher.
The initial working notice says: "Working on your request. Feel free to send
other tasks — I can handle multiple at once." Later updates replace it with
the current progress facts.
The executing tool registry supplies lifecycle metadata before optional verbose
activity suppression. Dynamically scoped history and runtime-owned inspection
tools therefore update the same notice without exposing private results or
depending on a client's base registry. This adds no inference calls.
History lookup selects chat/reset event types before copying runtime payloads,
preserving workspace and reset rules while excluding unrelated timing receipts.
Retained inspection descriptors union overlapping ranges of the same unchanged
owned source; fresh visible ranges are subtracted from carryover. Shorter
subsequent reads cannot erase earlier coverage, and duplicate excerpts do not
consume the budget needed for unique facts. Gaps and changed sources remain
unverified and require their own reads.
Retired-review cleanup discards verdicts but preserves validated source-window
descriptors. Their content is always rebuilt from the current request-owned
receipt and fingerprint; no old quality decision or copied fact is restored.
Adaptive browser counters update the same notice with observed pages/findings
without inventing a total or exposing captured source prose.
The shared notice publisher checks request publication ownership before queued
sends or edits, so revoked requests cannot publish stale updates. Approval waits
remain distinct from denials in the lifecycle projection.
Its delay starts when shared execution accepts the request, before memory and
context preparation, so cold preparation does not postpone feedback. Telegram
mirrors secondary chat history outside its event loop and bounds terminal-reply
lookups to recent indexed turns. History mirroring cannot block typing refreshes
or the completed answer; final network acknowledgements precede typing shutdown.
Telegram typing refreshes retry transient failures; background refreshes reuse
one connection pool for their active lifetime. Confirmed delivery stops typing
before post-delivery persistence cleanup.
The first Telegram typing signal starts before attachment processing and
admission, while the separate progress notice retains its fast-turn delay.
Publishing the terminal result releases active-turn ownership and dependency
waiters before history cleanup finishes. Cancellation of one waiting successor
cannot cancel its predecessor. Admission uses only genuinely active requests
plus the current conversation family; older conversations remain recallable
without becoming an indiscriminate clarification menu. The existing semantic
decision receives bounded result/tool context and can filter plausible targets.
Numbered replies to one current option group bind that group without another
concurrent relationship inference.
Explicit delivery of an existing source artifact uses its native descriptor
format when the validated scope omits an extension. Only selected, completed
source producers qualify; excluded formats and incidental research captures
remain excluded. The same contract feeds Web downloads and messaging uploads.

Public research uses one stopping rule across API search, browser-only reads,
Code Mode and delegated tasks: resolve material requirements of the current
request, then deliver. A positive shortlist does not require exhaustive source
discovery. Optional background and media do not block delivery unless required
by the current request or an applicable durable preference. An explicit complete
inventory, comparison or broad negative conclusion retains its required coverage.
This uses the existing primary model and typed source receipts, without another
classifier call or a fixed page quota.

That stopping guidance is backed by a request-owned research allowance shared
across the primary LangGraph loop, Code Mode's native calls, common recovery,
approval continuation, scheduled reports and Deep Agents. The primary's existing
validated scope selects `research_effort` (`standard`, `extended`, `exhaustive`)
and optional material `research_questions`; no prompt keyword classifier or
additional ordinary-chat model call selects a policy. Standard defaults are 24
native read units, eight research model attempts and 75 seconds of research;
extended and exhaustive allow twice and four times that work. Independently
batched reads count their items before admission. Scope widening and retrying
programs do not reset spent work or upgrade an already-reading request.

At exhaustion, both model visibility and the native boundary close external
research. The primary retains exact source receipts, local inspection and
requested production/actions, and composes findings with explicit unresolved
facts. Writing has a separate 60-second/eight-call allowance; native local or
action waits do not spend composition time, and completed productive writes
renew only composition, never research. Artifact-capable model calls retain their
registered generation timeout; only a structured non-source tool selection earns
that generation time back. Selecting another source still spends research time.
Artifact focusing alone never certifies that required source work is complete.
Fixed-tool scheduled/delegated workflows expose the same scope control for
declaring effort, while retaining exactly their existing tool permissions.
Actual provider attempts and native calls are bounded, including a
provider that ignores its timeout argument. Spent counters survive continuation
without counting time waiting for the user's consent. `conversation.research_budget`
records the phase, effort, reason, usage and limits without raw source payloads.

Adapter-declared same-request snapshots reuse exact observations with origin
invocation metadata. Writes, declared observation-changing navigation, and an
explicit `research_refresh` invalidate snapshots; failures are never cached.
Principal, adapter identity and declared context bindings isolate reuse. Code
Mode supports `output_fields` JSON pointers into a final value and a bounded
research projection (16,000 characters per typed source bundle, capped at
48,000; other programs retain an 8,000-character ceiling), while exact native
records and inspector-only windows stay
available. The corresponding `NULLION_RESEARCH_*` environment settings tune the
allowances without changing ordinary chat behavior.

Native page batches enter the same discovery/observation ledger as individual
reads. Successful page outcomes observe their containing pages; outbound links
remain candidates, and failed reads prove no observation. The model checkpoint
shares repeated invocation-id sets losslessly, keeping exact URLs and receipt
identities while avoiding repeated large indexes. Code Mode model projections
are bounded separately from artifact payloads; archival compaction retains the
typed projection rather than reintroducing entire raw source collections.
Original outputs and VM values remain intact and inspectable. Invalid local
inspection pointers return request-owned keys and correction metadata, so a
projection pointer error can be repaired without repeating external research.
Scheduled report-writer errors record their typed cause and phase without
source bodies or credentials. Writer timeouts retain the shared deadline and
have a distinct user-facing cause. A delivered failure notice cannot become a
retained partial report or a successful scheduled-run outcome; previously
grounded partial findings and verified artifacts retain their existing receipts.
Account-summary recovery uses all current-request sources and preserves their
dated facts. It distinguishes reporting what a source says from certifying live
account status, without imposing a new account-status audit on every summary.
Presentation never selects a preferred source family from substring overlap
with subjects, senders or event titles. A combined, paraphrased account answer
remains authored content; raw-payload and copied-body protection apply separately.

Release availability is shared by the Web update button and messaging notices.
Active Telegram, Slack and Discord adapters check in the background, beginning
30 seconds after startup. A SQLite cache and cross-process lease share one
release lookup per hour; an installed HEAD change invalidates that cache. The
target is the same released tag used by `/update`, never unreleased `main`
commits or a downgrade from a development candidate. No chat turn waits for a
release fetch or invokes a model to decide whether to show a notice.

Each configured operator destination receives one notice per available release,
with its native update command (`/nullion update` on Slack). Content uses the
shared `UserMessage` contract. SQLite claims are committed before transport and
survive checkpoints and restarts. Confirmed or uncertain deliveries are never
replayed; only failures known to precede acceptance can retry. The adapters own
escaping and native transport, while shared code owns availability and dedupe.
Notices never install an update automatically.

Ordinary turns expose registered public search and page-batch READ tools beside
the existing cheap read defaults, including later turns with saved history.
Their presence does not execute them, load connector inventories, or add a scope
classifier. Account, outbound-network and approval boundaries still apply at
invocation. Explicit independent search batches run at most two queries at once;
dependent work waits for prerequisite evidence. Cancellation and deadlines stop
new submissions while preserving completed findings and per-query failures.
Model-context projections retain all returned source identities, exact URLs and
prices while removing exact duplicate links and redundant fields. Full receipts
remain in runtime storage. Completed reads use the existing shared progress
contract; client adapters only render and transport it.

Low-level browser tools keep session/page state and serialize operations on the
same physical session. A tracker must carry trusted setup context and the
verified physical session handle into repeated reads. Raw model aliases and a
new task scope must not silently choose a different page. A browser restart can
invalidate in-memory handles; source failure is distinct from a negative match.

`browser_read_pages` executes a model-selected read plan in retained browser
state. Independent URLs use at most two concurrent task-owned tabs, with
navigation, observed readiness and structured extraction in one invocation.
Explicitly related Playwright read tabs share their task's browser context,
including cookies and storage, through Auto and recovery adapters. Unrelated
tasks retain isolated contexts. Closing a read tab releases only that tab;
the context closes after its last task/session lease is released, even when
the parent has no open page or closes before its children.
Text reads retain a bounded visible-link index with coverage alongside the text.
The same index is used by ordinary `browser_extract_text`; links are source
references rather than inferred real-world entities.
Actual constituent reads receive registry-validated native receipts, so the
runtime does not demand redundant navigation/extraction calls. These receipts
cannot attest writes, come from adapter metadata rather than page payloads,
and survive normal continuation and request-delivery metadata updates.

The registered adaptive browser tool enforces a request-local native page read
before starting its nested agent, even when the primary initially selects the
adaptive tool. It returns that read as tool-step evidence so the primary can
answer or decide which interaction remains. Current native observations of the
same URL allow delegation; old conversation history cannot unlock it. Opaque
request identity and objective digests prevent reuse across unrelated requests
or amendments. An explicit typed source-capture delivery requirement can go
directly to visual capture. A generated-image output requirement alone cannot
promote research screenshots to the requested final image. Pending recovery consent and cancellation never unlock adaptive
execution. This rule is shared tool execution policy for every client and adds
no classifier or ordinary-chat model call.

The optional delegated Browser Use worker performs adaptive multi-step browsing
when interaction is needed. Auto/CDP mode reuses the configured managed browser;
explicit Playwright/headless selection uses independent workspace-owned state.
Neither multi-page research nor delegated browsing is the worker used for
frequent live polling. The primary retains the original requested dimensions
and decides when enough evidence has been gathered, without a separate planner
or quality-grading call. Plain turns do not enter either browser path.

Model timing events retain reported token counts when the provider supplies
them. Nested browser performance records retain model/capture time, call count,
reported usage and the count of calls with reported usage. Missing usage is
unknown rather than zero; these measurements do not imply a provider quota
percentage or billing estimate.

## Ownership and durability

Principals, workspaces, source permissions, chat destinations, approvals, and
resource handles remain separate. Durable state belongs to the instance's
SQLite checkpoint. Model/provider calls and chat transports are external
boundaries; retries do not establish successful delivery by themselves.

A live match stops its source checks and creates an outbox notice. Claims,
leases, current-state checks, and bounded retry deadlines suppress late work.
A remote transport can still have ambiguous send outcomes during a crash; this
is not an absolute exactly-once guarantee across third-party systems.

## Linked source context and history reads

Uploaded source descriptors are recorded before model work and retained only on
the selected request ancestry. In-flight amendments can reuse the source even
when their predecessor was superseded before publishing a reply. Independent
requests, conversation resets, cancelled branches, and other conversations do not
inherit those uploads. Missing files are marked unavailable rather than silently
removed from the request.

Scope selection also receives a bounded view of the selected request ancestors,
including in-flight inputs superseded by an amendment. Its cache key includes
that context. Request text and uploads use the same lineage traversal and stop
at cancelled branches, session resets, or conversation boundaries. A supplied
URL remains source context for the original mission. A `live_research` scope
cannot finish with only a navigation receipt.

Explicitly selected tools can expose one hop of declared read continuations as
optional tools. This follows tool metadata, adds no classifier calls, and does
not make those tools mandatory. Write continuations are not added; each tool's
source-consent, visual-capture, boundary, and approval checks still apply. Native
search stays available as a research alternative when declared by browser tools.
Registered read tools tagged `public_source_discovery` are also available for an
explicit URL or a selected web scope when no exact entry tool was chosen. This
availability does not invoke the tool or bypass source consent.
A required `file_download` is also a web source entry. Acquiring an archive or
document for inspection does not additionally require browser navigation to its
download URL; the download, content inspection, and final deliverable still need
their own successful receipts.

Connector app inventories retain their provenance in scope discovery and cached
prompt context. An installed reference catalogue advertises supported routes
(`app_inventory_kind: supported_catalogue`); it does not establish connected
accounts or authorize reads. Account choices require user selection or verified
connection evidence. Existing read tools can inspect an unambiguous requested
source through normal permissions.

A completed, grounded public-source answer does not restart browser work merely
because a tool advertises optional continuations or the request includes a URL.
Explicit unfinished source contracts, known material failures, approvals,
iteration limits, required tools, and pending generated files still prevent that
completion shortcut. Search snippets do not become proof of a full-page read.

Approved reads resume answer synthesis rather than publishing an intermediate
tool receipt as the final answer. Pending batches retain tool-call ids, arguments,
and completed receipts, so the resumed batch can reuse an earlier completed action
without executing it again. Matching applies only to the captured pending batch;
new calls still pass ordinary policy and approval checks. Approved write receipts
retain their direct delivery path.

History search returns bounded excerpts with explicit truncation metadata. The
same tool accepts `read_turn_id`, `start_char`, and `max_chars` to read a selected
saved reply in exact character windows, including material and citations beyond
the search excerpt. Reads retain workspace authorization and do not generate or
deliver files. Saved assistant prose remains reference material rather than
independent verification of external facts. An exact read avoids repeating the
discovery workflow; it does not suppress a recorded material-completion failure.

## Artifact and source completion

Separate captured records may share a display name, such as two departures from
the same carrier. Presentation deduplication compares the verified title and
details actually shown; a shared title or page is not a record identity. Exact
duplicate cards remain rejected.

A typed artifact format contract keeps supporting files out of the delivery
selection. Competing revisions within one format still require an explicit final
selection. Terminal creation scopes use the current workspace artifact directory;
ordinary shell inspection keeps its configured working directory. Web downloads
must resolve through permitted artifact roots, and a missing download must never
be reported as an attachment success.

Media repair feedback distinguishes a missing file from missing embedded raster
data and exposes current-turn reusable image paths. A selected linked/plain
report may accompany a separately selected, verified embedded report; an
unselected older image-bearing draft cannot satisfy that obligation. Repair
targets the existing output rather than recreating the entire package.

Staged files retain provenance aliases to their original generated paths. Those
aliases can select only known deliverables; several revisions sharing one source
path remain ambiguous. A successfully downloaded source image may replace an
earlier browser collection candidate without changing the required final format
or attaching either source image to the chat.

SVG embedding renders the actual self-contained artwork in an inert Chromium
image document with external requests blocked. Content-addressed raster copies
are written atomically; adjacent PNG files are not assumed to be equivalent.
Conversion failure must not fabricate a generic image in place of the artwork.
Self-contained SVG image data URIs (base64 or percent encoded UTF-8) use the same
resource validation and native rendering as inline markup. Inline HTML SVG
normalizes HTML character references for XML rendering and preserves local
XLink references. Data documents remain strict XML. Failed media guards retain
only structural counts in diagnostic summaries, never the private HTML body.
PDF HTML can supply a complete inline SVG directly. The shared media guard
recognizes actual drawing elements; empty, malformed, and definition-only SVGs
do not satisfy the visual requirement. The typed media contract reaches the
native tool invocation after scope changes. When that contract requires an
embedded visual, the PDF adapter resolves the SVG's document CSS in an inert
browser and embeds its actual pixels, preserving the original viewport and
layout. External resources remain blocked. The final PDF must contain the
embedded image; a successful conversion call alone is insufficient. Ordinary
PDFs without this requirement retain their native vector rendering.
Self-contained HTML preserves complete inline SVG artwork unchanged. Its writer
and final file-evidence counter use the same structural drawing and resource
checks; empty definitions and external image references do not qualify. Native
HTML/PDF scope declarations retain these supported visual requirements even
when no separate browser or image-generation tool is selected.
The media guard consults the saved artifact's native media evidence when an
inline illustration has no separate source-image path. A separate plain memo
can accompany an illustrated poster without being forced to generate another
image. PDF print measurement uses the paper's print viewport and media, and an
automatically inserted illustration cannot collapse the following grid/flex
content to zero width. Generated captions do not expose source asset filenames.
PDF media preserves the complete image rather than cropping it to fill its box.
Spreadsheet cells remain data even when a value is an image filename or URL.
Embedding requires an explicit media field or image-path argument; listing files
must not silently become an image-generation or media-repair workflow.
Browser JavaScript inspections attach independently read local-document
provenance. Inspecting a generated document does not count as new source
research or make its own file stale. An arbitrary script return value cannot
claim that provenance. PDF printing keeps headings with their following content.

Material-requirements planning also corrects an invalid structured response once,
without tools or repeated file creation. Diagnostics record the failed schema
check and attempt number. Prepared filenames and repaired versions do not
invalidate an otherwise unchanged requirements contract; new evidence-tool
families and changed request context still invalidate it. Large existing
contracts receive a bounded response budget proportional to their row count.

Chat delivery commits a bounded conversation checkpoint instead of waiting for a
full runtime snapshot. It includes current thread metadata, recent receipts,
idempotency keys, and active task-frame state. A late scoped writer can append its
receipt and merge committed keys without rewinding a newer conversation head.
The shared HTTP/WebSocket chat path and approval continuations hold the existing
runtime-turn guard for their entire execution. Full dashboard status refreshes cannot replace that active
store while tools or the model still use it. After the turn, normal refresh can
observe other committed writers; conversation receipts and later tool timings
remain durable. Checkpoint reloads for dashboard status run outside the ASGI
event loop so a slow read cannot block chat completion or health responses.

CommonMark lists written with repeated `1.` markers are rendered and saved with
distinct visible ordinals. Numeric follow-ups resolve against those same saved
choices; separate lists and code blocks retain their original meaning.

A command that exits unsuccessfully does not make core execution unavailable.
The agent can correct its command under the same permissions. Identical failing
invocations still exhaust the failure budget, and that typed terminal state is
preserved through outer recovery rather than starting the whole request again.

Focused completion repairs have a bounded iteration budget. Browser cancellation
propagates to the owning isolated worker and releases its resources before another
request uses them. Linked clarifications may reuse an exact source declaration
from selected user-request ancestors for read tools; background history cannot
supply that authorization, and account writes retain their current-turn checks.
Internal browser jobs carry structured conversation and invocation ownership ids
so diagnostics and scoped cancellation do not depend on rewritten task prose.

Recovery runs carry current-request receipts into the graph before validating
completion. Conversation, principal, and request identity must match; another
conversation or a different request cannot reuse those receipts as current proof.
A retry retains completed invocation signatures and files while preserving the
registry's original action authority and normal approval checks.

A later inspection is a read receipt, not a new producer version. Its referenced
path cannot invalidate the generating tool's image provenance. An actual rewrite
still invalidates that proof. When packaging changes the delivery format, a
separately requested, self-contained sibling can retain its original format only
through a validated selection bound to that exact path and producer version.

A recovery also receives bounded, request-owned tool receipts in its model
context, after conversation/principal/request identity checks. Holding receipts
only in graph state cannot inform inference. Scope inventories are excluded and
serialized outputs stay inside the untrusted-evidence boundary. This recap grants
no new tools or actions. Optional link capabilities do not become required work;
link follow-up hints need an explicit boolean obligation before triggering a
recovery. Existing structured required reads still run through normal fulfillment.

## Code map

| Area | Source |
| --- | --- |
| Chat and context | `src/nullion/chat_operator.py`, `turn_context_policy.py` |
| Dispatch and amendments | `turn_dispatch_graph.py`, `messaging_turn_graph.py` |
| Tool execution | `agent_orchestrator.py`, `tools.py`, `runtime.py` |
| Presentation and files | `response_sanitizer.py`, `artifact_lifecycle.py` |
| Messaging delivery | `messaging_adapters.py` and each platform's `*_app.py` |
| Recurring work | `crons.py`, `cron_delivery.py`, `doctor_crons.py` |
| Live observations | `live_tracking.py`, `live_tracking_tools.py` |
| Browser | `plugins/browser_plugin/` |
| Web controls and API | `web_app.py` |

For contributor workflows and verification boundaries, use [Testing](e2e-testing.md)
and the repository's `AGENTS.md`. Runbook commands should derive runtime paths
from the active instance, not a contributor's personal checkout.

Completion evidence accepts both textual and structured location fields. A valid
place object from a source adapter must not invalidate the accompanying current
data merely because it is not a display string. Empty, failed, and control-only
place objects remain invalid. This validation uses output schema fields and adds
no classifier or extra source lookup.

A mixed delivery selection can retain completed documents, workbooks, and data
exports while returning only linked HTML members for portable packaging. The
producer receives separate linked and retained path lists and must reuse the
finished outputs. The selector still verifies candidate file signatures and
rejects unknown or changed paths. Native spreadsheet CSV and TSV variants remain
plain data exports; they cannot satisfy or inherit a workbook's embedded-media
requirement.

Current validated output exclusions supersede an older task frame's matching
attachment requirements during fulfillment. Optional tool continuation metadata
may advertise further reads, but cannot reopen a resolved file-identity scope or
a final action receipt after all explicit requirements are complete. Inspected
text and delivered image previews remain answer evidence, while filename-only
searches render compact inventories without attaching their inputs.

Normal chat scope identifies recurring scheduling separately from live tracking.
An executing cron blocks both scheduling and live-tracker control capabilities;
that execution deny policy must not classify live-tracker tools as recurring
scheduler actions in an ordinary chat.


Failed CDP connections disconnect their own Playwright driver with a bounded
cleanup deadline before using the existing raw-CDP fallback. Cancellation clears
the same owned transport reference. Cleanup never closes the borrowed browser;
a stalled disconnect must not block every later browser operation.


Filename-only discovery preserves a composed answer that names a verified file.
An internal `MEDIA:` pointer is not a composed filename answer. When source
attachment delivery was not requested, that pointer cannot invent a new artifact
requirement or leave an attachment-success receipt after the file is suppressed.
Runtime-generated evidence previews fall back to the verified filename inventory;
inspected source answers and explicitly requested existing-file delivery keep
their separate contracts. These checks use tool schemas and protocol metadata,
not user-language phrase matching.


Connector scope metadata distinguishes source resolution from source selection.
A verified source with `source_resolution_required=true` and
`source_selection_required=false` still needs an exact read-tool scope; it is not
permission to invent account choices from a supported-service catalogue. A
terminal answer in that state triggers at most two LangGraph resolution nudges.
Failure to resolve then returns a typed terminal failure and prevents another
outer recovery loop. Genuine source-choice states retain their numbered options.
No new classifier, provider probe, or inventory call enters ordinary chat.

An existing workspace edit that needs a managed delivery copy retains its
basename inside a unique private export directory. The original edited file is
source state; only the named export is deliverable. Delegated external bundles
share one export namespace and retain relative paths among selected files,
without copying unselected parent contents. Exact native filename literals take
precedence over partial filename tokens during alias selection. The common
attachment protocol reads the complete path on its line, including spaces,
Unicode and quoted paths; normal root and delivery-role checks still apply.
Transport directive lines are excluded from filename preferences: an internal
attachment path cannot select an output or displace another selected artifact.

Web archive resets persist only the owned conversation's reset event and active
frame change through bounded SQLite persistence, without waiting for a full
runtime snapshot. The archive operation runs outside the Web event loop. Scoped
checkpoint rows use their real collection tables, task frames use frame identity
rather than branch identity, and late reset/frame writers respect reset times so
they cannot remove a newer frame or reactivate an older one. History-retaining
archives continue to expose saved chat evidence for a later fresh conversation.

Weather and market reads return their full structured receipts to the primary
producer. `direct_read_complete_request=true` is a read-scope hint, not proof
that a current-value template answers a date, period, units, field or comparison
question. The producer selects the relevant observations and applies the shared
presentation policy; no independent reviewer or extra intent classifier is added.
When no model is available, the native weather fallback preserves every dated
daily forecast rather than silently selecting the first row as “today”. Weather
geocoding uses structured locality/region fields for a compact place label while
preserving the exact resolved coordinates and source provenance.

Live notification delivery persists the exact notification audit event with a bounded
SQLite upsert. Durable chat history and the live notice ledger own delivery receipts;
a full runtime snapshot must not block `finish_notice`. If the secondary audit write
is temporarily busy, the event remains in memory for the normal later checkpoint
without turning an already delivered notification into a failed send.

A typed artifact delivery contract uses the configured artifact model deadline,
including the research-to-production handoff before a producer is visible.
Keeping source access during generation must not silently revert to the shorter plain-chat deadline.
Plain turns keep their existing deadline and do not gain a classifier or extra model
call. Model timeout diagnostics record the deadline used at the failed invocation.

Declared output formats supported by selected registered producers remain output
requirements even when a research tool also emits that format as incidental
source evidence. Optional producers stay optional; the runtime does not invent
another mandatory action. Native model interruptions without a final file retain
their cause in the shared artifact manifest, so Web and messaging delivery report
the interruption rather than replacing it with a generic attachment failure.

The existing search-plan model may declare `source_discovery_required=false`
for a comparison of finite requested subjects or properties. Every required
subject and fact still needs its own grounded completion coverage. This flag
does not waive discovery for a comprehensive search or a broad negative
conclusion, and older plans retain their conservative discovery requirement.

`spreadsheet_create.rows_source` reads a complete local JSON table through the same
workspace/approval boundary as other local file reads. It accepts a JSON array or
explicit top-level rows/columns keys, rejects mixed inline/source tables, limits input
to 16 MiB and 100,000 rows, and retains explicit row-count validation. Archive inventory
references advertise this generic table input so a compact preview does not become
an empty workbook or a partial reconstruction. Source JSON remains internal.

Multi-sheet row counts have explicit scopes. Top-level `expected_rows` remains
the total across every worksheet. Each `sheets[i].expected_rows` validates only
that sheet's data rows, excluding headers and other sheets. A four-entity table
can therefore coexist with summary and assumption rows. Mismatches report the
scope, JSON pointer and actual worksheet counts before writing a file.

Archive extraction reserves an empty destination atomically. An existing destination
is preserved and a fresh permitted sibling is used, with its actual path returned
in the tool receipt. If the permission boundary allows only the existing destination,
the tool requests a new permitted destination instead of merging or overwriting.
Follow-up transformations must use that extraction's inventory or actual directory;
files from earlier requests cannot silently join its data set.

Session stop persists only the cancelled conversation through a bounded scoped
checkpoint and runs that persistence off the async event loop. A busy secondary
write cannot reverse an effective cancellation. A stopped-session timestamp clears
its persisted active frame without clearing a newer frame written by another turn.
Full SQLite checkpoints encode payloads and compare existing rows before dirtying
pages, limiting the time rollback-journal readers are excluded from the database.
Restarted runtimes initialize their checkpoint fingerprint with the same canonical
hash used by later saves. An unchanged local snapshot skips a full save even if
another process has written to the database. This does not acknowledge that other
snapshot: the next real local change must still merge it, and Telegram must still
refresh before treating it as loaded. A forced save or missing checkpoint still
writes. Building and merging a genuinely changed full snapshot remains proportional
to retained state; scoped event saves remain the preferred hot-path operation.

Web and Telegram background checkpoint requests use the same coordinator and
coalesce per runtime. Current-turn conversation/head/ingress receipts are written
incrementally, including commands without a chat head. Telegram completion no
longer waits for a full snapshot; action and approval commits remain synchronous
at their existing native boundaries. A runtime's own completed snapshot signature
avoids an unnecessary full reload; another writer's unseen signature still needs
refresh. Full snapshots remain proportional to retained state and are not a
replacement for scoped foreground writes. A forced request arriving
during a normal save forces the next pass; it does not force every later pass.
Worker cleanup is bound to that worker's ownership token so a retiring worker
cannot clear a newer worker's pending requests. Failure to start a worker clears
its queue ownership so a later request can try again.

Unset streaming and thinking-display settings are authoritative indexed values;
they never fall back to copying full history. Recent artifact receipts are read
from the bounded current conversation index. Initial context uses compact
capability awareness for discovery/native read scopes, retaining profile, memory,
consent rules and exact tool discovery contracts. Connected-source, scheduler and
artifact scopes keep their richer context, loaded after structured selection.
Registered read-only tools use their metadata for this decision, including the
initial uncached scope; an installed native read does not force a full contract.

Composite adapters may attest that an invocation executed only declared native
reads. The registry binds that receipt to the invocation and accepts only its
registered read operations. Generic action-receipt enrichment preserves native
metadata and does not describe an attested read-only invocation as an action.
Selected-parent observations retain those facts and their timestamps across
successive follow-ups, while action contracts and permission grants are excluded.
A substantive answer is not replaced merely because it does not repeat search
snippets: the primary agent owns its response to the current question.

Source-result compaction branches on adapter-owned collection declarations,
including collections returned by composite tools. Large model views retain row
order, record counts, declared identity/context columns and original JSON
pointers. Clipped values and omitted columns are explicit; exact records remain
in the request-owned receipts and are accessible through retained inspection.
Provider call-id ambiguity prevents replacing history blocks, but does not deny
inspection of exact request-owned invocation ids. Extraction, artwork, report
content and source-verification logic continue using the original receipts.
The model view also preserves native-read strategy and its declared adaptive
continuation. A native preflight does not imply that the interactive controller
ran; the primary can select that continuation when page evidence leaves an
interactive requirement unresolved.

Research recovery and comparisons remain primary-agent reasoning. Existing
inference receives focused guidance after observed discovery or an external
source failure, without a reviewer, extra classifier or execution cap. A failed
provider is distinct from absent availability; alternative routes must respect
the user's source restrictions and consent. A shared comparison instruction
normalizes quantities, calls for explicit arithmetic and distinguishes identical
specifications from functional alternatives. No calculation tool is required.

`ToolSpec.independent_batch` declares the array argument and explicit independence
flag for a read tool. The shared tool graph validates each adjacent model call
against its schema and adapter preflight before combining matching arguments
within the declared array limit. The effective model transcript retains the
actual merged invocation/result; a runtime audit records the original call IDs.
Current-tab reads, different sessions, dependent plans, approvals, writes and
invalid/blocked read plans remain separate. Browser reads retain their native
two-tab concurrency limit; search batches use the same generic contract.

Text previews use the shared `LatestTextUpdateQueue` on Telegram, Slack and
Discord. Model token callbacks never wait for transport edits: at most one edit
is in flight and the newest draft replaces older queued drafts. Finalization
discards obsolete queued previews, retains ownership of the in-flight send,
and applies final formatting to the same message. Completed answers are sent
immediately on Web and Telegram rather than replayed as artificial streaming.
An unsuccessful final edit withdraws the draft before fallback
delivery; a draft is also withdrawn when a decision or attachment owns the final
response. Typed already-delivered metadata survives response unwrapping.

Browser Use source tasks reuse the SDK's fresh decision snapshot for captured
evidence instead of independently taking another snapshot before and after
each inference. The SDK's automatic vision mode uses text and controls first;
a structured screenshot action includes the current image when visual inspection
is needed. This action cannot create or attach an extra file. Existing runtime
source screenshots remain available as evidence. Explicit page reads and completion checks still capture fresh
state after actions. The adapter reports model/capture timings and publishes
deduplicated runtime page/finding counts through the existing shared activity
channel, including through the authenticated isolated-worker bridge. It never
uses source prose as a progress message or claims that a finding completes the
request. Optional progress failure does not fail the lookup; ordinary turns
do not load the browser, poll progress or make another inference call.
Nested model/capture timings and browser session-recovery reasons are preserved
in tool timing events even when source receipts are compacted for history.
Browser decisions use one native `browser_decision` schema through the configured
model client and isolated-worker bridge. They are validated before the SDK acts;
text-JSON compatibility still uses the same validator without another request.
The schema is no longer appended as a large user message at every decision.

Native browser tools try one bounded reconnect to the configured browser after a
verified connection failure. Observation/navigation operations may replay; clicks,
submissions and other uncertain actions do not. Reconnect restores uniquely
identified task pages and never guesses between duplicate URLs. If reconnection
fails, typed `user_choice_required` state pauses the run instead of triggering
completion retries. The common `UserMessage` contract presents numbered recovery
choices. A user-selected headless session applies only to that task, preserves
the configured browser and other tasks, and is passed to the isolated SDK worker.
Public-page work need not be signed in; account-only work may still need the
original session. This recovery adds no model decision to ordinary requests.

Auto/CDP controllers select one managed endpoint: the normal CDP setting wins
over the older SDK-only override. A controller attach/capacity failure returns a
typed `alternate_tool` handoff to ordinary browser tools on that existing session.
It cannot silently launch an independent browser, discard the login, or claim the
request is complete. Native browser opening/reading resolves the control handoff;
the primary producer still completes the user's goal. Shared-browser shutdown
detaches the controller rather than killing the user's browser. The explicitly
non-interactive Playwright mode can use workspace-owned independent SDK state,
with a storage-state lease and the configured visibility, without borrowing another
browser's login. The SDK restores cookies and origin storage from a private workspace
snapshot across worker restarts. A fresh snapshot before shutdown preserves sign-out
without merging deleted cookies back from older state. Browser scratch profiles and
files remain temporary; managed Auto/CDP profiles remain owned by the existing browser.

Scheduled execution leases include process ownership. On POSIX, a verified dead
owner can be reclaimed before expiry; live owners remain protected. Legacy tokens
and platforms without a safe liveness probe retain their bounded expiry grace.
Startup reconciliation must preserve runs owned by another live process. An
interrupted owned run from the past 24 hours produces one durable, retryable notice at its configured
delivery destination, without waiting for the repeated-failure threshold. The
notice explains the missing result and how to request another run. It honors
notification opt-outs and does not replay actions automatically: effects completed
before interruption may already exist. No new model call is needed for recovery.

Connector metadata resolution verifies only that a quoted source reference occurs
in the current or structurally linked user request. That quote alone does not
establish selection of a specific account or permission to read it. Resolution
receipts and bounded recovery instructions preserve the original request's
source-choice and consent requirements. The agent can resolve an explicitly
selected source from metadata, or return to typed connector discovery with
`source_user_requested=false` to request the numbered source choice. This uses
existing scope turns rather than a classifier on ordinary chat requests.

Oversized scope results retain their typed source-selection, permission,
scheduler and artifact contracts in chat history, Web history and startup
maintenance. Optional connector catalogues are omitted from the compact
receipt rather than replacing the whole scope with a JSON text preview.
Source-choice options retain only compact provider labels and read-tool
metadata. A truncated source quote cannot retain a verified-evidence claim.

Tools tagged `required_scope_only` remain unavailable when merely listed as optional alternatives. An exact validated requirement activates them without a new classifier call. Plain chat retains its existing fast path. Presentation cleanup preserves multiline answer bodies and facts identified by declared source-record identities; mentioning a tool in a data table does not make that table a disposable status paragraph.

Scheduled-delivery audit records and chat turns use exact bounded SQLite writes across Web and messaging. Busy writers retry captured receipts off the delivery path; they do not delay terminal job status behind a whole-runtime snapshot. Attribute evidence may serve as non-entity background context; interface controls still cannot be promoted to entities or background facts.

Material fact requirements keep stable identities while accepting later completed source adapters with registered read/source-evidence metadata. This changes admissible proof, never invocation permissions or action requirements. Large source histories retain bounded declared identity paths without misrepresenting truncated content as complete source records.

PDF text positions are measured at text-drawing operators. Extractor text buffers may flush after the graphics transform changes; using that later transform can falsely place valid text outside later pages and trigger needless regeneration. Font metadata still comes from native extraction; neither method claims pixel inspection.


Raw CDP navigation correlates document-ready lifecycle events with the returned
frame and loader identifiers. Queued events from older documents cannot finish
a new navigation. Same-document navigation uses its own event, and provenance
comes from the observed document URL, including redirects, rather than the
requested argument. Protocol reference: [Chrome DevTools Page domain](https://chromedevtools.github.io/devtools-protocol/tot/Page/).


### Compact tool contracts

A tool may opt selected scalar schema properties into discovery with `ToolSpec.discovery_parameters`. Both compact chat awareness and full capability context project those values from the registered input schema, including interval and duration bounds. Tool names alone do not establish unsupported limits. Discovery remains metadata-only and cached by registry revision; changing schema or discovery metadata invalidates the corresponding stable context. A missing user-selected lifetime requires clarification before tracker creation.

Live observation setup records a bounded admission receipt for each exact source
read, independently of model-context compaction. Oversized reads retain their
rejection reason instead of becoming indistinguishable from untested probes.
The model receives the exact prepared tool arguments and can narrow and recheck
an inadmissible source. Setup and polling share the same Unicode-preserving size
budget. Source-region precondition errors keep the existing tool scope while
inspection repairs the selector. A verified successful read clears only that
tool and browser session's consecutive precondition failures; unrelated reads
and source transport failure budgets do not reset them.

Semantic collection polling persists bounded identity hashes from adapter-owned
`source_record_collections[].change_identity_fields` metadata in the tracker
payload. Each evaluation receives current record pointers with baseline/seen/new
facts derived from that durable state. No source prose or vendor-specific path
selects temporal behavior. Only resolved observations commit the seen identities;
source errors and incomplete-scope decisions preserve the previous baseline.
The existing per-observation and evaluator packet budgets remain enforced.

### Chat layout and Telegram serialization

Chat clients wrap prose to the viewport. Scheduled-report presentation must not
insert fixed-column wraps, infer a heading from prose followed by prose, or add
paragraph gaps between consecutive continuation lines. Keep explicit list,
paragraph, code, and artifact boundaries intact on repeated delivery passes.
Generated scheduled-task headers are idempotent for Unicode and symbol-only job
names as well as Latin names, including when a platform adapter wraps a report
that the shared delivery workflow already formatted.

Telegram uses the shipped Mistune inline parser with an allowlisted HTML
renderer. It supports emphasis across a single soft line break, nested emphasis,
formatted link labels, quotations, and literal code. Code entities stay outside
emphasis, link, and quotation entities to satisfy Telegram's nesting rules.
No classification or extra model call is needed for formatting.

Render before splitting: `format_telegram_chunks` closes and reopens formatting
spans at message boundaries without splitting HTML tags or entities. Budgets are
conservative UTF-16 units, including serialized markup. Foreground and scheduled
replies use the same chunker. An oversized attachment caption is delivered in
full as separate messages rather than truncated; the attachment bytes remain
unchanged. Formatting fallback sends readable plain text without a diagnostic
wrapper or newly exposed Markdown delimiters.

Delivery formatting must preserve numeric facts. Collapsed numbered-list repair
requires an anchored, consecutive enumeration, and orphaned-list renumbering
requires one contiguous ascending list. Ordinary prose dates, amounts,
measurements, and separated references cannot be treated as options. This
syntax-based transformation is language independent and remains outside product
intent routing.

Saved execution setup has a separate 30-second model deadline; the existing
short source-scope path retains its five-second deadline. The resolver receives
a valid JSON schema, and successful saved contracts remain cached against task,
connection permissions and registered writes. An attempted but failed setup
returns a typed failure before source work. It must not silently become a
read-only grant or trigger producer/recovery calls with requested writes missing.
Preflight diagnostics include validity and safe failure categories, never raw
provider exceptions or connection records.

`workflow_reasoning.py` supplies one provider-independent reasoning contract to
existing chat, orchestration, planning and delegated-agent calls. It asks the
model to establish the whole requested outcome from current context and selected
references, check premises and constraints, correct its own plan, and compare
what was requested with what the final answer and files actually deliver.
Conflicting dates, targets or preferences require a specific correction or a
numbered question; the model cannot silently replace the user's target or grant
itself account access. These instructions improve model behavior; they do not
prove correctness by themselves.

Missing required tool arguments also return a typed `correct_arguments` result
before a handler runs, including the missing fields and their registered schemas.
Retry accounting distinguishes adapter-declared validation categories: correcting
one category and reaching a different check is progress, while changing argument
values or error prose cannot reset the same-failure limit. Global turn budgets
and the live tracker's three-failure pause remain separate limits.

This does not run a full agent on each live-tracker poll. Polling retains its
bounded observation/evaluation/repair contracts; setup and user-requested changes
receive the shared request checks. Model assessment can still miss an error;
structural validation, permissions, evidence binding and delivery receipts remain
independent requirements. Native weather resolution also preserves a requested
postal area: when geocoding returns a parent locality covering several codes,
it obtains a matching structured postal result before using its coordinates.
An unresolved area is a location failure, rather than a silently substituted
city forecast.

### Shared workflow recovery state

Report-recovery evidence budgets reserve space for later source families and
retain account record identities, check scopes, date bounds and result counts.
Repeated identical hydrated reads do not crowd out later checks or receipts.
Truncated content remains marked as truncated. The connector email adapter
exposes page completeness and continuation tokens; only adapters with actual
pagination support advertise that input in their tool schema.

Saved-task execution carries a typed `execute_saved_task` phase and the saved
job identifier. Recurring enabled/disabled state does not determine whether an
already-dispatched one-off run may execute. Local working files and checkpoint
writes are distinct from registering or changing background work; every normal
permission and approval boundary still applies.

Reusable-tool catalog receipts distinguish the current operation from the saved
definition's execution history. Save and read metadata use `execution_verified`;
only the actual run receipt uses `verified` for its validated execution result.
A successful save therefore proves persistence without claiming the code works.
Catalog searches include a typed `tools` collection, a returned-row `count`, and
`has_more`, including an explicit zero-result search. These receipts remain
citable independently for requested search, save, and run requirements; a save
or read cannot substitute for a required execution.

An execution-only internal file prerequisite (`constraint` with
`delivery_required=false`) can cite its current runtime-bound working-file
record when the producer is eligible and the record is explicitly internal.
An explicit file creation action can use its current file record only when the
matching positive producer receipt is also present and eligible, whether the
file is selected for delivery or remains internal.
This verifies the intermediate file without attaching or narrating it. It does
not allow a generated artifact to establish external facts or user-facing
constraints, and a superseded execution receipt alone cannot prove current
working-file contents. An unbound artifact-record claim is rejected.

When source discovery only prepares accepted background work, the foreground
material contract can be setup-only. That phase requires a real accepted
execution receipt and action rows eligible only for the accepted setup tools
and registered read-only preparation tools, with an accepted setup action required;
configuration constraints and internal preparation remain independently checked.
It does not demand search coverage of future matches. The request-bound cache
retains this applicability decision, while any immediate source fact,
comparison, researched file, or other action retains ordinary source coverage.

Scheduled report recovery reserves a bounded identity/status inventory before
allocating source excerpts. Large email bodies cannot consume the space needed
to represent later calendar checks or action receipts. When compact details are
insufficient, the report composer can use the shared typed evidence inspector
once against retained outputs from that run; it cannot invoke tools, read
files, gain permission, or access another request. One rejected inspection may
be corrected, and repeated invalid requests fail explicitly. Source-data
omissions are unknown rather than proof that a check did not run.

Scheduler inventories expose an offset, stable inventory version, completeness
flag and continuation offset. Model-context compaction keeps identities before
long task instructions and preserves structured pagination. A sampled history
page is marked as a sample and cannot advance its cursor past unseen jobs. A
changed inventory invalidates an old continuation version instead of silently
skipping or duplicating jobs.

The cron create/update API accepts an explicit `schedule_timezone`. Valid IANA
timezones are retained with the job and its next-run calculation; invalid names
are rejected without changing the saved job. Omitting the field retains the
existing local-default behavior. Disabled manual replay clones can therefore
preserve the original schedule and timezone without activating recurrence.

Cancellation stops scheduled report repair and continuation. Completed receipts
remain available for diagnostics, while cancellation is recorded separately from
an incomplete report or source failure.

Tool workflows use `workflow_recovery.py` inside the existing agent graph. The
controller records terminal tool facts, invocation signatures, unresolved
failures, successful repair receipts, and a request-scoped recovery budget. Its
identity includes the principal, conversation, and goal. Same-request recovery
and approval continuation retain this state; an unrelated request cannot inherit
its authority or evidence. Amendments still use the existing task/frame context.

A failed call with a validated repair contract contributes a compact checkpoint
to the next existing model round. It does not start a new classifier or require
a second provider. At most two distinct recovery checkpoints are injected per
request; repeated unchanged failures add none. The existing tool-failure,
iteration, timeout, cancellation, and approval limits still apply. Discovery
remains bounded by `RequestRecoveryContract`, including account consent and the
original allowed actions. Completed action receipts remain the authority for
avoiding duplicate writes.

Progress is stored in bounded `workflow_checkpoints` SQLite rows, independently
of whole-runtime snapshots. Stale revisions cannot replace newer progress.
SQLite contention preserves the in-memory/continuation state and does not abort
or repeat a completed action. These rows are progress evidence, not an automatic
crash-replay queue. Ordinary text turns allocate no checkpoint and perform no
checkpoint database access.

Adapters can declare `ToolSpec.observation_setup_tools` and return structured
`observation_setup` prerequisites. `observation_replay.py` validates those calls
against the current registered schemas and permits only bounded, dedicated READ
tools. Live workers replay them at worker startup, in a job-owned session, without
changing matcher indices or baseline history. Browser item extraction records
the actual rendered source URL so a different process or restarted worker can
load a fresh page instead of borrowing a process-local creator tab. All steps
share the job session; an identical navigation already completed in the same
check is reused. New portable observations default to `source_refresh: navigate`:
later checks reload the saved source in the same worker tab. Explicit
`source_refresh: reuse` retains the loaded DOM for a verified self-updating source;
existing saved choices are preserved. A verified missing page
invalidates only the worker startup receipt and permits one reconstruction.
Worker session namespaces derive from adapter-owned source recipes, with
context-bound creator aliases excluded from recipe identity. Distinct recipes
retain distinct pages; equivalent reads and explicit prerequisites share a
source namespace. Prerequisite completion receipts include their actual target
context, so a past navigation on another page cannot satisfy the current read.
Terminal and paused jobs release only their own resources.

Stored schedules optionally carry `execution_window` with an explicit `fixed`
or `rolling` policy (`workflow_validity.py`). No dates are extracted from task
prose. Fixed windows stop after their authorized end; rolling windows produce a
fresh date range in the configured timezone on every run. Legacy schedules have
no policy inferred for them. Ambiguous choices require normal numbered user
clarification. The policy does not assert that a source published data or that
an action was completed.

A failed portable observation may use `observation_recovery.py` when its adapter
explicitly declares editable `observation_repair_parameters`. It inspects only
tool-declared, bounded READ prerequisites, asks the configured model for one
schema-validated argument update, and executes one fresh read. Fields outside
the adapter's repair contract, source projection, session binding, matcher,
permissions, and condition remain unchanged. Browser item reads permit selector
repair while retaining the original section heading, collection limit, and projection.
Browser snapshots include bounded, observed region locators, headings, and link
counts so repair decisions can refer to the actual rendered structure.
Successful condition evaluation and the current job lease are both required
before the replacement probe commits. Unknown results, denied access, invalid
proposals, cancellation, and expiry never save the proposal. Ordinary polls do
not invoke this repair model; unresolved checks retain the existing pause and
notification policy.

### Progress during completion recovery

A partial producer may select exact `retry_tool_names` from its supplied tool
catalog. The runtime validates that structured selection, focuses the resumed
registry and retains completed reads/actions, page-chain evidence and the exact
report identity. The partial file remains a fallback while execution continues.
Its request/file digest prevents unchanged incomplete bytes from activating the
file-exists completion shortcut after a later source read. A changed report gets
the delegated content verdict before it can become complete. Failed recovery
retains an unchanged valid partial file; it does not generate replacement copies.

Consumed source results larger than 8,000 characters are eligible for archival
previews before the aggregate 64,000-character tool-history target is reached.
This is a model-context optimization, not a source, output, or execution limit.
The newest source/inspection batch, media blocks, request amendments, action
inputs, and original receipts remain intact. Repeated presentations of one
cached receipt and repeated nonexecuted argument failures use typed references
to the newest equivalent contract, retaining one complete visible copy.
Ambiguous or repeated protocol ids keep their original evidence in full rather
than guessing ownership or generating self-references. Different schema constraints, executed
failures, approvals, and distinct action receipts are not merged. Exact original
outputs remain inspectable; an omitted preview field never means source absence.
Runtime recovery checkpoints replace only the previous runtime-owned checkpoint,
validated by its position and digest; user messages are never matched by prose.
No summarizing model call, connector read, or new tool-call quota is introduced.

Interactive fast profiles retain their original configured workflow client.
Tool receipts and typed artifact/presentation state promote the current request
to that client. A structured `max_output_tokens` incomplete response can also
promote and replay before any partial tool call executes. Promotion persists for
the rest of the request, honors the original client budget, and never mutates a
shared client. Ordinary completed chat stays on one fast-profile call. Model
timing events record the selected profile and output cap. LangChain inherits the
client budget unless a caller explicitly supplies an adapter override.

Adapter-declared `ToolSpec.same_request_read_cache` permits reuse of an exact
successful read snapshot in the same request. Any non-read attempt invalidates
the cache. Native message-body reads opt in; browsers and live observations do
not. Failures, changed arguments and separate requests cannot reuse snapshots.
The existing calendar/search deduplication is also invalidated by writes and
binds receipts to their own invocation arguments, preserving older evidence
without mistaking it for a refreshed result.
Tool timing records include an argument fingerprint without raw arguments.

Evidence binding distinguishes workflow roles from independent subjects: an
observed source may support its fact and the report describing it. Independent
entities/comparison sides, separate actions, and separate requested files still
require their own eligible evidence or validated record bindings.

### Tool argument protocol recovery

Model timing records retain completion status/reason and malformed-call counts;
tool timing records retain decode category, offset, length and digest without
logging raw argument bodies. Scoped tool visibility alone does not establish
that an image provider is unconfigured, and setup hints cannot replace useful
source findings or local-renderer results.

The LangChain adapter preserves rejected calls in `AIMessage.invalid_tool_calls`
instead of turning them into executable empty/default calls. Native tool nodes
therefore cannot execute those fragments. A subsequent existing model round
retains an explicit protocol-correction notice in its assistant history.

Runtime coverage overrides preserve independently grounded findings instead of
reconstructing an empty decision. After the existing protocol correction, an
invalid completion matrix may retain a valid partial-findings certificate. It
cannot certify completion, a conditional alert, a write or file delivery. Unknown,
failed/denied citations support only explicit limitation rows; inspection-only or
unknown citations cannot support publication. A limitation certificate never
satisfies a material requirement or grants file delivery.
Clarification and approval retain certified findings followed by numbered
choices. Existing artifact selection, validation and delivery guards remain in
force; a created file is distinct from a delivered file.

### SQLite connection ownership and source failure facts

Short-lived database operations use `sqlite_connections.sqlite_connection` (or
an explicit equivalent owner). Its transaction context retains SQLite's commit
and rollback behavior and closes the connection in `finally`, including early
returns, setup failures and commit failures. A native SQLite transaction context
alone does not close its handle. Approval claims, scheduler settings, memory,
credential persistence, caches and workflow checkpoints must not depend on
Python's garbage collector to release database descriptors. Persistent owners
remain responsible for their own lifetime. Read-only URI connections construct
the filename with `Path.resolve().as_uri()` before adding mode parameters, so
literal `#`, `?`, `%` and Unicode path components retain their filesystem meaning.

Conversation-analysis failures return a controlled HTTP 500 with the normal
`ok`/`error` envelope. The endpoint logs the exception type and does not expose
private storage or provider error text, or raise another exception while handling
that failure.

### Requirement-bound operational failure attribution

Without an explicit causal binding, a useful eligible source read prevents an
earlier failed attempt from being inferred as the reason for an unfinished
comparison or missing state verification. Entirely failed source paths retain
their typed failure explanation. Derived operational codes are recomputed from
current receipts on continuation; intrinsic deadline/contract outcomes retain
their owner. This changes neither fulfillment nor recovery budgets.

### Search coverage and stopping decisions

`search_coverage.py` builds a bounded source ledger from adapter-owned `source_candidates` and native `browser_item_collection` records. Nonempty adapter-declared `source_record_collections` also supply observations, including structured detail reads without a text field. Declarations must resolve to records with their declared identities. Failed calls, empty records, navigation alone and compacted identity inventories do not supply content observations. A captured collection observes its document, not each linked target. It preserves exact query/fragment URLs, treats navigation separately from observed content, and stores the ledger in the goal-bound workflow checkpoint. Model-facing source context is rebuilt from full retained receipts after compaction; restored observations alone cannot authorize completion. Adapters may explicitly declare `canonical_source_url` without certifying any date or filter. Native media/download adapters declare `observed_url_fields` on source-record collections only for actual fetched resources. Screenshot crops declare the captured page URL from the native screenshot receipt; requested pages and linked asset URLs do not establish observation. Distinct deliverables bind distinct native artifact records, while their supporting source records may be shared. Reusing the same primary file across independent deliverables remains invalid.

Comparison plans select `comparison_mode=bounded` for practical recommendations among verified options, or `exhaustive` when the request requires complete coverage or a global optimum. Legacy plans retain exhaustive source checks. A bounded assessment binds every selected option and benchmark to current positive observations, verifies all material requirements and filters, and explicitly discloses its claim scope. Its typed `frontier_candidate_ids` identifies discovered contenders that could credibly change the ranking; each must be checked or excluded with evidence before `ranking_complete` can close the comparison. Incidental unvisited listings remain pending, not certified irrelevant or worse. No fixed candidate count determines completion. Named required subjects, negative conclusions and exhaustive comparisons keep their coverage requirements, including discovery truncation limits.

A non-idempotent upload has distinct acknowledged, rejected, and unconfirmed outcomes. `delivery_acknowledgement.py` binds progress to actual SDK replies and stops a batch when an acknowledgement is lost. Read/write/protocol failures do not authorize replay; an established pre-connect failure or explicit retryable rejection may retry. Provider SDK error normalization stays in the transport adapter. Telegram scheduled, operator, interactive and callback document uploads share media timeouts. Slack and Discord uploads use the same acknowledgement rules. This policy covers artifact types through their existing descriptors, with no filename, account or content matching.

An unconfirmed/partially acknowledged send propagates through the cron LangGraph and persists a receipt with confirmed, uncertain and unsent paths. It never becomes a successful upload or a definitive statement that nothing arrived. The delivery owner may send one status notice, without automatically replaying the uncertain upload. A confirmed SDK response remains authoritative if later client cleanup fails. Durable group delivery stores an in-flight item before calling the transport; restart with an unacknowledged in-flight item becomes unconfirmed, rather than repeating that item. This avoids blind replay, but cannot promise exactly-once remote acceptance when a platform supplies no recoverable acknowledgement.

A numbered choice attached to an owned pending request with a registered
effectful tool uses the existing structured scope decision with that request's
context. It bypasses the safe-read preflight, which cannot establish a mutation
scope. This is a gated replacement for that preflight, not a classifier on
ordinary turns. A decline must retain its selected no-action scope; blindly
restoring the parent's mutation tools would recreate the stale action duty.
Existing artifact-format follow-ups retain their dedicated typed route. Historical tool context remains visible as evidence, but cannot widen or replace this validated current-choice scope with an older inventory or mutation duty.

The same acknowledgement policy covers non-idempotent text sends. A format
fallback requires transport evidence that the first message was not accepted;
a lost response cannot justify a second plain-text message. An uncertain final
stream edit cannot fall back to a new final message. Live-tracker and Doctor
notice owners persist an unknown acknowledgement and do not replay that notice.
Reminder delivery keeps its durable claim on an unknown acknowledgement, records
`reminder.delivery.unknown`, and does not send an origin fallback to the same
uncertain target. Acknowledged external alerts remain acknowledged if secondary
history persistence fails. Outbox failure notices are attempted once across
recovery, independently of retries for explicitly unsent result files.

Transient tool resources belong to one shared chat request, including all of its recovery passes. The request's outer `finally` releases its browser tabs after success, failure, cancellation or suspension, independently of Code Mode. Ordinary research must never lease tabs to the whole conversation or retain them merely because `browser_open` ran. Browser profiles and authentication persist independently of transient tabs. Runtime-declared long-lived owners, such as active trackers, and pages intentionally opened for the user retain their separate leases. Failed close attempts retain exact ownership and get a bounded retry; a missing session yields a zero-count receipt, not proof that every browser tab closed. A close-only scope never creates a navigation requirement.

Explicit `/stop` additionally releases the stopped conversation's retained tool-resource leases after cancellation. The shared stop path scopes cleanup to `conversation:<id>` on all chat surfaces; it never requests global cleanup or closes another conversation's leases. Repeated stops are idempotent.

Scheduler follow-up context retains a bounded identity/timing index alongside sampled detailed rows. The index preserves exact names, selection identities, enabled state and human-readable schedules from the observed source page, allowing a wording correction without repeated reads of full task instructions. It reports index completeness and source-page continuation independently; omitted details and unseen source pages never become verified by compaction.

External search constraints are separate from execution restrictions. Restrictions
on purchases, account writes or file delivery are verified against the run's
material requirements and execution audit, not by repeatedly reading an external
page. Model-facing source-constraint assessments require a citation when claiming
verification; an empty citation list cannot establish an external fact.

A positive finite shortlist can use `source_discovery_required=false` when its
requested records and properties are verified within the observed inventory.
Finding several qualifying records does not alone create an obligation to search
additional providers. Explicit provider coverage and broad absence conclusions
retain their wider discovery requirements. Browser evidence presents an explicit
`control_index` on each captured control; this index binds the stored capture and
is separate from browser automation action indexes.

Live semantic conditions can explicitly select snapshot comparison. Setup tests
the complete prepared source and seeds the same durable comparison record used
by background polling. Each successful check replaces that record; unknown or
failed checks preserve it. The record is bound to the probe and its adapter-owned
source prerequisites, so changing the source starts a new baseline. First
snapshots never generate change alerts, and resuming a comparison does not
discard its first genuine later change. Collection identity tracking and
structured comparisons retain their existing behavior. Current and previous
snapshots share the existing evaluation packet budget without clipping.

Browser locator previews expose typed text truncation metadata on both browser
backends. Semantic monitor admission rejects incomplete previews before storing
an active tracker. Complete text reads declare their independently observed URL
as a bounded, read-only replay prerequisite, allowing a background worker to
restore or refresh its own tab without inheriting a creator's physical handle.
Repeatable full-text reads additionally require an observed readiness selector
or stable label. The adapter verifies it during preparation and repeats that
bounded wait in the worker. A loaded page shell is not ready source data, and
the controller does not substitute a fixed sleep or a site-specific detector.

A later status confirmation preserves the selected object and consent limits,
while distinguishing a constraint on Nullion's own mutation from an account-wide
historical comparison. Exact-target mutation receipts establish the objects the
assistant changed; current read receipts establish their present state. Neither
proves that no other actor changed unrelated objects. Explicit historical audits
still require baseline or history evidence.

Scheduler inventory queries include task IDs as well as names and stored
instructions. Inventory-version mismatches remain retryable argument repairs:
the correction retains the original selectors and page size, resets the cursor,
and supplies the current version. Compact scheduler context preserves typed
recovery, permission, and terminal-action fields. It must not turn a recoverable
snapshot change into an unavailable reader or a valid empty inventory.

Browser SDK reports retain the actual captured document text and control state separately from accepted observations. The shared compact source view includes a bounded index with exact source URLs and JSON pointers; the existing retained-evidence inspector can read an earlier snapshot after navigation changes. Rejected or incomplete report claims remain rejected. Captured documents are internal source evidence and never become delivery attachments merely because they were retained.

When a file inspection uses an unknown absolute path outside its current workspace roots after producing artifacts of that format, the shared artifact lifecycle offers the exact retained paths once. It does not guess an alias, inspect the unknown resource, or grant access. Declared original source paths remain valid after a delivery copy. A corrected call uses normal file policy; an intentional retry of another path also uses normal policy. Ordinary reads without matching retained artifacts and reads inside existing roots take the existing fast path.
The orchestration graph and LangChain/Deep Agents adapter use the same reference recovery. Typed repair instructions remain structured in delegated tool results, and a repairable argument error does not dispatch an alternate tool with the same invalid arguments.

Scheduler inventory tools declare `scheduler_inventory` metadata and a single-kind `inventory_scope`. Selecting an inventory read exposes the other registered inventory reads without loading their records or enabling mutations. A display label does not establish whether an existing task is a cron, live tracker or reminder; an empty result covers only the queried kind. Reading a live-tracker inventory also does not pin the conversation to live creation/mutation mode. Reminder reads are workspace-filtered, and reminder writes use the same workspace-access checks as scheduled tasks.

### Conditional scheduled reports

Cron expressions use the job's stored `schedule_timezone`; a New York schedule
therefore keeps its local time across daylight-saving changes. A structured
`execution_window` with `mode: daily`, `timezone`, `start_time`, `end_time`, and
`weekdays` can additionally prevent early manual or delayed runs from performing
work outside a permitted local interval. Weekdays are Monday=0 through Sunday=6;
intervals include the start and exclude the end and do not wrap midnight. The
scheduler remains enabled outside that interval, and no provider is called.

Report recovery uses the existing composition call to evaluate explicit saved
no-delivery conditions. A withheld result must quote the saved condition and cite
current runtime or successful source evidence in a request-bound schema. It
produces no placeholder artifact, no automatic research continuation, and no
scheduled notification. Failed sources and uncertain conditions do not establish
a no-delivery outcome: useful partial reports remain deliverable. Manual runs
receive a brief explanation when a condition prevents a report. Recovery metadata
stays internal and does not introduce another presentation layer.

### Compact scheduled report presentation

The same contract passes through delegated scheduled tasks and grouped report
summaries. Transport adapters only render, escape, split and deliver the resulting
message. Attachment cleanup preserves paragraph and list boundaries. Invalid
presentation metadata falls back to usable public content without another model
call or a new publication gate. Plain chat and legacy plain-text reports retain
their existing paths. Artifact contents and completion decisions remain separate
from presentation.

Source-answer grouping retains explicit title and field selections across a
cosmetic repair for the exact same evidence set. A grouped point's advertised
schema requires title identities. Repair diagnostics list compatible captured
roles and same-capture entity anchors; they never reclassify source records or
join different snapshots. Partial recovery first keeps an independently valid
quoted field group together before separating incompatible records. Specific
selected source limitations remain visible without an additional generic assembly
failure note; completion stays false when answer preparation failed.

This is a presentation repair path, not independent factual verification or a
guarantee that a model judgment is correct. Missing actions, source observations
and files still require execution or a concrete limitation. Ordinary requests
gain no extra classifier or model call.

The browser adapter can report pending controls by captured source/control
identity. A current enabled prerequisite keeps a partial browser step resumable,
even if its report requested termination. Disabled, stale or unknown control
references do not cause continuation. This signal permits neither purchases nor
automatic UI mutations; the normal browser executor and permissions still apply.

A browser step may instead return `next_action=user_input` with a typed missing,
ambiguous or conflicting prerequisite. Its source id and exact quotation must
match the current captured document/control, including the final page capture.
The adapter then ends the SDK step and returns the existing shared numbered
user-choice contract, preserving partial findings without starting another source
search. Primary and browser agents share input-resolution guidance: use saved
profile values first, verify the meaning of selected values and repair conflicting
selections before interpreting results. This is not a vendor-specific address
resolver or an additional classifier/reviewer, and a typed clarification does not
grant permission to act.


Native-tool report corrections do not require a browser source-selection
verdict. The completion parser receives whether a source-answer selection
actually exists; a null verdict is valid when it does not, while an explicit
false verdict or an unverified actual selection still rejects the correction.
Required outcome coverage, positive receipts, consent and artifact verification
remain independent. Diagnostics distinguish that source-selection mismatch
from other correction coverage errors.

The shared report contract also carries optional root `extra_details` for report-wide
explanations and methodology. The common renderer includes that expansion only in
detailed mode, including the saved detailed preference for a `default` report.
Required outcomes, amounts, dates, action exceptions and material uncertainty stay
in the visible item fields. Common action totals appear once; the footer is not a
second summary or verification narrative. Original generation, recovery, delegated
reports and grouped summaries share this contract without another model call,
prose classification, output truncation or changes to artifact contents.
Synthesis-only scheduled helpers receive the same contract when their producer
has typed report metadata. They may replace that report with a valid shared
`report_message`; an unstructured or malformed editorial rewrite preserves the
producer's existing answer. Group summaries retain report ownership after a
cosmetic `content_fallback`, so a later summary does not discard the policy.

`TurnResult.prepared_answer` retains the composed public answer across delivery
boundaries. Its request, text, source and selected-file binding must still match
before a transport preserves it. It is not a fulfillment or upload certificate.
Fallback delivery cannot replace that answer with generic grading prose.

`completion_pipeline.CompletionCheckpoint` binds a producer terminal decision to
the original request, final answer, execution/approval state, tool receipts and
selected file bytes. Common chat recovery, structured continuations and history
fallback consume that same checkpoint. They cannot restart a published partial
answer with a fresh budget. Changed requests, drafts, receipts, approval state or
selected files invalidate it; transport annotations do not. Registered unfinished
actions remain unfinished. The checkpoint controls ownership only: it grants
neither fulfillment, permissions nor delivery acknowledgement. Scheduled report
execution publishes its checkpoint after report composition, not at the earlier
research-to-report handoff.

Producer inspection calls in one model response share the existing 24,000
character view budget. Unshown windows return `budget_exhausted` descriptors and
remain available on subsequent responses. Fresh exact windows are preserved;
the retained index does not repeat those same windows. Archived previews and
rebuilt inspection windows share the model history budget. These limits affect
the model view only, never original receipts, exact facts or tool-call counts.

Scheduled report prompts delegate presentation policy and the report schema to
the common `chat_text` contract. Only scheduled title ownership and execution
context remain in the scheduler wrapper. Compact reports place provenance once
in sources, routine shared outcomes once in shared details, and process/audit
narration in optional expansion. Required decision facts, discrepancies and real
coverage gaps remain visible; detailed reports keep optional expansion.

Runtime persistence and scheduler sidecars share one OS file lock per canonical
database path. Nested calls on the same thread reuse the outer ownership rather
than opening another handle and waiting on themselves. Only the outer context
releases that lock, including on exceptions. Separate threads and processes still
acquire the OS lock; forked children cannot inherit the parent's reentrant
ownership. This applies to full checkpoints, scoped writes and scheduler leases
without changing their SQLite transaction or scheduling policies.
Secondary chat and scheduled-delivery receipts share bounded foreground writes
and a coalesced retry worker per database path. SQLite busy/locked errors defer
these exact rows without replacing an already prepared response or replaying
tools. The worker retains the originating store and rows across runtime reloads;
primary actions, permissions and approvals keep their existing durable-commit
requirements. Corruption and other permanent foreground storage errors still
surface rather than being misclassified as contention.

Loading or validating persisted conversation events builds the history indexes
with execution observers suspended. Restoring a snapshot must never publish old
diagnostics as new tool/model work, write them back through the current cron's
sink, or replace their run identity. Live observation resumes after hydration,
including when invalid history raises an exception. Scheduled sinks also reject
events already owned by another job or run.

A lease-based interruption is provisional health evidence. A later terminal
receipt for that same execution replaces it, including an approval pause; it
cannot overwrite a newer attempt's status. A task waiting for budget approval
must not remain classified as interrupted after its pause is durably recorded.

Activity rows use the shared run-activity renderer on every surface. Only
allowlisted structured summaries, such as message/event counts and a deferred
run receipt, supplement the tool name and status. Arbitrary tool output, raw
errors, URLs, filenames, and internal repair instructions remain in execution
evidence rather than being copied into activity cards or final reply traces.

Native extracted inventories carry complete extension totals from the actual extracted
entries, excluding skipped metadata and unsafe paths. The JSON source and compact
source handle retain the same totals. Historical tool lookup preserves bounded
source handles, record counts, and valid totals without expanding their private rows.
These remain references to the selected past observation, not fresh action authority.
A workspace search count must not substitute for an owned inventory
when preparing a later export.
Source handles take priority over expendable recent tool previews inside the
existing bounded history budget, so later reads or writes cannot erase them.
This work runs only for an explicit history lookup and adds no model call or
filesystem scan to an ordinary chat turn.

Scheduler inventory identity guidance survives both primary model projection and
repair projection. Full names retain distinguishing suffixes; detailed row
sampling must not shorten names or stand in for the complete typed page index.

Web active and archived history share one restore renderer. Stored
`runtime_turn_id` metadata restores the visible bubble's identity and completed
turn registration. Reconnect and terminal delivery can therefore recognize the
existing reply rather than creating a second bubble after reload.

The Browser Use adapter bounds SDK awaits without waiting indefinitely for SDK
cancellation handlers. Its execution budget includes initialization time; final
inspection and detach remain separately bounded. The isolated worker atomically
publishes its native report before Python drains SDK background tasks. The parent
can accept that report and terminate the remaining worker process, retaining
validated observations and incomplete-audit flags. A timeout or partial report
does not certify a complete lookup or turn an unobserved absence into a fact.
Owner cancellation still takes priority over accepting a late report.

A configured Browser Use CDP session is started under a bounded readiness check
before creating the agent. A typed timeout, including a timeout wrapped by SDK
exceptions, permits a temporary isolated-browser fallback within the same task
budget. Other connection errors remain errors. This does not change the saved
endpoint, reuse a failed connection, or start a model step before readiness.
Worker exceptions are also published before asynchronous shutdown, so a hanging
SDK background task cannot replace a concrete failure with an outer timeout.

A published browser report also survives a subsequent nonzero worker exit.
The parent reads an available atomic native report independently of process
shutdown status and retains concrete worker errors as errors. Owner cancellation
is checked again after exit, so cancellation cannot lose a race to late publication.
Missing/malformed reports still fail, and existing resource cleanup remains active.

Read-handler timeouts and empty search result sets declare invocation-scoped
failures. Recovery preserves the reader for other sources or queries, including
its visible scope receipt. Tool-wide outages retain their retirement behavior;
identical failed calls remain subject to existing bounded recovery.

The shared final presentation boundary applies the same sensitive-value redaction
before adapter delivery and durable chat storage. Redaction preserves Markdown
link boundaries, following URL parameters, and quoted secret delimiters, and is
idempotent across repeated delivery/history passes. No classifier or formatting
model call is added to ordinary turns.

Media settings changes refresh the registered media adapters as well as configuration metadata. Web settings saves and the shared Telegram/Slack/Discord service reload use the same refresh helper. Enabling generation exposes its tool without restarting; disabling removes it. The messaging service fingerprints media configuration and rebinds only when that configuration changes, preserving ordinary-turn and unrelated-setting fast paths. Existing file, consent and transport checks still apply.

Missing-file failure content is shared by fulfillment, artifact finalization and platform delivery. When a producer returns a typed HTTP rejection, each boundary delegates to the same actionable explanation instead of replacing it with a generic attachment notice. The latest failed producer receipt takes precedence over an earlier corrected-plan rejection. This changes failure content only; attachment requirements, valid-byte selection and completion status remain enforced.


### Source-to-output phase ownership

Artifact creation follows the validated output formats, independent of language,
clarification count, or chat surface. `ToolSpec.produced_artifact_extensions`
declares a registered producer's formats; source-only captures and account-write
tools cannot become producers through that metadata. The existing scope and
request-recovery contract expose a missing output stage once per format set.
Discovery does not execute tools or bypass executor permissions. Read-only
existing-file requests remain read-only. When no dedicated producer is configured,
the existing registered executor may be selected under its normal approval rules.
Research receipts and constraints remain in the same request; source screenshots
are never substituted for the requested generated artifact.

Tool-scope receipts explicitly identify unavailable requested capabilities.
Source compaction retains native delivery manifests and producer receipt identity,
including terminal failure causes. Missing-output failures distinguish generation
from upload, and platform adapters
preserve the shared terminal failure explanation rather than replacing it with a
generic attachment notice. The same delivery selection and presentation boundary
continues to serve Web and messaging surfaces.


### Context compression integrity

Compaction changes model-facing views; request-owned native source records,
action receipts and artifact bytes remain unchanged. Source observations use
their typed compactor before generic collection projection, so large captured
pages cannot displace accepted quotations, values, links and uncertainty.
Recompacting a source view preserves its exact capture inspection references.
Filename references defer output-versus-source selection to the primary model's
structured tool scope, without a separate attachment-format classifier call.
A quoted source-delivery request and one exact native filename match can select
the existing file for delivery after root, existence and descriptor validation.
Ambiguous matches and scopes that decline delivery cannot select attachments.
Code Mode wrappers do not change native filename-discovery classification.

Read-only artifact retrieval linked to a prior turn defaults history lookup to
that conversation. An explicit workspace scope or saved-turn id still supports
cross-conversation lookup. This uses the validated scope and linkage state,
without interpreting phrases or adding a classifier to ordinary chat.

Focused artifact packets contain bounded structured previews and explicit
omission paths rather than cut JSON strings. They retain invocation identities;
the common producer context exposes all retained records, including records
outside the preview sample. Producers may inspect required omitted fields with
the existing read-only retained-evidence tool instead of fetching the source
again. Current inspection protocol pairs and user image/document inputs reach
the consumer intact. Completed producer history is retired only using native
invocation-to-protocol mappings, preserving failed attempts of the same tool.

Delegated fallback summaries declare sampled result counts and omitted fields.
Opaque ids, source cursors, paths, dates and URLs remain exact or the containing
view is explicitly unavailable; shortened handles cannot become new tool input.
These are model-view and recovery rules, not another grader or permission grant.
Plain turns gain no model call, source fetch or broad database scan.

### Request-owned Code Mode execution

Research programs return selected findings, dates, regional scope, supporting
passages, exact source URLs and unresolved questions together. The model view
prioritizes a compact program value over duplicate native read inventories;
full receipts remain available under their original invocation ids. Calendar
adapters declare schedule record fields so bounded observations retain their
times and exact identities without copying lengthy optional notes. Search
snippets discover candidates; verified claims require underlying source evidence.

The primary's structured scope decision also owns output and media format
selection. Preparation does not run separate format/media model calls or scan
all scheduler jobs for ordinary primary-agent turns. Explicit reply/job state
continues to identify scheduled work.

Designed PDF producers expose up to three bounded previews rendered from the
actual PDF bytes, bound to both PDF and image hashes. These internal previews
reach the existing composer, including through Code Mode; they are not delivered
as attachments and rendering does not certify visual quality. PDFium rendering
is serialized because its native API is not thread safe. Images preserve their
full frame unless the author marks decorative art with `data-nullion-crop=true`.
An inspection read cannot revoke a producer's embedded-media receipt or cause a
replacement PDF. Shared attachment presentation supplies a readable caption if
cleanup leaves only an icon or punctuation.
Media checks use the latest completed producer at each path. The shared delivery
manifest may select a separate plain memo beside an illustrated document without
causing another image-only fallback file.
`pdf_create.card_pages` accepts bounded page titles, subtitles and content cards
with optional local images. It renders the same designed-PDF pipeline without
requiring the model to generate repeated CSS, fixed-height panels or oversized
brand artwork. Custom HTML remains available when the design needs it.

The shared runtime retains the existing direct executor and an optional Monty
Code Mode interface. `NULLION_CODE_MODE=on` with the `codemode` installation extra
(`pip install '.[codemode]'`) exposes `code_mode` for scoped reads and writes
across tool families. There is no gatekeeper module, per-tool redirection,
intent classifier, or compilation of native tool calls into wrapper programs.
The model produces a program when the sequence is understood; every `call_tool`
uses the existing native execution boundary, including source consent, security,
action review, approvals, cancellation, artifacts and activity. Scope changes
remain explicit native controls. The sandbox cannot grant or widen permission.

One request owns one logical Python session. Variables survive reasoning turns
through trusted, bounded Monty checkpoints. A program pauses at the native call
that requires approval, retaining completed operation receipts and its exact
continuation. The shared approval path resumes that checkpoint, including when
the pending operation is a write. It rechecks the native action under the new
approval; earlier completed effects do not run again. SQLite compare-and-swap
claims arbitrate duplicate workers and survive process restarts. A crash or
unknown outcome leaves a non-replayable claim. Executable bytes come only from
the private host-owned journal, never model arguments or source content. Terminal
completion clears executable/source state and retains a small claim tombstone.
If a native operation attaches an in-memory store to its first SQLite checkpoint,
the journal migrates its existing sessions transactionally before the next read
or claim. Existing durable revisions win; attaching persistence cannot reset a
running or blocked claim or lose the request's Python variables.
There is no automatic retry of a failed program or write. A recoverable native
failure stops the feed by default. A program may explicitly use
`call_tool(name, arguments, on_error="return")` to handle a failed read or a
verified unexecuted operation and branch to a scoped fallback in Python.
At most three such failures are handled per program; approvals, denials and
uncertain writes still stop or pause. No failed operation is retried implicitly.
An unhandled failure checkpoints globals and exposes typed repair diagnostics
to the next reasoning step. A corrected program may continue that session. The
same-request retry contract carries the session id, and the shared chat request
owns heap cleanup through its final recovery pass. Composed diagnostics do not
promote optional repair capabilities into new completion requirements. Unknown write outcomes
cannot repeat the same native invocation; an unusable session ends the run
instead of cycling tool-scope requests or requesting more iterations. Progress
reports completed counts and active operations, rather than repeating generic
working-with-results text after every tool. The shared progress message is one
compact line with its current phase and at most three useful counts, including
search-result totals when available. Zero findings and internal failed-step counters stay out of that notice;
their native evidence and final limitations remain intact. Web renders the same
content in a muted 12px status bubble. Messaging adapters retain their clients'
normal text rendering, one editable notice and terminal cleanup.
Preparation phases distinguish task context, saved context, tool preparation
and the primary model's next step before native tool activity starts. Slow
messaging requests preserve the initial acceptance notice, then coalesce edits
to the newest phase; a completed source/tool fact may be the first visible view.
Transport-acknowledged progress sends and edits contribute typed phase marks to
the existing surface latency record, without source text or a new model call.

Memory refreshes and normal checkpoint batches resolve their encryption cipher
once per consistent batch, rather than accessing key storage for every row.
The cipher stays local to that operation; snapshot revision invalidation,
owner filtering, authenticated decryption, corruption handling and subsequent
key rotation remain in the shared persistence path. Startup warms the existing
revision-aware memory snapshot without reinforcing memory or changing consent.
Ambiguous free-text follow-ups retain their existing relationship/permission
checks; a warm cache or displayed progress phase never grants tool authority.
Terminal tool-availability failures
remain unavailable for the current request, including through scope refreshes,
rather than being retried with different arguments. Image adapters retain HTTP
authorization/payment refusals as provider-availability failures without
producing a placeholder image. New requests can recheck availability after
the user resolves the provider condition. When a model chooses a revised
production plan, runtime-verified unavailable file producers may be replaced
by an explicitly selected local producer. Requested formats stay mandatory;
account actions, source-only retrieval and consent requirements are preserved.
Actual output bytes and the normal delivery contract must still verify the
replacement. Native control/scope-recovery receipts never substitute for the
failed operation receipt. Programs without a final expression return bounded
native observations and exact inspection references instead of an empty result,
so the model need not reread a source merely to recover its contents.
Screenshot preconditions distinguish an untouched blank tab from a local
document populated by the current workflow. Playwright and CDP attest actual
DOM content before capturing an `about:blank` document; a fresh empty tab still
fails with the loaded-page repair contract. Rendering local content therefore
does not require navigating to an unrelated external site. Artifact role,
selection and delivery verification remain separate checks.
Failed browser navigation also waits for the native error-document DOM event
within a one-second bound before returning its original error. This prevents
Chrome's late error redirect from aborting an immediate in-program fallback;
it never retries the URL or creates a replacement session.
Read-first prerequisites accept a verified native source error/timeout as an
attempted read. The browser adapter records that exact attempted URL for the
same request, allowing another controller without duplicating the failed read.
Consent, policy and argument rejections do not satisfy these prerequisites.
An assistant argument cannot change a configured desktop browser to headless.
The native browser boundary requires either an explicitly headless deployment
or a numeric reply bound to a stored native recovery choice and its exact parent
turn. The shared chat pipeline carries that request-owned grant across clients;
an unrelated turn or a website's proposed choices cannot authorize it. Rejected
mode selections leave the configured profile/session intact and return a typed
correction to use `mode="configured"`.

The Browser Use adapter estimates startup event pressure from page/tab targets,
which emit tab lifecycle events. Iframes and workers do not count as tabs. An
actual capacity skip is reported as such; it is not an attempted attachment
failure. Startup still has a bounded deadline and retains the managed browser
and its login if another controller must continue.
The native live-stage harness checks that a configured desktop endpoint belongs
to the selected macOS browser before validation, rejecting Docker/proxy owners.
It also fingerprints browser settings and fails if validation leaves them
changed. Explicit remote/fixture validation requires a process-local opt-in;
the harness never persists that override or restores over concurrent edits.

LangChain/Deep Agents use the same composed tool interface and native permission
boundary. Adapter fallback metadata cannot escape a program's selected tools.
`ToolSpec.code_mode_allowed=False` excludes native control operations; other
scoped tools are eligible in `on` mode. The transitional `read_only` setting
permits reads and explicitly opted-in native UI controls only. Adapter-declared
observation prerequisites are checked against completed native receipts before
broad controller calls. No rules infer user intent from prose.

Monty 1.6 runs locally without filesystem mounts or direct network access.
The session registers lazy, explicitly selected CPython host modules for pure
URL encoding/query helpers (`urllib.parse`), HTML escaping (`html`) and bounded
text layout (`textwrap`). The model sees the exact export manifest and can use
`describe_module(name)` for signatures. Normal imports and saved function
bindings work across feeds and approval restarts. Whole Python module objects,
arbitrary package loading and I/O functions are never exposed. Unique export
names such as `quote_plus` have direct bindings; shared names such
as `quote` require a module import. These bindings use the same bounded callable
and canonical accounting as imports. Binary return values are represented as
typed base64 data at the JSON result boundary; saved VM values remain bytes.
Host helpers have separate call, input/output and expansion limits; native calls retain their
existing scope, consent, approval and artifact contracts. Other installed
libraries run through an explicitly scoped native tool in the same session.
Unhandled missing-module errors return typed availability and recovery facts.
Two unsupported-import failures spend the request's durable import-recovery
budget and end truthfully instead of cycling scopes or extending iteration
limits. Completed writes are retained and never replayed. This policy is shared
by the orchestrator and LangChain/Deep Agents and adds no model call or package
inventory scan to ordinary chat.
Programs that only transform saved data or use pure helpers select
`tool_names=[]`; this never authorizes a native operation. A live session keeps
that data-processing interface even when no native tool is currently callable.
Invalid program envelopes return exact field, size and scope constraints through
the shared `correct_arguments` contract. Three consecutive validation failures
end the request instead of consuming the model's entire iteration budget; a
valid program resets that streak. Scope controls remain separate native calls.

Recovery follows Monty's [exception guidance](https://pydantic.dev/docs/monty/quickstart/python/)
and [snapshot contract](https://pydantic.dev/docs/monty/concepts/snapshots/):

| Failure | Session handling | Action handling |
| --- | --- | --- |
| Syntax, type or ordinary runtime error | Preserve safe globals; return typed exception and source frame positions | At most three consecutive failed corrections; a completed program resets this streak; never replay an earlier feed |
| Unsupported import | Return the host-module manifest and native-library recovery route; spend the durable import budget | Stop after two unhandled missing-module errors |
| Native tool failure or approval | Stop or pause at the exact native call; retain a trusted continuation only when the feed ends safely | Apply the native repair/approval contract and preserve receipts |
| Timeout or memory limit | Discard the heap, even if the worker still responds | End truthfully; retain completed action receipts |
| Worker loss, mid-execution conversion failure or failed checkpoint restore | Mark the session unusable; no automatic replay or restore from the failed heap | Keep confirmed/unknown outcomes distinct; do not repeat writes |

Host helper exceptions use Monty's exception channel. An unknown external
function becomes a correctable `NameError` naming the function, never a new host
capability. Sandbox guidance requires string exception/assert messages rather
than unsupported structured exception arguments.
Resource failures while draining a failed feed also prevent checkpoint reuse.
Only runtime-owned, compatible checkpoints may be restored; suspension state
and native approval receipts remain authoritative across restarts.
Retained-evidence inspection is offered inside Code Mode when the current
request has omitted native fields. It uses the existing inspector, ownership
validation and shared 24,000-character view budget across the program's calls;
it performs no new source reads. A native program whose arbitrary return value
exceeds its return allowance first projects typed source findings; an arbitrary
oversized value retains its execution result and native receipts, with an
explicit omitted-value diagnostic and bounded native observations.
Data-only oversized values still require a smaller return value. Neither path
replays the program or certifies completion of the user's requested outcome.
The native registry attests availability failures of local file producers
separately from account actions and source-only tools. Once the final contract
accepts an alternative's actual file bytes in the matching requested format,
the shared outcome layer binds that success receipt to the optional failed
method. A stale provider billing warning then cannot contradict a completed
delivery. Required methods, account effects, approvals, absent files and
unrelated successful tools remain unresolved until their own contracts pass.

Execution time, memory, source size, native call count and transferred data are
bounded. Programs use native APIs for file generation, account changes and other
writes, and all calls preserve the original native receipt identities. Compaction
retains operation receipts and exposes exact inspection references for omitted
large values. Both execution interfaces share the existing presentation layer.
Large program values also retain a bounded host-owned view of native findings,
with exact invocation references. Accepted facts and selected control values
precede optional selector catalogs, document text and capture inventories. The
program's arbitrary return value cannot manufacture those native observations.
The shared delivery boundary preserves a verified approval pause ahead of file
captions, and preserves the native action receipt after approval. Preparing an
attachment cannot replace either the pending action or its completed outcome.
The orchestration deadline excludes time spent inside native tools, whose
existing execution deadlines and cancellation remain authoritative. Timing
receipts separate native wait from orchestration work. A valid slow source must
not fail the next Python statement after its native operation has completed.
Parallel CDP reads exclude tabs with an in-flight close acknowledgement from
tab assignment, preventing another read from inheriting a closing target.

The default `off` configuration preserves the old tool interface and executor;
a missing optional dependency also keeps it available. Plain chat starts no
worker, performs no checkpoint IO and gains no model call. Disabling Code Mode
blocks stale program selections. No Node service is required. Stage changes and
live acceptance are performed only after local verification and outside active
user testing.

### Native delegation preflight and saved local defaults

`browser_run_task` owns its request-bound native read preflight, including when
called first from Code Mode. It does not advertise a duplicate outer read
prerequisite that could reject the program before its preflight runs. JavaScript
page execution retains its observation prerequisite. A failed preflight records
that delegated interaction did not execute; cancellation, source errors and user
choice still prevent the adaptive worker from starting.

The shared profile supplies the default local location across chat surfaces. An
unrelated trip destination does not replace it. Current explicit locations and
linked-task locations take precedence; independent-task and short-message guards
retain profile defaults and ask only for information that remains unresolved.

Native detail reads retain visible, scope-bound anchor labels and exact resolved
URLs alongside their record, with explicit link counts and truncation metadata.
Single and batched detail extraction use the same DOM program, so choosing a
detail view cannot hide item links needed for subsequent verification.

Shared comparison guidance distinguishes a quoted billing interval from actual
usage duration. Derived effective rates use the verified usage quantity and
retain the original source rate basis, rather than treating a weekly/monthly
price label as proof of elapsed time.

Explicit `keep_open` on browser open/navigation promotes only a successful
session to a conversation-owned lease using runtime identity. Request cleanup
still closes ordinary research sessions. Follow-up capture retains the original
page state; browser close, conversation stop and shutdown release the lease.

Native failed-process captures carry an invocation-bound incomplete-observation
receipt. The primary can publish its authored partial report with a native
limitation while fulfillment remains false. This ends redundant success nudges
only when captured operations are the entire missing tool contract; missing
actions and artifacts continue through their existing recovery boundaries.
Raw streams are never automatically appended to final messages.
