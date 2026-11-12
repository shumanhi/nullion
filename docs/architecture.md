# Runtime architecture

The chat surfaces share routing, tool policy, persistence, and result
presentation. Their transport and rendering code differs, so a passing Web
workflow alone does not prove Telegram, Slack, or Discord delivery.

## Shared presentation contract

`preferences.py` owns one `PresentationPolicy` for emoji, tone, length, structure,
Markdown and complexity. Existing chat, delegated-task, completion-review, report
repair and live-condition model calls receive that policy. It applies only to
user-facing prose fields, not tool protocols, evidence, identifiers or consent.
Presentation adds no model call to an ordinary request.

`chat_text.py` owns the common `UserMessage`, `ReplySection`, `ReplyFinding` and
ordered `ReplyBlock` contracts. Producers supply facts, headings and numbered
choices from typed state; this renderer applies the saved preferences. Live
alerts, scheduled headers, Doctor notices, reviewed findings, source answers and
merged results reuse this path. Legacy authored text passes through the same
final policy without guessing sections from wording or imposing fixed line widths.
Code, links, attachment directives and artifact bytes remain literal; styling
does not truncate findings, add completion requirements or discard grounded results.

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

The existing material-requirements planner owns the current request's output
contract. Its validated formats and evidence-tool families travel as runtime-owned
`ToolResult.response_requirements` metadata, bound to the request digest and
execution identities. Tool-output payloads cannot introduce that metadata.
This distinguishes an explicitly empty format list from an unknown contract:
an old scope guess or unrelated active frame cannot revive a file requirement.
Serialization preserves the same contract for Web and messaging delivery.
Grounded partial text remains available when a required file is missing, with
the missing work disclosed and fulfillment still false. This adds no model call
to ordinary chat and does not supply approval or missing action receipts.

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

1. The adapter identifies the authorized principal, workspace, conversation,
   reply anchor, attachments, and active task state.
2. Typed turn dispatch links an amendment or follow-up to the relevant task,
   or accepts an independent request. A conjunction in user prose is not a
   task-splitting rule.
3. A cached compact capability index gives the model tool awareness. Exact
   tools and skill/connector context are loaded when structured scope requires
   them; ordinary chat avoids a new broad classifier on every turn.
4. The orchestrator invokes tools through Sentinel capability and resource
   boundaries. LangGraph coordinates typed task state, recovery, and completion.
   Exact scope choices come from the current registered tool metadata. An
   unregistered mandatory tool fails scope validation atomically, so it cannot
   become an impossible completion obligation. Context-only turns retain a compact
   metadata index of registered routes, cached by registry revision and warmed at
   startup, without loading full account inventories or adding a classifier call.
   Corrected structured scope may
   supersede unregistered names while retaining registered actions and consent.
   Source-scoped tool failures retain the tool for other URLs. Retries are
   counted by the exact source set, independently of argument or error wording;
   bounded recovery may widen capabilities without declaring that tool broken.
   A timed-out completion verifier retries verification once against the same
   prepared evidence. If it remains unavailable, valid selected files and the
   useful answer remain deliverable; review availability stays internal and
   does not restart generation. A separate bounded repair deadline prevents a large global
   turn budget from becoming an unlimited completion-repair loop.
   Terminal artifact failures carry the same final decision through outer chat
   recovery, which must not start generation again. Delegated source readers
   can request a bounded handoff of prior source steps and public search
   candidates. Those hints stay untrusted and cannot become new captured facts.
5. Structured evidence and artifact descriptors drive the visible answer,
   receipts, and attachments. Intermediate state files are not deliverables.
6. The adapter sends the result, preserving its conversation/reply target, and
   records delivery evidence.

Completion separates the producer's evidence-bound claim from optional review
advice. A current, valid producer claim survives an editorial-only correction.
An evidence-backed factual contradiction prevents that completion claim.
An unavailable helper does not invent unfinished research or restart tools.
This does not certify an unknown result: explicit remaining work, failed
composition, missing required receipts, and invalid source selections still
retain their independent failure state. A reviewer cannot override a failed
literal source selection without supplying a grounded corrected answer. Usable,
individually validated findings remain deliverable with their actual limitations.

An amendment received while a task is running updates the linked request.
Publication checks prevent an older response from being delivered after a newer
amendment. A status question does not authorize changing a saved job. A short
cancellation can use the recent verified job receipt; ambiguous candidates need
clarification rather than a guessed deletion.

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

Low-level browser tools keep session/page state and serialize operations on the
same physical session. A tracker must carry trusted setup context and the
verified physical session handle into repeated reads. Raw model aliases and a
new task scope must not silently choose a different page. A browser restart can
invalidate in-memory handles; source failure is distinct from a negative match.

The optional delegated Browser Use worker is a different workflow: it performs
bounded multi-step browsing in its configured isolated profile or dedicated CDP
browser. It is not the worker used for frequent live polling.

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

The same selected media reach the existing material planner, source composer,
completion reviewer, and artifact selector. A resumed approval restores media
from the captured request input, rather than the latest recovery instruction or
an unrelated older upload. Source images can identify the requested subjects;
they do not prove current prices, availability, external actions, or delivery.

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

Source answers distinguish real subjects from page controls and attributes, and
retain the verified files that are available for delivery. Completion review
checks the requested outcome independently from presentation quality. Internal
repair instructions and the reviewed user-facing failure explanation are separate
fields; exhausted recovery must not publish the internal correction checklist.

Separate captured records may share a display name, such as two departures from
the same carrier. Presentation deduplication compares the verified title and
details actually shown; a shared title or page is not a record identity. Exact
duplicate cards remain rejected.

Grouped excerpts select their exact source identities directly. The adapter
normalizes their redundant flat index before rendering and independent review,
without dropping explicitly selected fields. Unknown identities, conflicting
selections, duplicate groups, mixed captures, and invalid subject roles remain
rejected. Model timing records retain the exact visible tool names for incident
analysis without recording credentials or full tool payloads.

Composition repair receives the rejected structured draft and independent point diagnostics together,
including missing summary citations and captured title roles. It uses the existing
single repair budget and the same evidence; it does not relax semantic validation
or restart source collection just to repair presentation.
The graph records when the advertised source-answer schema requires semantic roles.
Rendering enforces that typed requirement on initial and repaired drafts; omitting
role fields cannot downgrade a failed semantic check to legacy rendering.

A typed artifact format contract keeps supporting files out of the delivery
selection. Competing revisions within one format still require an explicit final
selection. Terminal creation scopes use the current workspace artifact directory;
ordinary shell inspection keeps its configured working directory. Web downloads
must resolve through permitted artifact roots, and a missing download must never
be reported as an attachment success.

When several prepared files remain ambiguous, one bounded structured selection
uses opaque candidate ids and actual content previews. It cannot execute tools or
generate another revision. Runtime validation checks each selected file is
unchanged, records the delivery manifest, and independently reviews the selection
before transport. An invalid selection ends without sending arbitrary candidates.
Listing every created path in a draft does not resolve competing files of the
same format; those candidates still pass through the typed selector. The
selector can retain multiple separately requested outputs of that format.

Media repair feedback distinguishes a missing file from missing embedded raster
data and exposes current-turn reusable image paths. A selected linked/plain
report may accompany a separately selected, verified embedded report; an
unselected older image-bearing draft cannot satisfy that obligation. Repair
targets the existing output rather than recreating the entire package.

Completion review binds each selected file to a successful producer receipt and
its current content hash. One generator may produce several separately verified
deliverables; a file receipt does not establish a live source fact or an external
action. Bounded content previews include Office documents, spreadsheets, PDFs,
visible HTML text, and ZIP member text without extracting archive paths. A
terminal generator publishes typed output descriptors only for files changed by
that invocation and selected by the current artifact contract.

Source-backed artifacts expose bounded, runtime-bound references for individual
literal capture records even when the chat reply contains only a file receipt.
One browser invocation can then support several distinct researched subjects.
The reviewer still checks subject relevance and factual coverage; a file record
cannot prove source facts, and a repeated fact cannot count as distinct subjects.

Package constraints reference their declared deliverable members through typed
subject ids. The review checks that exact set against the prepared attachments;
it cannot reuse those file records to prove source facts or external actions.
File inspection reports embedded media and bounded decoded image checks without
placing image payloads in the chat context. Dedicated tools remain preferred,
while an explicitly selected core execution tool stays available for permitted
repairs under its normal permission and approval checks.

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

A transient completion-review timeout or invalid protocol response retries only
the same tool-free review once. An explicit output-token truncation can increase
that review response budget within the configured bound. It does not restart source collection or artifact production. An exhausted
review records its error type in the artifact manifest and retains the existing
unavailable-review delivery policy without claiming a successful content review.

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

Final artifact selections are bound to producer receipt versions. A repaired file
at the same path can clear an earlier media failure, but a later plain overwrite
cannot inherit the old media proof. A new producer version requires a fresh
selection and content review. Web delivery honors the shared terminal manifest
without calling a second format planner or reviving unpackaged source files.

Linked HTML reports expose their local image dependencies as structured metadata.
The file selector can return a typed packaging requirement to the producer. A
portable bundle contains only the report and its required relative assets;
working copies and supporting inputs remain outside the final delivery. A
runtime-validated packaging choice carries a request-owned continuation through
recovery scope rebuilding: it permits the archive helper only for the selected
page and its recorded local image dependencies. Model arguments alone cannot
establish that grant, and unrelated writes remain outside the recovery contract.
Finishing a bundle triggers another bounded final selection over the original
prepared candidates and the new bundle. Ready standalone siblings remain
selectable even when the producer omitted them from its draft attachment list;
packaging does not silently turn those files into missing generation work.
A typed packaging scope may replace an intermediate member format with the
bundle format while preserving separately requested standalone outputs. Both requirements planning and
content review have bounded output-aware deadlines; ordinary chat does not
enter either verification path. Content preview capacity is shared across the
remaining selected files, so an early long report cannot exhaust the entire
budget before later exports or bundles are inspected. The total preview budget
stays bounded and each deliverable retains its own file identity and review.

A later inspection is a read receipt, not a new producer version. Its referenced
path cannot invalidate the generating tool's image provenance. An actual rewrite
still invalidates that proof. When packaging changes the delivery format, a
separately requested, self-contained sibling can retain its original format only
through a validated selection bound to that exact path and producer version.

Quoted source records share review capacity across captured sources before spare
capacity is reused. Whole quotations and source fingerprints remain intact; the
review records how many records were omitted by the bounded evidence budget.
Budget omissions do not imply that a source was never checked. An adapter may mark an optional tool `explicit_scope_only`. Generic scope
expansion and companion-tool expansion preserve this boundary; only a typed
exact-name selection exposes the tool. Registration metadata controls this
without introducing a model call on the plain-chat path. Core terminal execution
remains discoverable and retains its normal permission checks. Browser task
success and execution-audit completeness are separate facts: a normal SDK step
limit can leave a complete read-only audit while the research task remains
partial. Interrupted history or failed final capture cannot claim a complete URL
inventory.
Text-only completion reviews also receive bound capture records before a
presentation exists. After a presentation checkpoint, new source parents from a
recovery retain their own bounded proof instead of disappearing behind the old
selection. Represented parents are not duplicated as extra evidence aliases.
Native DOM detail records declare singleton source collections, and collected
images declare their actual image records. Both participate in the same bounded,
exact-record evidence allocation as delegated source captures; compacting the
review cannot silently remove their provenance.

The independent requirements plan uses the original request and selected
reference context. Supporting filenames and helper scopes do not define user
requirements. A request-owned cache survives every LangGraph finalization return;
tool discovery, inspection, file revisions and packaging reuse it. Changes to the
request or selected reference pixels invalidate it. Runtime-bound factual records
from a newly used adapter can prove an existing factual subject; that exception
does not authorize actions or turn an output artifact into source proof.

A recovery also receives bounded, request-owned tool receipts in its model
context, after conversation/principal/request identity checks. Holding receipts
only in graph state cannot inform inference. Scope inventories are excluded and
serialized outputs stay inside the untrusted-evidence boundary. This recap grants
no new tools or actions. Optional link capabilities do not become required work;
link follow-up hints need an explicit boolean obligation before triggering a
recovery. Existing structured required reads still run through normal fulfillment.

When the independent plan declares requested output formats, the final reviewed
manifest carries those formats through transport. A supporting render cannot
become a required or delivered file just because a helper scope named its suffix.
Manifest scopes remain bound to current producer versions, and a parent selection
supersedes child selections. Scheduled HTML reports retain their existing delivery
review owner and do not acquire a second requirements planner.


Authorized raster image reads can supply a bounded, hash-bound pixel preview to
the model and its independent review. Preview availability and delivery are
recorded separately from visual interpretation; file metadata never certifies
image content. Source files remain inputs and are not attached merely because
the agent read them. A composed answer is preserved when those inputs are
suppressed from delivery.

A tool-owned preflight that executed nothing can receive one small correction
window inside the request's hard iteration limit. Permissions and destructive
action approval are unchanged. An invalid completion-review evidence binding is
instead repaired by one tool-free review retry against the same files and facts;
it does not restart research or artifact generation. Runtime diagnostics record
structured grounding errors so failures can be traced to their actual boundary.

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


Generated deliverables proceed to artifact selection and independent source/content
review instead of a forced, separate chat-only source summary. Explicit source
captures retain their concise receipt composition. A typed source-observations
report also triggers sourced-artifact review, including when its adapter is not
one of the built-in browser tool names. Source-answer context exposes rendering
limits; repair diagnostics report excessive detail length independently of
quote/group relationship errors so the single revision can address both.

If preflight rejects an overwrite of a current, natively produced artifact
package, the controller hands the retained files to this same selection/review
path once. It does not restart generation or infer completion from extensions.
The package must still satisfy the independent count, content, source, and
action requirements. Queued provider calls receive explicit non-execution
receipts during the handoff.


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


### Selected files, naming copies, and review recovery

Runtime file evidence is independent of whole-request readiness. The completion
review binds each selected existing output to its actual producer, size and SHA,
even when another deliverable or source check is incomplete. A pending content
review remains active after a successful repair; creation alone cannot skip it.
A typed selection may retain a plain memo beside an illustrated poster. Media
requirements still apply to the final selection, and a plain copy cannot replace
the required illustrated result.

A requested filename alias is a naming-only byte copy. Its runtime receipt carries
the original media provenance and moves only a current selected manifest decision
to that alias, preserving review status, producer versions and separately selected
siblings. An excluded draft cannot become deliverable through renaming. Later
writes invalidate the inherited review.

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

Portable linked-HTML bundles are inspected in place without extracting archive
members. The review sees the actual HTML entry, relative image dependencies,
missing assets and unrelated members, together with the runtime packaging choice.
Required dependencies count as package contents rather than extra attachments.
Creation contract facts such as a workbook's validated formula requirement survive
evidence compaction. Internal document rows and charts remain constraints of that
file, while separately requested live facts require their own source evidence.

A producer or repair iteration limit does not discard a completed artifact batch.
When a final version decision is still missing, the runtime permits one bounded
read-only selection using the existing file candidates, then applies the same
content and media verification. It does not grant more producer executions.
Successful verified delivery survives the limit; incomplete work retains its
failure outcome. Calendar query bounds, forecast coverage and current producer
validation facts remain available to completion review after compaction.

Native browser text extraction carries a schema-declared document record bound
to the URL observed independently under the browser session lock. Completion
review can therefore retain and cite the actual page text alongside collected
images, without treating a navigation receipt as page-content evidence. Local
workspace previews retain their separate source-file provenance.

Archive extraction previews retain a typed reference to the complete JSON row
inventory in model, review and saved followup context. A small entries sample is
never the full dataset. The ready manifest workbook can be selected without
reconstructing rows; transformations may read or process that full source through
normal permitted tools. Inventory references remain internal source data and do
not become attachments.

Web archive resets persist only the owned conversation's reset event and active
frame change through bounded SQLite persistence, without waiting for a full
runtime snapshot. The archive operation runs outside the Web event loop. Scoped
checkpoint rows use their real collection tables, task frames use frame identity
rather than branch identity, and late reset/frame writers respect reset times so
they cannot remove a newer frame or reactivate an older one. History-retaining
archives continue to expose saved chat evidence for a later fresh conversation.

### Read completion ownership and internal artifact review

A weather or market read completes a request directly only when the current structured
`request_tool_scope` decision explicitly sets `direct_read_complete_request=true`,
selects that one required read, and has no tracking or artifact obligation. Otherwise
the existing agent continues with the observation and can finish setup or further work.
No extra intent classifier runs on plain chat turns.

Completion review retains separate runtime-bound records for current internal working
files, including content previews, without adding them to prepared delivery receipts.
An internal file can prove a requested preparation step while the delivery manifest
continues to attach only selected final files. Superseded producer versions remain
audit history rather than proof of current file contents.

Live notification delivery persists the exact notification audit event with a bounded
SQLite upsert. Durable chat history and the live notice ledger own delivery receipts;
a full runtime snapshot must not block `finish_notice`. If the secondary audit write
is temporarily busy, the event remains in memory for the normal later checkpoint
without turning an already delivered notification into a failed send.

A typed `needs_packaging` selection transitions out of temporary content-review
focus layers while retaining the underlying scope, recovery and permission
contracts. Its next focus exposes only exact scope selection, archive creation
and local file read/write work. A successful exact scope request extends that temporary focus only with requested tools listed as available in its validated result. A new typed recovery decision replaces earlier temporary focus rather than intersecting with it. The underlying scope, request-recovery contract and executor retain permission and denial checks. This allows legitimate research, edit and execution transitions without granting a tool through metadata discovery alone. Repeated scope selection stops only when its last two execution contracts are equivalent. Packaging attempts have a separate two-attempt
bound; final version selection retains its own bound. This prevents both a
hidden archive tool after content repair and an unrelated generation restart.

Completed producer invocations remain available to review as execution-only receipts
when a later edit or verification supersedes their file version. These receipts can
prove that an explicitly required creation action ran, but cannot establish current
content, facts or delivery. Only the current selected file record proves those.
An in-place edit preserves the current path; supersession invalidates older versions,
not the successfully written replacement at that same path.

A typed artifact delivery contract with a visible producer uses the configured
artifact model deadline even while research tools remain available. Keeping source
access during generation must not silently revert to the shorter plain-chat deadline.
Plain turns keep their existing deadline and do not gain a classifier or extra model
call. Model timeout diagnostics record the deadline used at the failed invocation.

Structured source-answer preparation uses that same longer-output deadline,
including its existing bounded repair attempt. If a malformed cross-capture
selection cannot be repaired, recovery validates the model-selected records
separately against their captured excerpts and roles. It drops unsupported joins
and draft introductions, retains selected source limitations, and publishes only
the recoverable findings with an explicit partial status. Unselected inventory
and independently rejected semantic interpretations cannot enter this fallback.
An exhausted preparation result carries an evidence-bound terminal receipt so
outer review does not restart composition or research. Scheduled report delivery
retains its own review and artifact ownership. Ordinary chat gains no model call.
Per-model timeout copy is distinct from a true whole-request deadline.

Selected follow-ups carry `selected_request_context` from the same guarded turn
ancestry used for source attachments. Execution, source composition, action
preflight, requirement planning and completion review receive the original goal
and its intervening choices, independently of a platform's quoted-message
formatting. This uses stored turn identities, conversation/workspace ownership,
session-reset boundaries and cancellation/supersession state. A numbered choice
narrows the existing goal; it cannot silently replace requested properties or a
deliverable with a subject roster. Current revisions, cancellations and requests
for explanation remain authoritative. In-flight parent requests can contribute
their stored user input before an assistant reply exists. Context is bounded and
reports truncation; independent turns perform no ancestry history read or new
model call.

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

For linked follow-ups, the existing scope decision may select
`evidence_policy=retained`. This carries successful read receipts only from the
kernel-selected parent, with original timestamps and source identities, across
the shared model and review boundary. Resets, unrelated conversations, other
workspaces, failures, writes and scope grants cannot supply those receipts.
External read tools stay unavailable under this policy even after scope
widening or generic recovery: the request recovery contract preserves the same
observation policy when a retry builds another registry. Plain first-pass replies
receive the same bounded native receipts and original timestamps. Their saved
audit retains those reads without presenting them as newly executed calls.
Retained-evidence inspection and permitted artifact production remain
available. Fresh checks use the default `fresh` policy and acquire current
observations instead. This adds no classifier call to ordinary plain turns.

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

Web background checkpoint requests coalesce per runtime. A forced request arriving
during a normal save forces the next pass; it does not force every later pass.
Worker cleanup is bound to that worker's ownership token so a retiring worker
cannot clear a newer worker's pending requests. Failure to start a worker clears
its queue ownership so a later request can try again.

Telegram finalization retains ownership of pending draft sends even when the
short streaming wait times out. It drains those sends before deciding whether
to edit an existing message or send a new one, and applies final formatting to
the same message. An unsuccessful final edit withdraws the draft before fallback
delivery; a draft is also withdrawn when a decision or attachment owns the final
response. Typed already-delivered metadata survives response unwrapping.

Scheduled execution leases include process ownership. On POSIX, a verified dead
owner can be reclaimed before expiry; live owners remain protected. Legacy tokens
and platforms without a safe liveness probe retain their bounded expiry grace.
Startup reconciliation must preserve runs owned by another live process. An
interrupted owned run from the past 24 hours produces one durable, retryable notice at its configured
delivery destination, without waiting for the repeated-failure threshold. The
notice explains the missing result and how to request another run. It honors
notification opt-outs and does not replay actions automatically: effects completed
before interruption may already exist. No new model call is needed for recovery.

Artifact review distinguishes native-format layout facts from rendered visual
inspection. DOCX generation exposes an explicit `page_numbers` boolean for
dynamic footer PAGE fields; it defaults off and rejects incompatible output
formats rather than silently dropping the option. Native inspection reports
the actual `footer_page_number_field_count`, independently of producer claims
or body labels. Bounded DOCX/PPTX structure and PDF text geometry expose actual type
sizes, explicit colors, page/slide dimensions and out-of-bounds content. These
facts are tied to current file versions and do not claim pixel inspection or
perfect visual quality. A renderer is a verification method, rather than a new
universal action obligation; explicit visual inspection requests still require
visual evidence. An unavailable optional renderer must not invent a new required
action for otherwise verifiable native formatting.

A completion review's blocked decision does not by itself prove that source access
is blocked. If its typed coverage identifies an unmet fact and the matching
requirement still has callable permitted read tools, bounded recovery continues
with those reads and preserves already-created artifacts. Explicit source stop
receipts and executor denials remain barriers for their methods. This validation
uses structured requirement ids and registered side effects, adds no ordinary
chat classifier, and never treats a generated file as proof of an external fact.

Connector metadata resolution verifies only that a quoted source reference occurs
in the current or structurally linked user request. That quote alone does not
establish selection of a specific account or permission to read it. Resolution
receipts and bounded recovery instructions preserve the original request's
source-choice and consent requirements. The agent can resolve an explicitly
selected source from metadata, or return to typed connector discovery with
`source_user_requested=false` to request the numbered source choice. This uses
existing scope turns rather than a classifier on ordinary chat requests.

A packaged linked document does not remove a separately requested sibling of
the same format. Retained sibling identities remain eligible for fresh final
selection after a new producer version; their previous review proof is still
invalidated. Final selection and the delivery adapters use the same current
manifest restoration, so the transport cannot drop a newly reviewed sibling
because of an intermediate member-format exclusion.

Oversized scope results retain their typed source-selection, permission,
scheduler and artifact contracts in chat history, Web history and startup
maintenance. Optional connector catalogues are omitted from the compact
receipt rather than replacing the whole scope with a JSON text preview.
Source-choice options retain only compact provider labels and read-tool
metadata. A truncated source quote cannot retain a verified-evidence claim.

The native CDP fallback preserves evaluation semantics for JavaScript functions
and arguments. It obtains the evaluated remote handle, invokes a returned
function exactly once, serializes object results and releases handles with
bounded cleanup. Function declaration syntax is checked before expression
wrapping; ordinary programs retain their original execution. Protocol exception
details propagate as failures, including primitive thrown values. Independent
page-URL reads therefore produce actual URLs rather than serializing function
objects as empty results, allowing native extracted text to retain its source
record binding through artifact review.

Versioned runtime artifact selections remain authoritative after read-only verification. Later reply prose or a partial MEDIA list cannot drop a requested sibling or reopen a discarded draft. New producer versions still require fresh structured selection and independent review. Unversioned legacy selections retain their explicit-selection behavior.

Tools tagged `required_scope_only` remain unavailable when merely listed as optional alternatives. An exact validated requirement activates them without a new classifier call. Plain chat retains its existing fast path. Presentation cleanup preserves multiline answer bodies and facts identified by declared source-record identities; mentioning a tool in a data table does not make that table a disposable status paragraph.

Scheduled-delivery audit records and chat turns use exact bounded SQLite writes across Web and messaging. Busy writers retry captured receipts off the delivery path; they do not delay terminal job status behind a whole-runtime snapshot. Attribute evidence may serve as non-entity background context; interface controls still cannot be promoted to entities or background facts.

Current explicitly empty local read scopes supersede inherited writer and attachment-format guesses across the agent, task-frame fulfillment and transport delivery boundaries. Current generated artifacts and independently reviewed deliverables retain their own lifecycle. A verified filename-only answer remains intact. Metadata-only history scopes that omit an output decision do not cancel prior obligations.

Material fact requirements keep stable identities while accepting later completed source adapters with registered read/source-evidence metadata. This changes admissible proof, never invocation permissions or action requirements. Large source histories retain bounded declared identity paths without misrepresenting truncated content as complete source records.

Delimited artifact evidence reports actual CSV/TSV row counts and consistent column widths. Suspicious literal row escapes or ragged output fail structural validation. All tool-produced exports also receive the shared outcome review; intentionally empty templates remain valid when emptiness is part of the requested goal. A valid file is reviewed without rerunning its producer. Archive JSON inventories omit optional image cells for nonimage files, allowing mixed file inventories to be reused as structured spreadsheet sources without inventing missing-image requirements.

PDF text positions are measured at text-drawing operators. Extractor text buffers may flush after the graphics transform changes; using that later transform can falsely place valid text outside later pages and trigger needless regeneration. Font metadata still comes from native extraction; neither method claims pixel inspection.


Raw CDP navigation correlates document-ready lifecycle events with the returned
frame and loader identifiers. Queued events from older documents cannot finish
a new navigation. Same-document navigation uses its own event, and provenance
comes from the observed document URL, including redirects, rather than the
requested argument. Protocol reference: [Chrome DevTools Page domain](https://chromedevtools.github.io/devtools-protocol/tot/Page/).

### Bounded completion repair

When independent completion review finds missing outcomes, the execution graph keeps its existing bounded repair window. Each model response in that window receives a transient runtime checkpoint containing the remaining response budget, unresolved outcomes, prepared artifact paths, and browser session read state. The checkpoint does not grant permissions, mark source claims as verified, or extend the window. The agent should reuse opened pages, read the specific missing evidence, update the existing deliverables when necessary, and reserve a response for final delivery. Ordinary turns do not build this checkpoint or incur an additional model call.

Completion review uses short request-local references for long source and artifact record identities. The runtime resolves those references to the original immutable invocation IDs before validating coverage, record fingerprints, side effects, and per-subject bindings. Unknown references remain invalid; this protocol does not create evidence or change execution permissions.


### Compact tool contracts

When an answer explicitly needs exact local filesystem or executable paths, the
model's local tool-scope plan sets `include_local_path_details=true`. The runtime
accepts that flag only for `local_shell` or `local_files`. Formatting then
preserves only exact paths present in completed local read/command evidence;
unrelated paths and ordinary generated-file delivery keep their existing
presentation rules. The flag is request-scoped, requires no additional model
call, and must survive the review and Web/platform delivery passes.

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

### Shared request reasoning and outcome review

Outcome review is an assistant to the producer, not a permission, delivery or task-status owner. `review_assistance` applies a grounded correction or preserves the usable primary answer. Editorial disagreement and review availability remain diagnostic state (`outcome_review_role=helper`); they do not become failed execution receipts, hide valid files, replace findings with a grading paragraph, or generate failed-task alerts. A grounded clarification can accompany findings with numbered choices and preserve the unfinished request. Actual permissions, cancellation, required-action receipts, file validity, source selection, condition matching and transport acknowledgements retain their own checks.

The producer may take helper-requested improvements within its existing progress-based budget, retaining completed reads, actions and selected files. Group synthesis may suggest an explicit `execution_improvement` plan containing owned task IDs and their permitted tools in the existing summary call. A bare partial grade cannot dispatch work. The executor revalidates that plan, principal, registry and prior write receipts before any continuation; the plan grants neither permissions nor expanded scope. Group delivery status follows actual acknowledgements. A delegated editor timeout preserves a completed child result and dependency context. Synthesis-only helpers can improve the answer, but cannot change the producer's task status. Ordinary text still adds no review/classifier call.

Action preflight separates a semantic revision from an invalid response protocol.
An invalid JSON/schema response gets one tool-free correction using the unchanged
action and evidence within the original 30-second deadline. One retained-evidence
inspection has its own allowance, so the combined path can use at most three
review calls. Unused clarification fields may be null; a clarification still
requires a concrete question and multiple choices. Persistent invalid responses
and transport errors stop before the write. Runtime events record safe protocol
error categories, without rejected values or permission grants. This adds no
review call to ordinary text or already-valid action reviews.

Completion reviewers receive a valid JSON schema with the current requirement
identities and conditional clarification/alert fields. A protocol correction
carries safe validation codes and explicit correction instructions alongside the
same retained evidence. It does not resend an unchanged request, recollect source
data, regenerate files, or certify JSON from an explicitly incomplete provider
response. Diagnostics record validation categories without copying source text.
This runs only inside the existing tool-workflow review; ordinary text turns do
not gain another model call. Both completion and action-review schema hints
include the bounded retained-evidence inspection response while it is available.
After inspection, the hint permits only the normal decision. The schema must
never forbid an inspection that its instructions and runtime still allow.

Incomplete reviews can return bounded `supported_findings`, each with its own
text and current-request source/action citations. The runtime resolves short
references and rejects factual findings backed by unknown, failed, denied, scope
or inspection receipts. An explicit `kind=limitation` row may cite a real failed
or denied operation to explain its observed failure; it cannot prove successful
source work, an action or a completed deliverable. These are independently authored publishable findings,
not excerpts accepted from a rejected draft. A grounded correction can improve the primary answer; otherwise delivery preserves that answer and its valid files. Actual missing work may be disclosed concretely, while a grading status or generic review warning cannot replace useful findings. These corrections grant neither consent nor proof of unperformed actions or unavailable files. Completion certificates still require grounded evidence. This uses the existing assistance call and leaves ordinary text on its existing fast path.

The material-requirements planner receives a separate valid JSON schema with
exact registered tool names and allowed delivery owners. A typed accepted setup
receipt can prove the requested configuration action, including its constraints;
it cannot prove future matches or unrelated source facts. Contract diagnostics
record row kinds, eligible tools and delivery flags without requirement prose.
Changing the available proof schema does not change request identity or discard
a previously validated requirement contract.

A saved task's explicit execution phase also requires outcome review when its
producer makes no tool calls. Skipping execution or asking an unsupported source
question cannot make that run successful. The review may finish a genuine
reasoned answer without tools, request specific missing work, or preserve a
numbered clarification while keeping execution unfinished. Plain chat retains
its existing fast path.

The scheduled source resolver carries its runtime-validated source selection
into producer context and the review evidence contract. That describes selected
read sources; normal action permissions and approvals remain mandatory. A newer
pending source choice takes precedence. A validated public-only execution hides
private structured-account tools instead of exposing them by default.

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

Plain text turns retain their existing single generation call. They do not run
an independent classifier, allocate recovery checkpoints, or scan jobs/connectors
for this feature. The existing model call performs the reasoning. A turn that
uses tools has an additional, independent, schema-validated outcome gate. The
gate first binds material requirements to the original request, then checks each
requirement against actual tool evidence, selected file versions and the proposed
answer. Successful tool execution alone is insufficient. Execution-only constraint
rows may set `delivery_required=false`; they still need verified execution
evidence, but do not force intermediate values into the final answer. Requested
facts, actions and files retain required delivery coverage. Verified zero results
and valid action receipts are legitimate evidence; an unexplained empty result
is not. General explanations can use a `reasoned_answer` requirement without
inventing a source obligation after an incidental tool call.

The terminal gate covers regular graph finalization, early artifact completion,
direct operator shortcuts, approval continuations, Deep Agents and combined task
results. Scheduled HTML reports retain their dedicated producer/delivery owner.
Fallback generation includes an inert `NULLION_REPORT_OUTCOME_V1:` HTML comment
with schema-validated, request-bound completion metadata in the same model call.
The producer evaluates the saved requirements against source facts and the actual
document. Missing research is `partial`; missing or malformed metadata is
`unknown`, never automatic completion. A valid file remains deliverable with its
specific limitation and a `partial_success` scheduler receipt. Generation,
fulfillment and delivery have separate stored outcomes. Partial publication is
bound to the original request and exact selected file hashes; the known CSS-only
mobile overflow guard may renew an unchanged receipt. Other edits invalidate it.
This path does not add a second grading call or apply to ordinary completed
reports. Alerts still require verified matches; permissions, cancellation,
pending-source selection and file validity remain enforced. Synthesis-only
scheduled child steps retain their dependency-context check. These owners avoid
reviewing a scheduler control envelope as though it were the report. A validated
`deferred_execution` receipt binds accepted background work to its action receipt
and exact configuration. Foreground review checks that setup and every separately
requested immediate outcome; it must not wait for a future poll, match or alert
before acknowledging setup. The background owner still verifies and delivers
those later outcomes.

Before a mutating invocation, an independent action preflight returns a validated
`proceed`, `revise` or `clarify` decision. It compares the proposed action with the
original goal, available evidence and registered argument schema. Declared defaults
and enum values retain their adapter-defined meanings. The bounded evidence packet
retains result envelopes, counts, coverage and identity declarations while marking
omitted collection examples explicitly; a preview cannot turn missing context
into a claim that a source field is absent. A revision returns to the existing graph;
a clarification stops before mutation and presents numbered choices. Invalid or
unavailable reviews stop before writes. A proceed decision never supplies an
approval: existing policy, permissions and boundaries still run. Deep Agents use
the same guard through a request-local context, including LangChain adapters and
artifact fallbacks. Automatic saved-job selection enters that same executor; it
cannot bypass action preflight. Cancellation is rechecked after the model returns, so a late
preflight response cannot start an action after its caller timed out.

Preflight reports typed concern scopes. A trusted native adapter may attach
validation of the exact proposed arguments and prepared source bindings using
`ToolSpec.action_review_context`. Suggestions confined to those validated
mechanical contracts or declared defaults are retained as advisory diagnostics;
they cannot replace a valid prepared alias with a physical output handle. Mixed,
unknown, goal, target, constraint and consent concerns remain blocking. This is
not an execution grant: native admission and permission checks still run. Tracker
creation supplies this validation without source IO or another model call.
When preflight requires a user choice, the shared completion owner publishes an
`awaiting_input` checkpoint. Adapter recovery must preserve the numbered question
until the user replies, rather than reopen the action or manufacture a result.

The executor also supplies native registration evidence for the exact proposed
tool. A registration-only suggestion is advisory when that current binding
exists; provider configuration, connectivity, permission, user constraints and
targets retain their own checks. Final artifact receipts preserve an actual
review outage independently of file selection, so useful files may be delivered
without recording a completed content review or losing the diagnostic reason.

Reviews are cached only within the request and bound to the principal,
conversation, selected context, proposed answer, tool evidence and file
size/modification time. A changed answer, amendment or file invalidates the
receipt. Runtime-owned receipts prevent duplicate reviews at outer delivery
boundaries; tool JSON cannot manufacture them. Mandatory action receipts supplement
the synthesized answer instead of replacing other requested findings. Normal
finalization applies that presentation before reviewing the delivered draft,
preventing a formatter from silently removing reviewed content. Tool-bearing provisional answers
are buffered until terminal review. Shortcut repairs receive the actual retained
tool evidence in model context and disable the original canned completion path.
Runtime-blocked preparatory READ steps stay retired across later scope changes;
the independent goal review still requires source proof, and required mutations
are never retired by that mechanism. The requirements catalog includes attempted
and selected tools with actual statuses, so a failed first attempt does not make
an unmet action disappear from the plan. Native setup validation returns the
existing `correct_arguments` contract before mutation, preserving the verified
observation preparations for the corrected call. Bounded repairs retain completed actions and
working files rather than restarting production. Account writes and adapters
declaring `ToolSpec.replay_safe_after_completion` retain structured dispatch receipts
even when later result verification fails, so repairing an answer cannot dispatch
the same durable effect again. Transient browser and terminal operations keep
their normal execution semantics. Queued helper-file writes stop
when the requested files are ready for review. Unreviewed working files are not
certified as completed delivery.

Missing required tool arguments also return a typed `correct_arguments` result
before a handler runs, including the missing fields and their registered schemas.
Retry accounting distinguishes adapter-declared validation categories: correcting
one category and reaching a different check is progress, while changing argument
values or error prose cannot reset the same-failure limit. Global turn budgets
and the live tracker's three-failure pause remain separate limits.

Delegated tasks publish only reviewed results as dependency context. Their
combined summary separately evaluates the original goal against all child
results. Missing work can receive one bounded continuation using retained
receipts and the structured allowed tool scope. Invalid review coverage or an
unavailable reviewer cannot mark the group complete. Delivery retries use the
existing outbox and do not restart completed work. Legacy sequential missions
persist request-owned continuation evidence alongside pending approvals, so
resuming a later step retains prior results and the actual pending tool batch.

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

Scheduled executions persist diagnostic events incrementally with `cron_id` and
`cron_run_id`, including tool and model timings, schema failure categories, and
completion-review decisions. A request-local event observer follows the execution
even if its in-memory store is refreshed. It checkpoints captured rows rather than
serializing the full runtime; bounded lock retries preserve the captured rows.
Correlation metadata also remains on in-memory events so later saves cannot strip
it. A terminal `conversation.cron_run` records fulfillment, remaining work and
compact tool statuses without source bodies or tool arguments. These are private
diagnostics, separate from the user-facing delivery receipt. Plain chat does not
install this observer or incur a diagnostic database write.

Scheduled requirement planning and outcome review receive the run's actual clock,
timezone and saved schedule as typed execution context. These facts constrain
source checks; they do not become extra user deliverables or require scheduler
control tools inside a scheduled run. Changing the saved schedule remains outside
that run's authority.

Requirement-planning transport retries are bounded across the whole request,
including later finalization attempts. A default timeout can receive one larger
retry budget (at most 60 seconds); an explicit configured deadline is retained.
A verification outage is carried as typed outcome-review metadata. Scheduled
delivery does not reinterpret a verifier timeout as missing account work or launch a
fresh producer continuation to repair it. It reports the verification failure.
Invalid protocol responses may use bounded evidence-preserving report repair.
Structured response allowances reserve space for the configured reasoning effort
as well as JSON output. High reasoning retains the selected model and effort,
receives at least 4,096 tokens and a 60-second default verification deadline;
explicit deadline settings remain authoritative. Diagnostics record the actual
model and reasoning profile used by each check.
The default outcome-review deadline also accounts for the actual JSON evidence
packet size, including retained inspection windows. Large batch reviews can use
up to 60 seconds even with a short output allowance; small reviews retain their
normal deadline. Explicit timeout configuration always takes precedence.
Tool-free action preflight, requirement enumeration and outcome checks use the
same configured model with a bounded reasoning profile (`low` by default).
`NULLION_WORKFLOW_CHECK_REASONING_EFFORT` accepts `low`, `medium`, `high`, or
`inherit`; it never changes the main agent's research/response profile or selects
another provider/model. Dataclass adapters are copied; unsupported adapters keep
their existing client and bounded timeout recovery. Runtime diagnostics show both
configured and effective effort. Plain turns do not call this helper.

Report-recovery evidence budgets reserve space for later source families and
retain account record identities, check scopes, date bounds and result counts.
Repeated identical hydrated reads do not crowd out later checks or receipts.
Truncated content remains marked as truncated. The connector email adapter
exposes page completeness and continuation tokens; only adapters with actual
pagination support advertise that input in their tool schema.

Verified-match monitors share native prior-delivery context with the producer,
requirement planner, independent reviewer, report recovery and delivery decision.
The read-only persisted query
filters by saved task and delivery conversation before taking the latest five
receipts, and merges freshly delivered in-memory receipts awaiting a checkpoint.
Unrelated recent chat cannot displace that task's history. Only a successful
persisted read can establish that no prior messages exist; unavailable or bounded
history cannot prove that an item was never reported. Receipts establish prior
delivery, not current external facts, and failure notices do not acknowledge a
matching item. Ordinary reports do not load this monitor-only context.

Scheduled cron turns also reuse the foreground workspace-scoped memory reader
with reinforcement disabled. Its bounded known-user context reaches the
producer, action and outcome checks, report recovery and delivery decision.
This adds no classifier call or memory write. It respects the memory setting
and the saved task's explicit workspace ownership, including when notifications
go to a different recipient. Recipient-owned legacy memory is not merged across
that boundary. Another workspace's identity or preferences are never part of
that packet. Profile facts and durable preferences inform identity and
consent, but cannot establish current external events or account status.
Action-review diagnostics retain redacted reasons for revisions so an aborted
turn remains diagnosable without storing full proposed tool arguments.

The shared outcome-review packet retains normalized object-valued records even
when their adapter also returns a text receipt. Its evidence allowance scales
with the number of actual results, within a fixed upper bound. Oversized values
retain JSON object/array types and explicit omission paths; a truncated JSON
string is never substituted for structured proof. Omitted fields cannot prove
absence or a negative result. Planned mutating capabilities remain available
during read-focused recovery, subject to their original visibility, permissions
and approvals; recovery cannot grant new access.

Queries, identifiers, date bounds and continuation handles remain exact during
compaction. A value that cannot fit is omitted explicitly rather than altered.
The reviewer can request one bounded inspection of already-retained evidence
using `inspect_evidence`, an exact visible invocation reference, a JSON pointer
and a character window. The runtime validates ownership and schema, redacts the
retained value, and returns at most 24,000 characters across 128 windows. The
character budget is shared, so batches of small fields do not fail merely for
containing more than sixteen records. The same constants define validation and
the model's structured `evidence_inspection_limits` packet. Runtime inspection
state distinguishes a rejected request from a completed inspection. It
does not call tools, open files, read another request or expand permissions.
The next review decides from those facts within the ordinary two-call bound.
If the first inspection request fails schema or ownership validation, one
protocol correction may extend that review to three model calls: rejected
request, corrected inspection, and final decision. This never repeats source
execution or grants access to another request. A second invalid inspection
stops that review. Diagnostics distinguish request transport from response
protocol failures without storing rejected source payloads. Ordinary turns and
complete small reviews incur no inspection or protocol-correction call.

Inspected windows survive same-request draft corrections and scheduled report
composition through the existing request-bound contract cache. The cache stores
only invocation identity, pointer, offset, length and a fingerprint of the full
tool result. Each subsequent view is rebuilt and redacted from current owned
results; changed, missing or no-longer-visible results invalidate that view.
The combined carryover is capped at 48,000 characters and 128 windows. Each new
inspection retains its existing 24,000-character limit. Both the reviewer and
scheduled composer contribute descriptors, so either can inspect a decisive
field once without requiring a later phase to repeat the source call. These
windows carry source facts, never a previous verdict, permission or completion
receipt; every changed report still receives independent outcome review.
Empty caches add no source hashing, network request or model call.

Saved-task execution carries a typed `execute_saved_task` phase and the saved
job identifier. Recurring enabled/disabled state does not determine whether an
already-dispatched one-off run may execute. Local working files and checkpoint
writes are distinct from registering or changing background work; every normal
permission and approval boundary still applies.

A report composer's incomplete claim or proposed remaining work is advisory.
When a validated material contract exists, a grounded candidate goes to independent
outcome review before any execution continuation. Only a validated missing-outcome
review can restart source/action work; the composer cannot invent prerequisites.
An optional verifier outage preserves usable public findings and valid selected
files without restarting the producer or certifying unfinished work. Permission,
cancellation, required-action, file-validation and delivery checks remain enforced.
Model-response errors and response timeouts both reach the shared
scheduled content preparer with a typed interruption fact. A recovered draft
still needs its own current source/action receipts. If composition itself fails,
an editorial verdict cannot mark the failure notice as a verified report;
independently valid files and retained partial findings remain deliverable.
Recovered reports retain the independent review's exact correction guidance
and typed requirement rows, evidence references, and the current rejected draft.
A request-local typed handoff carries the validated requirement contract and
latest review from execution into report recovery and back into a continuation.
Recovery resolves or reuses that contract before composing a draft. Its existing
request/context binding invalidates changed requests or preferences, while new
source adapters can provide more proof for the same outcomes. The handoff carries
no runtime/model objects, permission grants or completion cache; every revised
draft still receives fresh independent verification. Plain turns allocate no
handoff and add no model call. Saved delivery history is explicit in the review
packet as well as its scheduled execution context.
Before side effects, the shared action reviewer can inspect one bounded set of
retained same-request output windows, including reads older than its compact
preview. This performs no file/network IO and grants no permission. Invalid,
foreign, repeated or incomplete inspections fail closed; the producer's normal
approval and policy checks still run after a valid decision. Both the foreground
orchestrator and Deep Agent adapter supply their own request-owned results.
Outcome review keeps one evidence inspection separate from one protocol repair;
a truncated post-inspection answer can be corrected against the same proof,
without rerunning the producer. Output budgets scale with the evidence packet.
Every typed saved-task execution assigns final result transport to the scheduler,
including ordinary native-tool reports. Producer verification covers the report,
files and immediate actions; it cannot require a future platform-delivery receipt.
The scheduler's later persistence/upload boundary still must succeed.
If the material contract cannot be verified, recovery stops before report
composition or source re-execution and preserves a verification failure.
Independent review covers recovered native-tool reports as well as public-source
alerts. Artifact workflows that already have a dedicated delivery-owned review
keep that owner; other artifact reviews receive the actual selected file paths.
For alert-only saved tasks, the full-evidence outcome review also returns a typed
send/silent decision. The scheduler reuses it only with the exact completed outcome
receipt and a request-local binding covering task, recipient, clock, context,
delivery history, report, source/action evidence and selected file versions.
Changed or forged receipts cannot authorize delivery. Verified absence can satisfy
an absence-based alert condition; zero records alone never determine notification
intent. This avoids a second classifier reinterpreting a truncated preview.
Legacy results without that bound decision use a guarded classifier fallback.
Alert-only delivery uses the same model's bounded structured-check profile, with
an explicit decision/reason contract. An invalid or truncated response gets one
format-only correction inside the original 45-second deadline, without repeating
research. Provider exceptions or a second invalid response remain failed runs,
never verified negatives. Diagnostics retain attempt, output length, stop reason
and validation status, without copying private source content.
Native calendar listings expose a count of returned events, retaining the exact
requested date bounds and limit. A valid empty collection can establish zero
results for that lookup without claiming unbounded calendar coverage. Malformed
provider collections are failures, never fabricated zero-result observations.
A bounded report-only revision is available
when every material evidence requirement is satisfied, the draft still has a
presentation gap, and the reviewer requests no additional tool work. It must
pass a fresh independent review before claiming verified completion. Other recovery uses the same
specific feedback in the execution continuation, rather than restarting from a
generic instruction or the original failed reply. Verified report repair clears
only its own stale failure flags; it never clears independent source gates.
One report-only revision and productive execution continuations have separate allowances,
with at most five completion passes and the existing combined producer budget. A draft correction cannot consume the
remaining opportunity to verify a missing source, and an execution continuation
cannot consume the report correction. Repeated presentation-only failure stops
without restarting successful research. Prepared HTML enters this same revision path before partial-file fallback, including after source recovery. The correction receives exact selected deliverable names, not internal datasets as additional attachments. Runtime-grounded `execution_satisfied` assesses satisfied material rows using the same evidence checks independently of `draft_covered`; it never grants publication or consent. Bound report receipts distinguish `content_status` from `presentation_status`. A final summary still needs fresh independent review. If composition fails, supported file bytes remain deliverable with a summary-specific notice. Private `remaining_work` and reviewer edit instructions never become public captions or missing-requirement copy. Every changed draft or evidence set
still requires its own independent review attempt and bound decision.
The helper may suggest a bounded improvement within the producer's existing
scope. Grouped work requires an explicit structured plan naming owned tasks and
allowed tools; a partial grade alone cannot dispatch work. Cancellation still
prevents dispatch after child tasks finish. A necessary source choice or actual
permission boundary retains its explanation or numbered question. Reviewer
transport/protocol failures remain internal and cannot erase a useful answer or
selected valid files, create a failure streak, or certify completed execution.
The producer's pre-improvement result is retained if an optional rewrite times
out. Grounded corrections remain applicable; an uncited critique is not a veto.
Failed required tools, pending source selection, missing action receipts,
unsafe/raw output and invalid files retain their independent checks. Scheduled
verified-match alerts still require their positive bound decision; an unavailable
helper cannot establish a matching alert.

The cron report's review status is recorded separately from execution and actual
transport receipts. Review-only failures do not restart source work or report
composition. Existing redaction/internal-output checks can still request a bounded
presentation repair. A changed repaired draft receives its own review; a successful
review updates the internal availability status. Delegated groups retain supported
findings and independently successful, selected, byte-valid files when a sibling
fails, while leaving the overall work incomplete. A delivered result with an
unavailable group reviewer is not a transport failure.
Failed or missing structured task dependencies block their dependent findings and
files, including transitive dependencies; unrelated successful files survive.
Recovery may clear scheduling dependencies to retry work, but preserves the
original publication prerequisites as typed task metadata. Recreating a file
does not erase an unfinished source dependency.
Child answers retain typed helper diagnostics for aggregation without adding
reviewer-status prose to the delivered answer. Existing delegated provenance,
file-content and execution failures cannot be promoted to success by a later
reviewer outage. A partial helper selection supplements a larger valid producer
answer instead of silently replacing it with a smaller subset.

Foreground execution reuses its already-loaded profile prompt in the shared
action and outcome review context, alongside durable memory. Explicit current
instructions override memory and profile defaults; defaults establish neither
consent nor live source facts. Profile changes invalidate context-bound review
decisions. A selected parent clarification carries its original unfinished goal,
question and tool identities as typed state across replies. Its optional tool
visibility is restored through registered metadata without restoring an action
obligation or granting execution permission. A no-tool acknowledgement is reviewed
against that pending request and can continue in the existing bounded graph.
Cancellation, replacement and explanation-only instructions can close the current
turn without running the old action. Parent lookup is bounded, workspace-scoped,
post-reset, and gated on a selected continuation; unrelated plain turns do not
perform it or acquire an extra reviewer.

Model review accuracy must be calibrated separately from runtime regression tests.
`scripts/evaluate_completion_reviews.py` reads sanitized, human-labelled JSONL
traces offline and reports false-block, missed-block, false-correction and
missed-defect rates, plus unavailable reviews. Missing label classes produce
unknown rates rather than perfect scores. The command never calls a provider,
reads a live account or changes a runtime. Supplying representative real-provider
traces and human labels is required before claiming model calibration.

Completion evidence is reusable across a finding, execution constraints, action
checks, and its report. Constraints such as reading the relevant records describe
conditions on that work; they are not additional source entities. Positive result
status, tool eligibility, and artifact-package member checks still apply. Distinct
facts/comparison sides, actions, and files retain independent record bindings.
The reviewer receives an explicit catalog of citable positive evidence and a
matching response schema; inspection and scope-control receipts cannot prove
source facts. Binding failures identify the conflicting references and subjects
so a protocol correction can use retained proof without repeating source calls.

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


Validated read-source consent survives in the review contract; the reviewer
cannot invent a new source-selection requirement after that selection is done.

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

A successful identical invocation resolves its failed predecessor. A successful
unrelated read does not. Existing semantic completion verification can also
resolve repairable failures when it verifies the original requested outcome.
Action preflight revisions are rejected proposals, not failed executions. A
validated `revise_plan` contract keeps the same capability available so the next
existing model round can correct its arguments. Each corrected proposal passes
preflight, policy, consent and cancellation checks again. Only a completed,
admitted execution of that same tool can retire earlier non-executed proposals;
it does not certify the final outcome or clear actual execution failures. This
contract applies to the main graph and delegated Deep Agents. Repeating an
unchanged rejected proposal remains bounded by the existing failure guard.

Two equivalent scope selections after successful work produce a correction in
the next existing model round, allowing actual execution rather than another
scope-only review cycle. A further unchanged selection stops that recovery
path. New execution evidence or a changed execution contract permits progress.
The terminal review may preserve supported findings or ask for input/approval,
but cannot reopen the exhausted path. Its evidence-bound receipt prevents an
outer completion check from repeating the same recovery without new evidence.
The LangChain adapter returns a withheld proposal to the model rather than
automatically sending its rejected arguments to a fallback implementation.
Scheduled report recovery also distinguishes an unchanged file receipt from
proof of completed research. A producer's partial/unknown verdict is retained
without another grading call until execution advances. Typed progress from a
productive continuation then invalidates that review shortcut, even when
earlier interruptions prevented a report review. File identity alone never
promotes partial research to complete.
Runtime-owned `recovered_by` receipts reference actual completed invocation IDs;
prose or JSON inside source output cannot clear a failure. Execution outcomes
retain historical recovery evidence while avoiding a false partial-failure
status after successful repair. Execution verification never proves delivery:
attachment/upload receipts and the existing delivery outbox remain authoritative.

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

`ToolSpec.observation_inline_setup_tools` identifies prerequisites already
enforced by the source handler. Replay validates their current READ contracts
but leaves execution to that handler, so duplicate saved waits cannot hide a
repairable readiness failure. Browser text reads retain their source projection
while permitting bounded repairs of `ready_selector` and `ready_text` from a
fresh native snapshot. After a verified repair, the saved comparison snapshot
is bound to the committed replacement so the next real change is preserved.
Temporary completion-review focus also retains a selected reader's declared,
callable READ setup tools rather than stranding recovery on the current page.
Repair budgets bind precondition failures to the last actual page receipt in
the same physical session. Failures on different sources do not accumulate as
one repeated action; a successful same-session read clears its corrected
precondition streak. Translating diagnostics or changing selectors on the
same source does not reset its budget.
Native navigation, extraction and direct HTTP read timeouts are invocation-scoped too; only an
actual capability/connection failure can retire the tool across sources.
At terminal handoff, an independently grounded corrected answer from the
existing outcome review replaces stale method-failure copy. This publication
does not grant consent, waive missing execution, or restart the stopped method;
the common boundary still computes fulfillment from the current receipts.

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

### Paginated source evidence

Adapters may declare a `source_pagination` cursor-page receipt with an opaque
scope identity, the current cursor, next cursor and actual returned count.
The shared review layer checks the initial page, every cursor link and the
terminal page. Missing links, cycles, disconnected pages and conflicting
receipts remain incomplete; duplicate page receipts do not inflate counts.
This describes listing coverage only. It never proves relevance, full message
reads, external actions or a completed user outcome. Native connected email
search emits this descriptor; other adapters can use the same contract.

Completion review and final composition receive those page-chain facts.
Terminal cron diagnostics retain opaque scope IDs and invocation references,
not cursor values or private query text. An incomplete review must author
independently cited public findings. Terminal delivery preserves those findings
and derives any remaining limitation from typed outcome/runtime facts; raw
review diagnostics cannot supply public copy. Execution remains unsuccessful.
A paused or unknown schedule must not promise a future scheduled retry.

### Progress during completion recovery

Scheduled completion keeps execution, report content and transport separate.
The common agent completion owner reviews a native prepared file against the
saved task and retained source/action evidence when no content verdict exists.
A generated fallback carries its content outcome in the generation call instead;
it does not need a second verdict over unchanged generated bytes. Valid HTML and
an upload receipt alone cannot certify research completion.

A partial producer may select exact `retry_tool_names` from its supplied tool
catalog. The runtime validates that structured selection, focuses the resumed
registry and retains completed reads/actions, page-chain evidence and the exact
report identity. The partial file remains a fallback while execution continues.
Its request/file digest prevents unchanged incomplete bytes from activating the
file-exists completion shortcut after a later source read. A changed report gets
the delegated content verdict before it can become complete. Failed recovery
retains an unchanged valid partial file; it does not generate replacement copies.

Scheduled execution uses one graph with the old initial-plus-continuation
producer allowance. The scheduler does not restart it from a second review loop.
The common owner retains its bounded improvement window; further improvement
requires new relevant successful evidence and the same remaining execution
allowance. Provider retries and model promotion consume that allowance too.
Repeated identical calls, failures and unrelated tools do not renew a window.
An approved budget resume supplies an explicit new baseline, not new permissions.
Report-only correction has its own single allowance; it
cannot perform missing source work. Cancellation and approval still stop work.
The report owner receives the run's cancellation checker and must not start a
content review after cancellation, including the handoff from a finished producer.

Alert-only monitors may return a typed `silent/no_verified_match` decision when
all required checks are satisfied but an unnecessary draft needs suppression.
The runtime grounds the full source/action matrix and requires complete declared
cursor chains before closing silently. Missing checks, pending actions, reviewer
errors and requested file obligations cannot use that transition. This applies
to native adapters as well as browser sources. Inline sourced replies are fact
requirements; internal checkpoint writes are execution constraints, not user
attachments. The planner's existing protocol correction repairs contradictory
file requirements without rerunning sources.

Producer context is bounded across ordinary source/report workflows as well as
artifact workflows. Large owned tool histories become typed previews; original
receipts stay in request state. The lazily exposed `inspect_retained_evidence`
tool resolves exact JSON-pointer windows from those receipts without network or
filesystem access. The entire newest tool-result batch remains visible at the
adapter's existing payload boundary before it becomes archived history. This
includes every parallel inspection, not only the last one. The fresh batch can
exceed the archival history target; it must not be shortened before consumption.
Previously inspected windows are rebuilt from unchanged request-owned sources
and remain available to later producer steps as well as independent
review/composition. Archived inspection receipts omit entire windows explicitly;
they must never shorten a window's content while retaining its original offsets
and character counts. Inspection receipts cannot establish new source facts or
completed actions themselves.
Security classification, scan findings and boundaries remain attached to each
result; identical runtime security instructions are included once per view.
Small histories also share identical policy text when the resulting view is
smaller, without changing source fields or enabling an evidence-inspection call.

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

Producer execution uses the existing turn/recovery iteration limits and user
cancellation. Tool counts, cumulative message-text characters and retained
evidence inspection counts are diagnostic metrics only, including across
same-request resume/retry; they must not stop a run. The removed
`NULLION_REQUEST_MAX_TOOL_CALLS`, `NULLION_REQUEST_MAX_MODEL_INPUT_CHARS` and
`NULLION_REQUEST_MAX_EVIDENCE_INSPECTIONS` settings are ignored. Character
counts are not billing-token estimates. Independent review and composition
keep their existing bounded packets and retry limits.

Evidence binding distinguishes workflow roles from independent subjects: an
observed source may support its fact and the report describing it. Independent
entities/comparison sides, separate actions, and separate requested files still
require their own eligible evidence or validated record bindings.

A helper can request scoped improvements within the producer's current progress-based turn budget. The executor retains completed receipts and normal action deduplication; an editorial vote cannot renew a review loop or obtain a new work budget. New evidence can renew the producer's existing progress window; an unchanged critique cannot. Plain chat does not scan review history or gain another model call.

Scheduled delivery may use one tools-disabled summary correction from retained evidence. This does not certify missing source work. A useful answer and selected valid files remain deliverable when editing or review is unavailable; editorial status alone does not convert the producer into a failed scheduled run. Actual incomplete source work, missing actions, invalid files and unsuccessful deliveries retain their separate outcomes.

### Tool argument protocol recovery

Model adapters decode tool arguments as JSON objects. Invalid JSON, non-object
arguments, unfinished streamed calls, and incomplete response snapshots remain
explicit protocol failures; raw fragments are never wrapped as executable input.
The orchestrator reports `invalid_tool_arguments`, `executed=false`, safe decode
diagnostics, and a `correct_arguments` recovery contract before scope mutation,
approvals, semantic action review, or execution. The next existing model round
can submit a complete shorter implementation or split work into complete calls,
while retaining completed source reads and normal permission checks.

Model timing records retain completion status/reason and malformed-call counts;
tool timing records retain decode category, offset, length and digest without
logging raw argument bodies. Scoped tool visibility alone does not establish
that an image provider is unconfigured, and setup hints cannot replace useful
source findings or local-renderer results.

The LangChain adapter preserves rejected calls in `AIMessage.invalid_tool_calls`
instead of turning them into executable empty/default calls. Native tool nodes
therefore cannot execute those fragments. A subsequent existing model round
retains an explicit protocol-correction notice in its assistant history.

### Review diagnostics and public publication

`review_publication.render_incomplete_review` is the shared terminal boundary
for direct chat, correction exhaustion, scheduled/manual notices and reviewed
file captions. The legacy `user_facing_failure` field and
`unresolved_requirements` are internal diagnostics even when their prose sounds
user-facing. They never supply public notice text. Certified findings keep their
independently validated positive receipt bindings. Actual missing requirement
kinds select concise notices; runtime-owned codes preserve operational deadlines
and missing required execution. Network adapters retain typed exception categories,
so transport causes can be described without copying private error text. A structured source-connection failure relevant
to an unmet requirement also retains its reconnection guidance; an unproved
reviewer claim cannot invent an access failure. All-satisfied material rows can publish clean
findings after a draft rejection without a fictional missing-work footer. This
presentation transform does not change execution, consent or completion status.

The same existing review call supplies typed `presentation` (`item` or
`paragraph`) and optional short `section` metadata on each supported finding.
The shared publisher groups adjacent items under one section heading and keeps
their multiline details attached to the item. The model selects one requested
entity or comparison unit per item, with related values, units, source links
and uncertainty together; prose remains available for summaries and explicit
format preferences. No natural-language phrase detector, sentence splitting or
additional model pass chooses the layout. Older findings remain readable;
malformed display hints fall back to plain text without invalidating grounded
evidence. Evidence aliases and recovery handoffs preserve layout metadata.
Certified limitation rows can bind `requirement_ids` to the unmet material
rows they explain. Only coverage of every unmet row suppresses an extra generic
incomplete-search footer; an unrelated limitation cannot hide another gap.
Operational failure notices and numbered input or
approval choices are retained. Web rendering keeps indented continuation lines
inside their list items, including restored history. Telegram uses the same
structured Markdown through its existing HTML transport formatter.

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

The already-loaded preferences also produce a typed `request_time_context`.
Requirements planning, action review and outcome review share its local date,
timezone and UTC acceptance instant. Explicit task dates/timezones take
precedence. Requirement caching retains that request reference across a UTC
midnight; scheduled execution uses its current run clock instead of an older
creation clock. Neither change adds a classifier or review to plain chat.

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

The native source-fetch adapter distinguishes HTTP rejection from network
failure using exception types. A failed HTTP request carries `reason=http_error`
and its existing `status_code` field; wrapped timeouts remain `handler_timeout`.
Filesystem permission and storage errors are not classified as network outages.
Caught HTTP error responses are also closed explicitly; `urlopen` can raise
before the normal response context has been entered. Connector, media-provider,
connection-check and loading-screen error consumers release their response body
even when parsing or reading the error fails. Cleanup cannot replace the original
request failure with an ordinary socket-close error.
Specialized producer and review summaries preserve these typed status, failure
and consent fields even when their display projection omits other payload data.
Nested recovery contracts retain their JSON types and plan structure; they do
not pass through the lossy display-value projector. Whole-message context limits
and retained-evidence inspection still apply.
This is transport metadata, not a retry decision or permission grant. The
existing source review and evidence-bound public limitation contract decides the
answer. Raw upstream exception text remains internal.

Conversation-analysis failures return a controlled HTTP 500 with the normal
`ok`/`error` envelope. The endpoint logs the exception type and does not expose
private storage or provider error text, or raise another exception while handling
that failure.

### Requirement-bound operational failure attribution

Outcome review can attach optional `blocking_failure_invocation_ids` to an unmet
material requirement. These are distinct from positive `evidence_invocation_ids`:
the response schema admits only known failure-role receipts, packet aliases are
resolved to request-owned IDs, and the runtime checks that each selected failure
is current, unrecovered and eligible for that specific unmet requirement. An
unknown, completed, recovered or unrelated receipt cannot select operational
copy. Failure references never satisfy requirements or grant consent.

Without an explicit causal binding, a useful eligible source read prevents an
earlier failed attempt from being inferred as the reason for an unfinished
comparison or missing state verification. Entirely failed source paths retain
their typed failure explanation. Derived operational codes are recomputed from
current receipts on continuation; intrinsic deadline/contract outcomes retain
their owner. This changes neither fulfillment nor recovery budgets.

The existing reviewer authors source-backed findings and explicit public
limitations. Internal correction prose stays internal. A completed browser click
without a subsequent observation is still unconfirmed state. Subjective quality
criteria do not create a universal numeric threshold. This uses the existing
review call and leaves ordinary chat on its existing fast path.

The final review owner also records `execution_review` under runtime-owned
`ToolResult.response_requirements`. Its binding covers the original request,
final text, ordered receipt identities, tool/status/output/error fields and
runtime recovery links. External output with the same field name has no
authority. Selected file versions are bound by path, size and modification time;
file validation and delivery remain separately enforced. A changed reply,
changed evidence or changed selected file version invalidates this receipt.
Legacy fulfillment and connector/status classification consume that existing
review; they do not call another model or infer recovery from a tool name.
Certified completion retires historical attempts from terminal failure
classification while keeping them in diagnostics. Certified partial findings
survive a current connector blocker. An incomplete review remains incomplete.
Cancellation, pending approvals, missing actions, required files and transport
failures retain their own enforcement. Scheduled outcomes validate against the
task and original result body so a transport header does not replace that proof.

When the legacy fallback knows which required tools are missing, only failures
of those tools may select operational copy. Mixed useful evidence without a
causal binding receives an incomplete-work notice rather than an invented
browser outage. Generator errors use typed timeout, plan-revision, dependency
or provider-configuration reasons; unknown errors receive a bounded generic
notice. Raw provider diagnostics and install commands remain internal. Only an
explicit `provider_not_configured` result can select provider setup guidance.
Image-provider exceptions preserve a typed timeout or generation-failure cause.
Review evidence compaction preserves bounded typed connection/control fields
across generic, connector, weather and browser projections; a completed adapter
receipt that still requires user action cannot become positive completion proof.

### Search coverage and stopping decisions

Action preflight judges the admissibility of the proposed step, separately from final outcome coverage. Its typed `issue_kind` and `step_effect` distinguish concrete conflicts from incomplete evidence gathering. A relevant observational step can proceed while later source checks remain necessary; a contradictory verdict receives the existing bounded protocol correction. This never grants permission or certifies fulfillment. Concrete conflicts, required user choices, security checks and approval gates retain their authority. Both foreground and delegated tools use the same assessment.

Unexecuted preflight revisions are bounded per tool and request, including across changed arguments, scope selections and restored evidence. Two identical rejected proposals retain the earlier no-progress guard; three unresolved distinct proposals stop that repair path. An admitted completed execution resets this repair count, so this is not a limit on productive tool calls or workflow size. Foreground review cannot reopen an exhausted path, and delegated adapters stop the SDK before more variants are proposed. Successful evidence and unrelated actions remain governed by their existing contracts.

`search_coverage.py` builds a bounded source ledger from adapter-owned `source_candidates` and native `browser_item_collection` records. Nonempty adapter-declared `source_record_collections` also supply observations, including structured detail reads without a text field. Declarations must resolve to records with their declared identities. Failed calls, empty records, navigation alone and compacted identity inventories do not supply content observations. A captured collection observes its document, not each linked target. It preserves exact query/fragment URLs, treats navigation separately from observed content, and stores the ledger in the goal-bound workflow checkpoint. Model-facing source context is rebuilt from full retained receipts after compaction; restored observations alone cannot authorize completion. Adapters may explicitly declare `canonical_source_url` without certifying any date or filter. Native media/download adapters declare `observed_url_fields` on source-record collections only for actual fetched resources. Screenshot crops declare the captured page URL from the native screenshot receipt; requested pages and linked asset URLs do not establish observation. Distinct deliverables bind distinct native artifact records, while their supporting source records may be shared. Reusing the same primary file across independent deliverables remains invalid.

Only requests with structured discovery or observed HTTP sources add a coverage plan to the existing material-requirements call and an assessment to the existing outcome review. Typed scope distinguishes an explicitly restricted lookup, finding any qualifying match, a comparison, comprehensive coverage, and a report bounded to observed evidence. The last mode must disclose its source scope and cannot certify broader absence; independent material rows still verify every requested report outcome. The review separately verifies requested constraints, relevance decisions and receipt-backed observations. An official-looking source does not grant broader coverage. A qualifying positive can complete a find-any request without reading every source; a negative cannot silently skip relevant alternatives. There is no vendor list, fixed minimum site count, prose-based intent classifier, or new model call for ordinary chat. Truncated discovery is explicit and cannot certify comprehensive absence.

Comparison plans select `comparison_mode=bounded` for practical recommendations among verified options, or `exhaustive` when the request requires complete coverage or a global optimum. Legacy plans retain exhaustive source checks. A bounded assessment binds every selected option and benchmark to current positive observations, verifies all material requirements and filters, and explicitly discloses its claim scope. Its typed `frontier_candidate_ids` identifies discovered contenders that could credibly change the ranking; each must be checked or excluded with evidence before `ranking_complete` can close the comparison. Incidental unvisited listings remain pending, not certified irrelevant or worse. No fixed candidate count determines completion. Named required subjects, negative conclusions and exhaustive comparisons keep their coverage requirements, including discovery truncation limits.

Scope-only wording correction reuses the completed evidence without requesting more source tools. Bounded comparison recovery preserves the reviewer's selected tools or uses eligible requirement tools rather than adding every READ capability. Actual source or contender gaps remain incomplete and publish a concrete coverage limitation, including when the reviewer terminates as blocked despite satisfied individual fact rows. Pending-source counts never imply those sources contain valid alternatives. These decisions extend the existing planner/reviewer calls; ordinary chat adds no classifier, discovery scan or model round.

The producer and reviewer treat the comparison frontier as evidence-backed: an unknown field or the mere existence of a listing is not evidence of a comparative advantage. A scoped recommendation ranks its checked shortlist without claiming global optimality. This distinction uses the existing reasoning and assessment schema, with no additional classifier call.

A scope rejected against the original request invalidates the planner/review cache even if no tool output changed. Completion recovery uses current scoped READ metadata, with ordinary permissions and action review intact. Blocked alternatives retain independently supported findings and specific limitations. Source/tool/schema errors never become verified negative inventory. Coverage diagnostics are recorded on `conversation.completion_review`.

After scheduled execution continuation, producer success alone does not reset a pending independent review. An exact fresh outcome receipt may close it; otherwise the updated text and receipts return to review before verified completion. This preserves successful native reads and action receipts without repeating execution merely for verification.

### Delivery acknowledgement and report-review freshness

A non-idempotent upload has distinct acknowledged, rejected, and unconfirmed outcomes. `delivery_acknowledgement.py` binds progress to actual SDK replies and stops a batch when an acknowledgement is lost. Read/write/protocol failures do not authorize replay; an established pre-connect failure or explicit retryable rejection may retry. Provider SDK error normalization stays in the transport adapter. Telegram scheduled, operator, interactive and callback document uploads share media timeouts. Slack and Discord uploads use the same acknowledgement rules. This policy covers artifact types through their existing descriptors, with no filename, account or content matching.

An unconfirmed/partially acknowledged send propagates through the cron LangGraph and persists a receipt with confirmed, uncertain and unsent paths. It never becomes a successful upload or a definitive statement that nothing arrived. The delivery owner may send one status notice, without automatically replaying the uncertain upload. A confirmed SDK response remains authoritative if later client cleanup fails. Durable group delivery stores an in-flight item before calling the transport; restart with an unacknowledged in-flight item becomes unconfirmed, rather than repeating that item. This avoids blind replay, but cannot promise exactly-once remote acceptance when a platform supplies no recoverable acknowledgement.

Prepared-report review is bound to the selected bytes, current execution evidence and final reply. Successful recovery reads invalidate an older review even when the file is unchanged; changed files likewise require fresh review. Unchanged evidence/file/reply does not incur another review. Review outages preserve usable primary answers and valid files without publishing review warnings or certifying completion; cancellation still prevents new review, actions or upload. This uses the common completion owner, with no scheduler-specific execution controller or additional review on plain chat.

After a reviewer outage, runtime caption normalization alone does not start another review of the same source receipts and selected bytes. Reaccepting an unchanged owned partial report preserves its real producer receipts instead of synthesizing another file write. New source results or changed bytes invalidate that reuse. Review schemas advertise one receipt reference per citation, with short packet aliases where available; legacy canonical references still resolve and undergo the same grounding checks.

Corrected chat reports cite the evidence relevant to their findings without repeating every execution reference. Required file outcomes retain separate positive producer/inspection proof and selected-file checks; a chat correction cannot complete a missing file or an unperformed action. Final-review availability remains separate from producer timeouts, source failures and delivery failures. A delivered report whose only unknown is helper availability is recorded as delivered, without inventing verified success or a failed-task streak. Genuine producer-declared partial work remains incomplete, and condition-alert tasks still require observed matches.

A file version that changes during a tools-disabled correction or active review is an integrity conflict. Preserve a valid partial file, leave completion unknown, and stop automatic rewriting of that version. A fresh grading result cannot authorize overwriting an unexpected concurrent change. Runtime fallback file receipts use typed `ToolResult` identities: unchanged accepted bytes reuse the identity; changed bytes acquire a new one. A fresh complete content review clears older partial/unavailable flags rather than allowing those flags to suppress a now-complete report.

The reviewer grades the selected publication target. When it authors corrected
findings, source presentation validity applies to that replacement against the
same captured facts and semantic roles, rather than the superseded draft.
The generated response schema advertises the correction parser's requirement
for a positive source-presentation verdict when a source selection exists.
Native reports without a selection may retain a null verdict. This prevents an
avoidable protocol retry without weakening execution, citation, consent or
artifact grounding, and adds no classifier or model call to ordinary turns.

A declared scheduler action that reaches finalization without its mutation
uses the same outcome review to distinguish an unperformed action from a
missing user-controlled choice. A validated `user_input` clarification needs
no external source receipt, grants no mutation, and retains the original
request and declared tool names for the numeric follow-up. With no observed
findings, only that question is published. The generic missing-action fallback
cannot overwrite this typed clarification. Ordinary requests without a pending
scheduler action retain the existing fast path.

An existing tracking-mode clarification can be resolved by a later structured
scope plan selecting exactly one registered creation type. An explicitly
requested clarification or a plan selecting both types still waits for input.
The selected mode is recorded in the scope before tool visibility is computed;
normal action review and approval remain independent of that selection.

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

Search coverage binds reviewed constraints through request-owned opaque ids;
optional descriptions can be paraphrased or translated without changing the
binding. Legacy saved reviews may use an unchanged plan description. An unknown,
missing or duplicate binding cannot certify the constraint. A review protocol
failure stays separate from unfinished source work and uses the existing
review-unavailable path rather than dispatching another search.

For `find_any`, one current observed match with verified constraints can complete
the request even when optional discovery entries have unresolved URL bindings.
Those binding diagnostics remain in runtime events. Negative, restricted,
exhaustive comparison and comprehensive coverage retain their source checks. Discovery URLs,
filters and fragments are not silently merged. Structured item collections
observe their containing page; linked entries remain discovery only. Multi-page
captured observations bind their own URLs. Observed evidence is retained
separately from the bounded discovery inventory so its truncation cannot discard
a verified positive match or certify an exhaustive result.
A structured URL-discovery receipt proves discovery without becoming evidence of
inventory, prices or requested filters. Coverage validates that receipt against
its current-request discovery ledger; material facts still require positive
observations. Compacted multi-page reports resolve their typed source indexes
instead of treating the wrapper's last page as the source of every observation.
Coverage also resolves citations to runtime-created source records through their
fingerprint-verified parent receipt. It does not accept model-supplied lineage
or promote an unknown citation merely because it names a valid parent.
A partial bounded source summary retains its explicit remaining outcomes and
terminal-source contract. It does not create a new broad-search obligation or
replace a specific source blocker with a generic coverage warning. A reviewer
that claims both complete delivery and insufficient bounded coverage is treated
as unavailable; it cannot certify completion or trigger new source work.

Explicit `/stop` releases the stopped conversation's retained tool-resource leases after cancellation, including browser tabs retained for follow-ups. The shared stop path scopes cleanup to `conversation:<id>` on all chat surfaces; it never requests global cleanup or closes another conversation's leases. Completed turns remain eligible for this cleanup, and repeated stops are idempotent.

Scheduler follow-up context retains a bounded identity/timing index alongside sampled detailed rows. The index preserves exact names, selection identities, enabled state and human-readable schedules from the observed source page, allowing a wording correction without repeated reads of full task instructions. It reports index completeness and source-page continuation independently; omitted details and unseen source pages never become verified by compaction.

Completion-review clarifications identify a typed input kind: source selection, source resolution, task parameters, or another missing input. A new source-selection or source-resolution gate that contradicts the runtime's verified requested read scope is an invalid review protocol response and receives the existing single correction against retained evidence. It does not trigger another account read, replace a valid answer with a needless question, grant write permission, or dismiss an actually pending source choice. Review events retain the compact source-consent flags for diagnosis.

The native paginated email adapter returns an exact `next_page_request` with its query, provider, limit and opaque continuation cursor. Producer and review compaction preserve this request. It is navigation metadata, not evidence that the next page was read or permission to invoke it; normal tool scope and boundary checks still apply. Cursor continuation avoids repeated date/category partitions when the same listing already provides its next page, and provider size estimates remain approximate.

General collection/account checks can complete as useful overviews bounded to the observed records when their checked scope is stated clearly. A remaining pagination cursor alone does not turn that goal into an exhaustive account audit. Independent requirements and review preserve explicit exhaustive listings, exact totals, absence claims and actions on every qualifying record; those still require complete corresponding coverage. Size estimates never establish an exact total.

A completion review that claims an unobserved candidate was checked, an exclusion was grounded without its discovery receipt, or a selected comparison record was observed without a bound receipt receives the existing single protocol correction against retained evidence before new research is dispatched. The correction may repair the classification or declare specific missing work; it cannot treat distinct filter/query URLs as interchangeable or certify an unobserved source. Required missing sources and invalid filters keep their normal recovery checks.
The model-facing coverage schema also restricts unobserved candidates to pending, irrelevant or blocked dispositions. Actual observed candidates remain eligible for checked. Runtime assessment retains precise binding validation for providers that ignore the supplied schema, including its existing ancillary-diagnostic handling; the schema does not create observations or merge distinct source URLs.

Search review schemas bind selected source ids to the current discovery inventory.
A complete negative or exhaustive assessment must account for every source required
by its plan; positive find-any results and bounded recommendations retain their
smaller coverage frontier. A complete review that omits required candidate rows
uses the existing single protocol-correction allowance with the exact missing ids.
The correction can classify a source from retained discovery/observation receipts,
or request genuinely missing work; it never treats an omitted row as fresh proof.

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

An explicitly accepted source-blocker alternative is represented in the search
plan by an exact permission quote from the current user request or its typed,
identity-linked user ancestry. Unrelated background and assistant-authored text
cannot supply that permission. Review distinguishes an `accepted_blocker` outcome
from actual inventory coverage: the fallback needs a current observed source
receipt and cannot certify inventory, broad absence, or unrelated requested work.
The browser adapter publishes its delegated workflow description and step-range
metadata through the cached compact capability index. This does not launch the
SDK, select a tool, grant permission, or add a classifier to ordinary chat turns.

The source discovery ledger accepts the native `public_search_results` contract
as discovery receipts. Result snippets do not count as observing linked pages;
actual content reads still bind observations separately. Once content has been
captured, presentation preserves a non-raw substantive answer from any source
adapter rather than replacing it with earlier search leads. Privacy, raw-payload
protection, independent outcome review and artifact/action checks still apply.
Source-basis explanations and citation presentation are constraints on requested
subjects; they are not additional independently sourced real-world entities.

Completion review retains a short allowance for rewriting without new evidence.
After that allowance, research can continue only when previously selected recovery
tools produced new positive content. Invocation ids or changed query arguments
alone do not renew it; repeated source quotes ignore labels, capture times and
screenshot paths. The rule is shared by source-report finalization and ordinary
or scheduled completion recovery. It does not increase the owning turn's limit,
change consent, reopen an invalid review, or certify an unfinished outcome.

Failed chat executions persist their fulfillment state. A compact unanchored
reply after an unfinished execution may use the existing structured relationship
decision with the previous assistant question and attachment metadata. Healthy
plain chat does not acquire another classification call. Selected continuations
carry bounded, reset-aware request ancestry into both execution and review.
Replacing a goal preserves identity-linked source facts as reference-only context;
it never restores cancelled requirements or treats historical consent as approval.
Older broken roots may recover a bounded window of same-workspace, post-reset
reference turns after a continuation has been selected.

Browser action preflight receives the exact observed target from the adapter's
latest same-session snapshot, including native link and form attributes. This
read-only projection avoids losing a late-page control to generic preview
sampling. Ambiguous targets and stale snapshots do not certify an action, and
native navigation metadata does not authorize a form submission. Previously
inspected retained evidence is preserved for preflight only while its source
identity remains current.

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

Filesystem searches distinguish an observed absence from missing evidence. A
filename scan returns its configured roots, exclusions, traversal error count,
and whether the scan completed. The shared filesystem evidence contract accepts
a negative result only for a complete filename scan. Content-prefix searches,
truncation, unreadable directories, and missing roots cannot establish general
absence. Presentation retains the reviewed answer instead of replacing a scoped
negative finding with a generic source-summary failure.

Scope suggestions remain subordinate to the independently enumerated material
outcomes. After action preflight rejects an unrequested proposal, completion
uses that same cached outcome contract before enforcing missing tools. A bound
completed execution review prevents optional outer retries from reopening the
request; explicit source-read dependencies still take the recovery path. Batch
scheduler actions expose separate records for each successful object operation,
so distinct requested actions cite distinct results without repeating inventory
reads. A failed sibling cannot borrow a successful operation's evidence.

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

Current source artifacts become attachment candidates only when the independent material requirement contract requires a deliverable, names their completed producer as eligible evidence, and requests their format. The shared artifact handoff validates roots and file identity before promoting source descriptors, then uses the existing final-version selector and review. Capture fast completion uses the same cached contract. An unselected required attachment cannot produce a successful execution receipt.

Archive extraction emits a bounded typed execution record and declares its source-record identity. Optional delivery path fields are absent when no manifest output was requested, so empty attachment metadata cannot invalidate a successful extraction. Both context compactors retain that receipt. Completion review binds adapter-declared source records from the original receipts before projecting away unselected artifact versions; this preserves prerequisite proof without making intermediate files deliverable.

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

Scheduled report producers and tools-disabled recovery use the same optional
`nullion.report.v1` content contract in `chat_text`. It compiles into the existing
`UserMessage` renderer before completion review. Primary findings appear first;
required details always survive compact formatting. Repeated outcomes and source
labels have explicit identities and render once. Optional historical expansions
appear for detailed reports; required titles, uncertainties, amounts, deadlines,
and requested provenance remain visible in either mode.

The same contract passes through delegated scheduled tasks and grouped report
summaries. Transport adapters only render, escape, split and deliver the resulting
message. Attachment cleanup preserves paragraph and list boundaries. Invalid
presentation metadata falls back to usable public content without another model
call or a new publication gate. Plain chat and legacy plain-text reports retain
their existing paths. Artifact contents and completion decisions remain separate
from presentation.

The requirement planner and completion reviewer receive the same typed report
presentation policy, so optional expanded source titles cannot become mandatory
work during recovery. That cosmetic policy is excluded from the material-outcome
cache identity. JSON file reads declare their machine-readable format; bounded
review projection binds exact JSON objects and pointers to that existing read.
Separate document records can support separate subjects without rereading the
file or relaxing positive-evidence and duplicate-proof checks. This projection
never promotes a local file into proof of live external account state.

Source-answer grouping retains explicit title and field selections across a
cosmetic repair for the exact same evidence set. A grouped point's advertised
schema requires title identities. Repair diagnostics list compatible captured
roles and same-capture entity anchors; they never reclassify source records or
join different snapshots. Partial recovery first keeps an independently valid
quoted field group together before separating incompatible records. Specific
selected source limitations remain visible without an additional generic assembly
failure note; completion stays false when answer preparation failed.

Partial grouping requires one captured record quote containing the title and all
selected fields; a shared page alone cannot establish a relationship. Detached
fragments appear as supporting details rather than new entities. Source citation
numbers are stable across the answer, its introduction and partial recovery.
Source-answer reviewers use the same shared presentation policy as scheduled
reports, including scoped shared limitations, without relaxing factual proof.

The completion reviewer can repair composition in its existing call by returning
`reply_target: corrected_findings` and certified findings containing the shared
`nullion.report.v1` envelope. The runtime renders that contract, requires exact
current requirement and evidence bindings covering every delivery-required
outcome, and applies the existing execution, source/search, consent and artifact
checks. Only a complete, grounded correction replaces the draft. Its review is
cached against both the original and corrected text plus request/evidence state,
so final delivery does not review the same correction again. A changed request
or evidence invalidates it. Partial findings can use the same layout without
promoting the incomplete verdict. Legacy prose remains supported.
Scheduled report delivery consumes the same approved replacement. Selected-file
review bindings include a streamed content digest so a same-size rewrite with a
preserved timestamp cannot reuse a stale completion verdict. The source-answer
presentation policy survives runner handoff, avoiding a redundant review merely
because the in-memory selection checkpoint is absent there.
An execution-only prerequisite must declare a usable evidence tool or artifact
package member. An empty proof path is rejected by the planner schema and
runtime parser and corrected in the bounded contract-planning step; it must not
reach the reviewer as an impossible-to-satisfy requirement. This does not waive
proof, alter an unmet requested outcome or invoke a source again.

This is a presentation repair path, not independent factual verification or a
guarantee that a model judgment is correct. Missing actions, source observations
and files still require execution or a concrete limitation. Ordinary requests
gain no extra classifier or model call.

Reviewer-published findings from tool workflows must use the shared report
envelope, including blocked and partial results. A partial verdict does not opt
out of the compact layout. The reviewer schema retains independent citation
validation and permits short plain-text limitation rows. Legacy provider prose
is preserved as a marked layout fallback, without another formatting call or a
new completion block; review events record the required layout, fallback flag
and actual finding formats. Structured reports and source answers use the same
report role for saved emoji preferences.

The review layout policy comes from saved user preferences, rather than the
producer's already-expanded draft. Optional specifications, instructions and
metadata are expansion, not additional user requirements. Quoted assistant
answers remain reference context; identity-linked user requests are retained
alongside them so an earlier assistant catalog cannot replace the user's goal.

An explicitly selected parent with `response_fulfilled=false` carries its
unfinished goal and previously used tool names through the same `pending_request`
contract as clarification recovery. Identity-selected ancestry supplies the
original goal when the parent was itself a short continuation. The normal
outcome review then checks a no-tool acknowledgement against that goal. Current
cancellation, replacement or explanation-only instructions still take priority;
this does not enable parent scans or extra reviews for unrelated ordinary chat.

Source selection's per-point character target is display guidance, not factual
validation. Complete quoted fields still reach the common outcome reviewer even
when they exceed that target. After a bounded source-selection schema repair is
exhausted, the common terminal reviewer may publish independently grounded report
findings or a certified corrected report. Literal excerpt salvage cannot replace
that reviewed answer. Model timeouts, cancellation, consent and semantic rejection
retain their existing protections; unavailable review preserves safe partial
content without granting completion.

A reviewer that labels an incomplete verdict as `corrected_findings` does not
produce a completion certificate. Normalize that composition target to the
partial-publication path and retain independently grounded findings; do not
throw away the entire review or repeat it for that field alone. A complete
corrected report may include a grounded limitation alongside its findings, but
only findings establish required-outcome coverage. All required execution,
source, consent and artifact evidence still has to pass. Terminal review cannot
reopen execution after its existing recovery iteration allowance is exhausted.

Scheduled inline reports retain the latest validated, evidence-referenced public
draft while execution recovery continues. If a later writer fails, the runner
may deliver those findings through the common message renderer with a short
unfinished-checks notice. The runtime-owned receipt binds the original request,
draft and existing tool-result versions; later completed writes, changed or
removed evidence, cancellation, consent or source-selection blocks invalidate
it. This fallback cannot satisfy a required file or authorize a match-only alert.
Delivery remains partially succeeded with completion unverified; timeout and
iteration diagnostics remain intact. Report-writer inspection, protocol repair
and transport retry share one 60-second allowance instead of restarting the
timeout for each call. This does not reduce the research/tool iteration budget.
Chat, Web and scheduled turns use `agent_turn_limits.store_budget_approval` for
the same durable budget choices. At an exhausted scheduled execution budget,
known remaining work and retained source evidence permit a Doctor approval for
additional 2x, 5x or 10x model rounds, or denial. A writer/reviewer timeout alone
does not create a budget request. The checkpoint retains messages, source and
action receipts, invocation arguments, artifacts, browser session identity and
the exact saved job revision. Budget grants do not grant tool permissions or
alter schedules. The single scheduled-continuation dispatcher resumes through
the original delivery graph in the existing background-work mechanism; approval
does not wait for the model. Durable claims and execution leases prevent duplicate
execution across Web and messaging. Manual runs retain both their isolated agent
conversation and the original user-visible delivery conversation; internal run
identifiers are never used as the approval's destination. Scheduled, manual and
resumed runs use one stored-outcome policy. A changed/deleted job, changed target or
workspace, or newer scheduled run invalidates the older continuation. Denying
additional work leaves the recurring schedule enabled. A claimed continuation
interrupted by process termination is not automatically replayed when action
outcomes may be unknown.

Source-answer composition distinguishes `verified_results`, `partial_results`
and `no_verified_results`, and marks points as requested results or supporting
context. Context does not render as a candidate or count toward candidate
coverage. The writer may provide concise summaries and limitation explanations
grounded in selected source identities; literal page excerpts remain evidence.
`PreparedAnswer` retains the writer's immutable `ExecutionOutcome` and its
request/evidence binding. Optional review can assist or correct that answer;
review failure cannot rebuild it into a raw excerpt list. Selected source text
without an authored answer is not a fallback composer. The shared message
renderer and transport adapters remain responsible for presentation and delivery.
New evidence, a changed request or changed selected file bytes invalidate the
prepared answer. Partial selection recovery keeps only valid authored points and
authored limitations; invalid cross-source joins are not split into a fresh dump
of independent fragments.

The terminal handoff also carries grounded partial findings and their causal
failure bindings. Delivery adapters must not infer that an older connector or
browser failure caused the current result after the owner has bound the answer
to newer receipts. This partial receipt does not certify completion or suppress
a currently bound blocker. Review cache keys include the resolved material and
delivery contract; resolving that contract during the first review must not
trigger an identical second review. Evidence inspection and protocol correction
consume the same elapsed review allowance rather than resetting it per call.
Acknowledged output with performed work and unknown semantic completion remains
partially succeeded without an invented source-failure reason. Doctor records
it as delivered, with no verified-success timestamp, when only the optional
helper is unavailable or advisory. Actual failed calls, missing requirements,
unfinished producer work and conditional alerts retain their failure handling.
Grounded helper corrections own the terminal answer; an older prepared draft
must not overwrite them during failure preservation.

The browser adapter can report pending controls by captured source/control
identity. A current enabled prerequisite keeps a partial browser step resumable,
even if its report requested termination. Disabled, stale or unknown control
references do not cause continuation. This signal permits neither purchases nor
automatic UI mutations; the normal browser executor and permissions still apply.

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

The shared report renderer accepts typed title ownership. Scheduled producers,
review-authored corrections and scheduled recovery suppress their inner report
title and role prefix because delivery owns the enclosing scheduled header.
Ordinary chat titles remain; explicitly required content titles also remain, using the typed title_is_required flag. Compact writing
keeps a fact or total once, concise identifying provenance, and material
uncertainty; incidental identifiers and process narration are optional detail.

Source-selection identity is bound into the review context and carried in the
internal result handoff. The outer publication review restores that context
before checking its receipt, avoiding both reuse across changed selections and
unnecessary re-review of an unchanged source answer.

Requirements planning and terminal review share outcome-owner guidance: compare
the user's goal, amendments and constraints with the usable delivered result.
Incidental collected attributes do not become acceptance criteria. Optional
unknowns may be disclosed; requested or decision-critical facts, consent,
execution and artifact evidence remain necessary. Editing defects should be
corrected using retained evidence in the current review rather than reopening
completed source work.

Report sources have an optional typed `url`, separate from their short label.
The common renderer keeps those targets in compact and detailed output. Review
also supplies exact URLs from runtime-bound source records to the renderer for
each finding's evidence references. If a rewritten report drops its links,
those cited targets remain as concise source links. Unselected records,
navigation inventories and guessed label-to-URL matches are never added. This
is presentation repair, not new evidence, a model call or a fulfillment gate.

The ordinary graph finalizer prepares and validates the selected content, then
enters `_complete_agent_turn`, the same review/recovery/publication boundary used
by shortcut exits. It does not independently review, retry and grade the answer
before calling that boundary a second time. Format-specific source selection and
scheduled report composition still have preparation strategies; these must retain
receipts and hand off to the common boundary rather than become adapter policy.

Scheduled content production is an optional `CompletionDraft` strategy invoked
inside that owner. It can compose text, retain source/action receipts and append
verified file-production receipts. It cannot run tools, approve actions or
resume execution. Inline composition consumes the owner's cached material
contract; recovery receives the current draft and structured review together.
The scheduler projects the terminal result into delivery state and resumes only
an explicitly approved saved execution. It does not review or reauthor that
result again. A current-byte-bound partial content receipt stays partial even
when a later editorial check or upload succeeds.

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

The typed `RecoveryCheckpoint` carries the same request's spent improvement
attempts, relevant evidence fingerprints and proposed recovery tools through
review and scheduled-execution handoffs and producer resume. Changing the user,
conversation or request invalidates it. It does not change the configured model
round allowance, expose new tools or bypass action approvals. New receipts may
advance the existing progress-based recovery policy; rebuilding a runner alone
cannot replenish its window.
Scope retries carry the same checkpoint and latest receipts too; refreshing the
scope wrapper cannot discard later work or create nested recovery owners. Legacy
execution receipts use the same file-byte fingerprints, so same-size rewrites
that preserve timestamps still invalidate stale review ownership.

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

Accepting an already generated report is a file inspection, not a new producer
write. It retains the original producer receipts and their selected-version
manifest through timeouts, budget continuations and final review. A synthetic
production receipt is appended only for bytes actually generated by the content
strategy. This keeps file selection separate from content-completion grading
and avoids invalidating a valid attachment merely by inspecting it again.

Activity rows use the shared run-activity renderer on every surface. Only
allowlisted structured summaries, such as message/event counts and a deferred
run receipt, supplement the tool name and status. Arbitrary tool output, raw
errors, URLs, filenames, and internal repair instructions remain in execution
evidence rather than being copied into activity cards or final reply traces.

Imported spreadsheet tables are data, not tool-control arguments. A source JSON
column named Image, image_path, or screenshot stays a literal column, as do
explicitly selected data columns in inline rows. Embedding uses explicit
image_paths/screenshot_paths or row media bindings outside selected columns.
This prevents archive inventories from promoting filenames into image requests.
Native artifact results retain the invocation's typed visual requirement in
runtime-owned response_requirements metadata. A failed optional illustration
cannot establish a new mandatory image obligation. Required visuals and legacy
receipts keep their existing checks; delivery review preserves this provenance.
The same rule covers PDF, DOCX, PPTX, and XLSX generation.
Native format requirements follow the actual structured output extension,
including PDF and HTML exports through document_create. They are not inferred
from the tool's default format. Plain CSV/TSV remain data-only exports.
The same typed contract reaches LangChain/Deep Agents tools, their declared
fallbacks, and direct native fallback operations. Error enrichment and scheduled
review retain runtime requirement and recovery metadata instead of reconstructing
incomplete tool results. Dependency payloads do not supply task authority.
HTML aliases use the shared format normalizer during repair and file counting;
missing paths and directories cannot masquerade as completed artifacts.

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

Required native action receipts are execution evidence for the shared completion
review, not prose that must be appended verbatim. The owner reviews authored
copy before receipt supplementation. A successful, grounded review binds that
exact copy to its request and complete native evidence using the existing outcome
fingerprint; subsequent shared sanitization preserves it without a second
acknowledgement. Changed copy, requests, action facts, or invocation identities
invalidate that presentation annotation. It grants no execution or delivery
authority. Unreviewed/raw replies and helper outages retain the native receipt
fallback, alongside useful findings. Deep Agents use the same owner and fallback.
Runtime metadata survives error enrichment, serialization, and scheduled
inspection; provider payload text cannot supply this annotation.

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

The shared outcome policy distinguishes requested subjects/properties from
contextual links and reference material. Finite descriptive research uses bounded
subject coverage; a contextual page does not introduce mandatory metadata checks
unless that data is needed to identify a subject or execute the requested action.
The independent review rejects both narrowed and expanded search plans through
its existing typed scope-mismatch decision, rebuilding the requirement rows from
the original request instead of sending new source work for invented obligations.

Review responses retain a correct draft without restating it as supported findings.
Bounded source assessments emit only decisions required for selected/restricted
sources and material frontier contenders; omitted alternatives remain pending.
The runtime still requires full candidate accounting for exhaustive/negative
conclusions and every requested fact's evidence. This reduces redundant reviewer
output without adding calls, relaxing proof or increasing execution budgets.

A published browser report also survives a subsequent nonzero worker exit.
The parent reads an available atomic native report independently of process
shutdown status and retains concrete worker errors as errors. Owner cancellation
is checked again after exit, so cancellation cannot lose a race to late publication.
Missing/malformed reports still fail, and existing resource cleanup remains active.


Browser reports preserve quote-validated evidence of source blockers, including
captured disabled control values and dates. These records carry the runtime-owned
`evidence_kind=source_blocker` marker through compaction. A blocker-only report
remains blocked; it does not establish inventory availability or zero results.
Previously captured useful facts survive a later blocked page. A permitted blocker
outcome still requires the request-owned review and current captured source proof.

Read-handler timeouts and empty search result sets declare invocation-scoped
failures. Recovery preserves the reader for other sources or queries, including
its visible scope receipt. Tool-wide outages retain their retirement behavior;
identical failed calls remain subject to existing bounded recovery.

An unambiguous package with one completed native producer for each explicitly
requested format can retain its selection even when a terminal failure replaces
the draft caption. Existing manifest exclusions, revised versions, and multiple
candidates in one format still require their existing structured selection. File
selection does not certify content completion: failed reviews remain disclosed.

The shared planner/reviewer guidance treats permitted alternatives as branches
of the same outcome. It must not turn an either/or request into mandatory work
on both branches. Independent actions, files, consent and execution constraints
remain mandatory. Completed native artifact fallbacks are evidence of the file;
an unexecuted rejected generator is not an additional user obligation.

The shared final presentation boundary applies the same sensitive-value redaction
before adapter delivery and durable chat storage. Redaction preserves Markdown
link boundaries, following URL parameters, and quoted secret delimiters, and is
idempotent across repeated delivery/history passes. No classifier or formatting
model call is added to ordinary turns.

Media settings changes refresh the registered media adapters as well as configuration metadata. Web settings saves and the shared Telegram/Slack/Discord service reload use the same refresh helper. Enabling generation exposes its tool without restarting; disabling removes it. The messaging service fingerprints media configuration and rebinds only when that configuration changes, preserving ordinary-turn and unrelated-setting fast paths. Existing file, consent and transport checks still apply.

Missing-file failure content is shared by fulfillment, artifact finalization and platform delivery. When a producer returns a typed HTTP rejection, each boundary delegates to the same actionable explanation instead of replacing it with a generic attachment notice. The latest failed producer receipt takes precedence over an earlier corrected-plan rejection. This changes failure content only; attachment requirements, valid-byte selection and completion status remain enforced.
