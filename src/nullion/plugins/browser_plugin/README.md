# Browser sessions and visual tasks

Low-level tools such as `browser_navigate`, `browser_snapshot`, and
`browser_extract_items` operate on retained sessions/pages. They share the
configured backend and preserve trusted session identity across related reads.
Temporary [live trackers](../../../../docs/live-tracking.md) use these narrow
read tools when suitable; they do not invoke Browser Use on every poll.

## Optional Browser Use worker

The optional `browser_run_task` tool uses Browser Use 0.13.10 with Nullion's
configured model. Its adapter marks it `explicit_scope_only`: a structured plan
or `request_tool_scope` call must select it by exact registered name. Broad web
scopes and reader continuation metadata do not implicitly add this heavier
worker. Existing low-level readers remain available, and an interactive workflow
can request the worker when it needs visual navigation. This uses registry
metadata without adding a classifier call to ordinary chat; idle chat never
imports Browser Use or starts a browser.

New installations prepare the worker automatically. Existing installations can
run the plugin setup with their application Python:

```sh
python -m nullion.plugins.browser_plugin.browser_use_setup --home ~/.nullion
```

The active lane automatically discovers `browser-use-venv`; an explicit
`NULLION_BROWSER_USE_PYTHON` overrides it. Setup installs Chromium too and checks
worker dependencies without changing the app environment.

For manual setup, install the dependency in a separate virtual environment using
`requirements-browser-use.txt`, then set `NULLION_BROWSER_USE_PYTHON` to that
environment's Python executable. Browser Use pins some SDK/Pillow versions that
conflict with Nullion's main environment, so install it separately. The worker
uses its own dependencies first and falls back to app-only packages from the
parent environment. Model calls use an authenticated ephemeral loopback bridge
to the parent's configured client. No model credentials are passed to the worker.

Each job launches an isolated Chromium profile. Browsing is visible on desktop
systems and headless on Linux when no display is available. Install
Chromium with the worker environment's `python -m playwright install chromium`.
`NULLION_BROWSER_HEADLESS=true` selects headless browsing, which some sites reject.
`NULLION_BROWSER_USE_EXECUTABLE_PATH` may select a compatible local Chromium
binary. The sandbox defaults to enabled; containers which require an unsandboxed
browser must explicitly set `NULLION_BROWSER_USE_SANDBOX=false`.

`NULLION_BROWSER_USE_CDP_URL` can attach to a dedicated browser owned by this
adapter, including a headed browser inside a container. Do not point it at a
personal browser: jobs navigate its active tab. The adapter detaches from an
explicit CDP endpoint and kills only browsers it launched itself. Access to a shared CDP endpoint is serialized between Browser Use jobs across Web and messaging workers. Reserve that endpoint for this adapter; other browser tools should use a separate browser.
A concurrent job reports a busy resource instead of mixing its pages with another request. Browser Use jobs are bounded to 600 seconds and
at most 30 steps; the default is 20 steps.

A task supplies a URL and objective as structured tool arguments. Navigation
and redirects are limited to that target host. Browser policy is checked before
startup; private IPs and blocked domains remain restricted. File upload, raw
JavaScript, filesystem, PDF generation and separate extraction-model actions
are excluded. This tool is for reading/comparing pages, not purchases, sign-in,
or external messages.

Each browser step supplies a bounded, runtime-captured page-state record before
model inference. The model can cite its source and line indexes immediately;
`read_page_state` supports literal `query` searches with surrounding text and
original line indexes, avoiding sequential scans of long pages. It also remains
available for additional lines or inspection. Page
content stays inside an untrusted-data boundary. Up to three actions may share
one model step, subject to the SDK's navigation and stale-state safeguards.
This changes only explicitly selected browser tasks, not ordinary chat.

SDK actions that return extracted fields alongside a compact memory summary
preserve those fields for the next model step. This keeps element attributes and
other action data available for reasoning while retaining the shorter summary
for subsequent history. It does not treat the summary as the full extracted result.

The tool captures screenshots automatically and retains only evidence used by
the report plus the last page/blocker capture. Exact quote/value matching grounds
observations against captured DOM and rendered page text. Unknown requirements and partial findings
remain visible; the library's success boolean is not proof of task completion.
The outer chat model summarizes the observations. PNG evidence is delivered
only when the typed request asks for image attachments; otherwise screenshots
remain internal evidence. Intermediate profiles, logs and model transcripts are not delivered.

## Repeated checks and service restarts

Missing or truncated item scopes expose bounded `available_scopes` with CSS
selectors observed in the rendered DOM, visible link counts and sample text.
The agent chooses and tests the relevant scope; these hints certify neither
editorial meaning nor complete coverage. Complete scoped reads avoid rebuilding
this discovery inventory. Duplicate image/title links retain the image-rich
record while preferring a semantic heading or text-link companion over its image
credit for the title. This behavior uses DOM structure and opaque URL identity,
without provider-specific paths, class names or English headline rules.

A browser-backed tracker carries an adapter-verified page setup into repeated
reads. Complete text and item reads can restore their source in a worker-owned
tab after a restart. Existing page state can update without reopening the browser;
full-text monitoring reads require `ready_selector` or `ready_text` from an
observed stable results landmark. The worker waits for that criterion after
navigation, rather than reading a client-rendered page shell. Ordinary text
reads remain available without this polling readiness contract.
New trackers default to `source_refresh: "navigate"`, refreshing the saved URL
in that same worker tab on each check. Explicit `source_refresh: "reuse"` is for
a verified self-updating page; it retains the loaded DOM. Locator discovery returns clipped labels with explicit truncation
metadata and cannot substitute for a complete semantic monitoring source. Source tools
remain subject to policy, bounded timeouts and workspace ownership. Browser Use
is delegated multi-step work and cannot be a live probe.

A running session can survive multiple checks, but an in-memory page is not
guaranteed to survive an app or browser restart. Persistent tracker metadata
does not manufacture a valid login or page after the browser disappears.
A source-access problem pauses the tracker rather than reporting no match.
Semantic `snapshot` history retains the last successful source read across
checks and restarts; failures do not advance it. Collection identity history
remains available for newly appearing records.

### Source meaning and answer composition

Captured observations carry an optional `semantic_role`: `entity`, `attribute`,
`interface_control`, or `context`. New browser observations should supply it from
page structure. A page action or section must not become a separate real-world
subject merely because its text is quoted exactly. Roles remain bound to their
source capture through compaction and answer selection; older observations stay
readable.

Source answers declare the roles requested by the user and the role of each
standalone point. Supporting fields stay attached to their subject. Controls may
be standalone findings when the request is about controls. Explicit summary
coverage prevents repeating an item already answered in the introduction. The
existing outcome review checks relevance, roles, duplicates, and material
limitations. A rejected presentation gets one correction using the same source
capture; it is never restored as a fallback or used to restart browser work.
An answer without an introduction may omit `intro_evidence_ids` or provide an
empty list. Nonempty introductory citations must reference known evidence.
Rejected selections record a durable validation reason for runtime diagnosis.

When an independently reviewed summary is useful but another requirement remains
unavailable, recovery retains that summary and its selected limitations. It does
not reconstruct the answer from raw fields or repeat summarized points.

If the configured CDP browser has enough existing targets to exceed the SDK's
startup event capacity, Browser Use runs that read-only lookup in a fresh,
headless temporary browser instead. Existing tabs are preserved; cookies and
sign-in state from the shared browser are not copied. The result records this
recovery explicitly. Normal CDP jobs keep their configured session behavior.

The native CDP fallback evaluates JavaScript functions with their arguments,
including async functions, instead of returning a function object as an empty
result. It preserves ordinary expression/program results, propagates JavaScript
exceptions, and releases remote handles after serialization. This also keeps
independent page-URL reads usable for source bindings on extracted text. The
fallback is covered against real Chromium and the shared chat/artifact boundary.

The full visual worker declares `required_scope_only` as well as `explicit_scope_only`. A scope listing it as an optional alternative does not expose it alongside mandatory lightweight readers. When the structured workflow actually delegates to it, select its exact name in both `tool_names` and `required_tool_names`; later exact scoping remains available. This avoids starting a several-minute worker before the ordinary reads selected for the same request.

Native item collections declare their record path and title/URL identity fields in `source_record_collections`. History compaction can then retain identities across a large collection without treating a clipped JSON preview as complete source evidence. Direct prices and other mutable values still require actual captured records; identity paths alone do not prove them.
