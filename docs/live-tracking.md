# Live trackers, reminders, and recurring jobs

Live trackers watch a changing source for a limited time. Use them for a platform
assignment, a short booking window, or another condition that needs frequent
checks and an end time. Availability depends on a readable, configured source;
a tracker cannot bypass a login, website restriction, or missing permission.

## Choose the right kind of work

| Need | Use | How it runs |
| --- | --- | --- |
| Notify me once at a known time | Reminder | Waits until its due time |
| Repeat a task on a continuing schedule | Cron | Starts the saved assistant task at each scheduled run |
| Watch a condition frequently for a short window | Live tracker | Prepares a read-only observation once, then repeats that observation until the deadline |

Nullion selects the route through structured task/tool scope. If temporary versus
recurring work, the condition, or the tracking window is unclear, it should ask a
numbered question before creating anything. A reply such as `1` selects an option.

## Start from chat

For example, after identifying the train and its live source:

> Watch this train's platform assignment every 15 seconds for the next two hours.
> Alert me once a platform is assigned, then stop.

Give enough information to identify the source and event. You can give an end
time instead of a duration. The assistant verifies the source before starting
and confirms the tracker's name, interval, deadline, and delivery chat.

There is no dedicated `/track` or `/live` slash command. The agent tools are
`create_live_tracker`, `list_live_trackers`, `stop_live_tracker`, and
`update_live_tracker`, and `resume_live_tracker`. These tool names are not chat commands.

## Limits and lifetime

- Default interval: 15 seconds. Supported intervals: 5 seconds–6 hours, independently of the chosen tracking lifetime. One-, five-, fifteen-, thirty- and sixty-minute checks are supported. A finite monitoring window remains a live tracker at slower intervals; use a cron for an ongoing recurring schedule.
- Lifetime: any positive finite duration, or an exact future end time with a time zone. An end is mandatory; there is no product lifetime maximum. Relative durations start when the setup is accepted, while absolute clock deadlines stay fixed. Trackers never become indefinite schedules. Timing and alert conditions can be amended with `update_live_tracker` while keeping the same tracker and source. A filter-only change preserves the deadline, cadence and previous source snapshot; it does not restart or extend the window.
- By default, a matching update sends an alert and tracking continues until the
  deadline. No per-alert Continue choice is required. Snapshot conditions compare
  with the immediately previous successful observation, including changes back
  to an earlier value. Unchanged results stay silent, and article/record monitors
  retain durable item deduplication.
- An explicitly requested one-shot tracker uses `stop_after_match=true`. It stops
  after the first match and retains its optional continuation choices. Existing
  saved one-shot trackers keep that policy until explicitly amended.
- No-match checks stay silent. By default, expiry sends one closure notice;
  `notify_on_expiry=false` suppresses that notice.
- Three consecutive failed or inconclusive checks pause the tracker and send
  one attention notice including the specific recorded cause, how to fix it,
  and the original deadline. A successful no-match check stays silent and resets
  the failure streak. The first two unsuccessful checks retry with backoff.
- A required approval or denied source access pauses checks immediately and
  sends an access notice. Resume after resolving access. A failed check never
  means no seats, no platform, or no availability.
- Resume keeps the original deadline for paused, stopped, or matched trackers. An expired
  tracker can reuse its saved source and condition with a new user-selected
  window. If no duration was supplied, the assistant offers numbered continuation
  choices rather than retrying an invalid resume. A retried renewal request
  never adds the same time twice.
- Expiry notices offer numbered choices: continue for 1, 2, or 4 more hours, or
  keep stopped. Reply with the number in the same chat; if other messages have
  arrived, reply directly to the expiry message. If several expiry menus are
  pending, a numbered tracker list lets you select the target before applying
  your chosen action, including in Web chat. Choices are valid for 24 hours.
  Extra time starts when selected and preserves the source, condition and check
  frequency and match policy. Continuous extensions keep checking for new
  changes until their new deadline and then send an expiry notice. Repeated replies never add the same extension twice.
- Each workspace may have up to 16 active or paused trackers.

The requested interval is a target, not a guaranteed notification latency. Source
response time, available workers, rate limits, and chat delivery can add delay.
Missed checks do not build an unlimited catch-up queue.

## Reuse and browser state

Each poll repeats the saved probe; it does not start a cron or a fresh assistant
conversation. Structured conditions compare typed fields without a model call.
An optional semantic condition uses a bounded, tool-free model evaluation.
Matches must cite an exact value or an exact quote at a verified source JSON
pointer. Text containing JSON is still text; invented pointers into it do not
count as evidence.

Browser item observations save a verified source URL as well as the read. The
background worker opens that source in its own tab. New trackers default to
`source_refresh: navigate`: each check reloads the source in the same tab, so
static pages can expose newly published data. This does not restart the assistant
or open a new tab each time. Explicit `source_refresh: reuse` is available for a
verified self-updating page and preserves its loaded state. Existing saved
refresh choices remain unchanged. It does not rely on a creator tab
handle surviving across Telegram and Web processes. After a worker restart it
can rebuild the page from the saved source recipe. A verified missing worker
page also invalidates its startup receipt and triggers one bounded reload.
Different prepared page recipes use distinct worker tabs. Equivalent recipes
and their explicit prerequisites share their own tab; one source cannot replace
another source's DOM on later checks. Completed prerequisites are reused only
for the target on which they actually ran.
Browser profiles and access
permissions still apply; saving a URL does not grant new access.

Other adapters can declare bounded READ prerequisites through the same source
replay contract. Reads without such a recipe retain their existing setup
behavior. Changing a section selector, signing out, or losing access can still
require a repaired observation. Three consecutive failed checks still pause
tracking and send the recorded cause and a next step. A creation receipt says
when the first background source check is pending; it does not prove that check
has already succeeded. Job-owned tabs are released when tracking stops, pauses,
matches, or expires; resuming reconstructs the saved source.

Delegated `browser_run_task` jobs are not repeatable live probes. They perform
full browsing tasks and may launch isolated profiles. Live probes must use
bounded dedicated read tools and cannot run shell commands, writes, generic
agent gateways, or artifact/screenshot generators.

## Manage in the Web UI

Open **Settings → Crons → Live trackers**. The dashboard also shows trackers.
You can inspect the deadline, state, last check, check count, specific failure
reason and next retry, then Stop or
Resume eligible trackers. The separate **How many live checks can run in
parallel?** setting supports 1–16 workers and defaults to 4. Changing it does not
change recurring-job concurrency.

To stop through chat, name the tracker or reply to its confirmation. If several
trackers could match, choose the one intended before stopping it. `/stop` stops
active conversation work; use a tracker Stop control or an explicit tracker
cancellation to end a persistent watch.

The Web service owns the live worker and must remain running, including for
trackers created through Telegram, Slack, or Discord. Jobs and deadlines are
stored in the instance's `runtime.db`. After a service restart, eligible jobs
resume from stored state; expired jobs stop. An in-memory browser page may need
reconnection and is not guaranteed to survive a browser restart.

## Delivery

Alerts return to the originating workspace and chat. Delivery retries are
bounded: a match is sent only before the tracker deadline and within 60 seconds
of detection. An alert that has become stale is marked undelivered instead of
being presented as a fresh result. The Web panel shows delivery-retry and
undelivered status. Source-attention notices remain deliverable until the
tracking deadline and are canceled when tracking resumes or stops.
Expiration notices retry for up to 24 hours, independently of the tracking
window. A missing setup acknowledgment does not suppress the terminal expiry
notice. A successful platform receipt means the service accepted the message;
it cannot prove that a device displayed a notification or that the user read it.
Telegram receipts carry the notice id for correlation with the platform message id.
No daily summary is created.

## Local API

These routes use the Web server's local-admin boundary. The admin list can
inspect all workspaces; agent tools remain scoped to the initiating workspace. Keep the server on a
trusted local interface; these are not public unauthenticated webhooks.

| Method | Route | Purpose |
| --- | --- | --- |
| GET | `/api/live-trackers` | List trackers and limits; optional `workspace_id` filters the admin view |
| POST | `/api/live-trackers` | Create from a prepared read-only probe and matcher |
| PATCH | `/api/live-trackers/{id}` | Amend the saved condition, deadline, cadence or continuation policy in the original chat |
| PUT | `/api/live-trackers/settings` | Set `max_parallel_jobs` from 1 to 16 |
| POST | `/api/live-trackers/{id}/stop` | Stop one tracker |
| POST | `/api/live-trackers/{id}/resume` | Resume before its original deadline |

Creation accepts `name`, `duration_seconds`, optional `interval_seconds`, `probe`,
`matcher`, optional `notify_on_expiry`, and a Web `conversation_id`. The
`Idempotency-Key` header makes an API creation retry reuse the same tracker.
A structured matcher uses 1–8 conditions with a step index, an output-relative
RFC 6901 JSON pointer, a typed operator, and a human summary. Missing fields are
unknown, not a negative result. The chat path also binds the probe to verified
same-turn observations; do not invent field pointers or session handles.
Setup and polling share the same lossless observation budget: 18,000 characters
per read and 55,600 for the combined semantic packet. An over-budget prepared
read is rejected at setup; later growth records its actual size and limit in
the failure reason. Evidence is never silently truncated to produce a match.

## Recurring trackers and Doctor

A cron configured to alert only on matches continues to use the recurring
scheduler. Its Doctor monitoring is separate from live-tracker error handling.
Under Crons, configure Doctor monitoring and the streak threshold (default 12).
Doctor distinguishes successfully completed no-match checks from failed checks,
and sends one notice for a continuing streak rather than a daily digest.

Semantic checks use the configured model with a 2,048-token output budget
(including reasoning) and a 30-second request budget. Explicit medium/high
reasoning settings use a separate low-effort evaluation profile; models without
explicit reasoning settings keep their normal request format. This does not change chat reasoning settings or require another
provider. Polls never overlap for the same tracker; its next interval begins
when the current check finishes. Truncated or timed-out evaluations count as
unknown, never as a successful no-match. Three consecutive unsuccessful checks
still pause tracking and send one cause-and-repair notice. Evaluation timing
and token-budget diagnostics are retained with the latest tracker evaluation.

Model connection changes saved in Settings also update existing live trackers
for their next check, without creating a new tracker or extending its deadline.

### Section-based source checks

Browser item observations retain their enclosing section heading and a coverage
record. A section label applies to its items; it need not be repeated in each
headline. Narrow a section-based tracker to the verified region rather than
assuming a ranked homepage sample covers it. A missing selector fails explicitly.

Semantic evaluations must identify the section they checked before returning
`no_match`. The runtime verifies that all items from that section were returned.
A missing or incomplete scope is an unknown check, preserving the three-failure
pause and repair notice. Positive matches still require exact current-source
evidence. Coverage metadata is retained with the latest evaluation diagnostics.

Use the structured `section_heading` argument to `browser_extract_items` for a
labeled region. It resolves a visible heading in any language and keeps links
from its enclosing region. The optional `selector` accepts native CSS only.
Duplicate URLs retain additional section memberships, and shared images do not
collapse distinct articles. Semantic chat setup evaluates the prepared evidence
once before saving: unknown or incomplete evidence must be repaired first. This
validation runs only during semantic tracker creation, never on ordinary chat.

Source preparation reports whether each exact read is ready for polling. A
successful browser read can still exceed the bounded observation size; the agent
must narrow that read and verify it again before creating the tracker. Setup and
polling use the same Unicode-preserving size calculation. Invalid condition
indices and pointers return the exact field needing correction. A missing page
region keeps the existing tracking scope while the agent inspects the page and
repairs its selector; repeating an unchanged failing plan remains bounded.

Semantic collection trackers also retain stable record identities across
successful polls and worker restarts. A source adapter declares
`change_identity_fields` in its structured collection metadata; native browser
article collections use the canonical URL. The evaluator receives runtime facts
showing baseline, previously observed and newly appearing records. Editing a
headline, reordering records or changing a relative-time label does not create a
new article identity. An empty verified collection still establishes a baseline.
Unknown or incomplete evaluations do not advance that history. History is
bounded; reaching its limit pauses with an actionable reason rather than
silently forgetting old records.

For changes to existing values or page text, a semantic matcher can select
`history_mode: "snapshot"`. The evaluator then receives the previous successful
observation as well as the current one. A complete first observation establishes
the baseline without alerting. Chat setup seeds this snapshot only after its
prepared source passes evaluation, and worker restarts retain it. Failed checks
do not overwrite a valid snapshot. Changing the saved probe invalidates its old
comparison binding. The default `records` mode retains collection-identity
behavior, and structured `changed` conditions remain available without a model.

Matched evidence must resolve exactly in the current observation. Step-relative
JSON pointers and equivalent packet-root `/observations/<step>/...` pointers
resolve to the same source; a conflicting step or previous-snapshot pointer is
rejected. A valid match is not discarded because its summary exceeds a display
length. All reported changes are retained, and platform delivery splits long
alerts into supported message sizes. Invalid decisions retain typed diagnostic
categories without logging raw source content.
The same evaluator call selects meaningful sections for multi-finding alerts.
Each section renders with a heading and one bullet per finding, separated by
natural paragraph breaks. Older single-summary responses remain deliverable;
an optional presentation error never discards a verified result.

Snapshot setup reserves space for both observations within the existing semantic
packet budget; it never silently clips comparison evidence. Locator discovery
such as `browser_find` returns bounded label previews with truncation metadata.
An incomplete preview cannot be admitted as a semantic monitoring source: use a
complete, scoped content read instead. Existing incomplete probes return an
unknown coverage result without wasting an evaluator call. The three-failure
pause and its alert remain unchanged.

A setup question must identify an actual missing choice or required confirmation.
Already supplied source, interval and duration remain attached to that unfinished
request. A numbered answer is scoped against the retained request; confirming
continues through the existing action and approval checks, while declining does
not inherit an older inventory or creation requirement. This is shared with
other scheduled actions, rather than a tracker-specific confirmation path.

For a repeating `browser_extract_text` read, supply an observed `ready_selector`
or `ready_text` landmark. Each read waits for that result container or stable
label, including after a worker navigates or restores the page. Do not use a
changing vote count, timestamp or headline as the readiness condition.
If a label also occurs in menus or footers, use a selector for the actual results
element instead of that broad text match. Readiness must identify result data,
not merely prove that the surrounding page loaded.
Cached setup text alone cannot prove that a newly loaded page has finished rendering;
admission rejects an unverified readiness contract. Readiness failures retain
the previous successful snapshot and use the normal failure/pause policy.

For large rendered lists, `browser_extract_items` accepts a `fields` projection.
Returning just `title` and `canonical_url` can preserve the required article data
within the live-check budget. Projection keeps record/section provenance and the
original coverage flags; it never turns a truncated collection into a complete
one. Arbitrary JavaScript remains unavailable as a repeating live probe.


### Date scope for recurring checks

Recurring jobs can explicitly store a fixed time window or a rolling upcoming
range. A rolling range advances using the configured timezone on every run;
a fixed window stops at its saved end rather than silently moving an event to
another date. Existing jobs retain their saved instructions until their date
policy is explicitly updated. When the intended policy is unclear, the assistant
should ask with numbered choices.

When an adapter supports it, a failed saved region read gets one bounded repair
attempt using the configured model and current page observations. The worker
can correct the selector while preserving the collection limit, then rereads and checks the
original condition. It saves the replacement only after that check succeeds.
It cannot change the requested condition, section heading, account, or output
fields to manufacture a match. If repair cannot be verified, normal failure
counting and the existing pause alert still apply.

### Shared request checks and polling

Tracker setup and user-requested changes use the same goal, premise and outcome
checks as other tool workflows. The agent should use the supplied context,
notice contradictory targets or dates, correct its own plan when possible, and
ask a numbered clarification when the intended choice is ambiguous. The action
preflight does not replace account consent or grant approval.

Each poll continues to use its stored observation steps, job-owned browser
session, condition evaluator and bounded repair budget. It does not restart the
chat agent or perform the general request review on every poll. Unknown checks
still count toward the three-failure pause and cause-and-repair alert.

## Presentation and condition amendments

Live alerts use the same presentation preferences and message renderer as chat
and scheduled reports, including emoji settings. Multiple changes have short
headings and one finding per item, with exact values and source links preserved.
Platform adapters only translate markup and split messages at platform limits.

To change what qualifies, ask in the original chat or reply to a tracker alert.
The assistant reads the saved matcher and calls `update_live_tracker` with its
complete replacement condition. Listing the tracker is not proof of an update.
The accepted receipt contains the persisted matcher and condition revision.
The same operation is available through the PATCH route. Invalid conditions
leave the stored tracker unchanged. Pending alerts and unfinished checks from
the superseded condition are invalidated. Already delivered alerts remain in
history. A paused tracker stays paused until explicitly resumed.
