# Configuration and local data

Use Web **Settings** for normal setup. The installer writes the instance's
`.env` under `~/.nullion` by default. A custom `NULLION_HOME` selects a different
instance. Restart the affected service after changing environment settings;
launchers also accept an explicit `--env-file` where shown by `--help`.

## Local HTTP connections

`nullion-tray --poll-interval 10` checks health and status every ten seconds by
default (minimum two seconds). Each tray owns a pool with at most two connections
and one idle connection, isolated from other instances. It bypasses environment
HTTP proxies for its local service. Failed health checks back off exponentially
up to 60 seconds; a manual refresh bypasses that wait. Concurrent refreshes are
coalesced, and quitting closes the pool.

The web launcher keeps idle HTTP connections for 75 seconds. Tray and Codex
model pools expire idle connections after 60 seconds, allowing normal tray polls
to reuse a connection. Custom server launchers should keep the server idle
timeout longer than the tray polling gap to preserve this benefit. Polling
intervals longer than these idle limits reconnect normally.

Apply the upgrade to both the web service and tray, then restart both. Each
running instance needs the updated pair; an older development or stage tray
continues using its previous transport even after production is upgraded.

Codex model variants share their originating client's pool (100 maximum
connections, 20 idle), while authentication headers and deadlines are supplied
per request. Pools close on explicit shutdown or when their last owner is
discarded. An unconsumed or failed model stream is closed, so reuse depends on
the provider completing the HTTP body. This never extends a response deadline.

Restart POST requests are not automatically replayed. If a response is lost
after sending, the tray reports an unknown result; check status before retrying.
A failure to establish the connection can still use the existing CLI fallback.

## Model

| Setting | Meaning |
| --- | --- |
| `NULLION_MODEL_PROVIDER` | Configured provider, such as `openai`, `anthropic`, or `codex` |
| `NULLION_MODEL` | Model identifier; a comma-separated value selects its first nonempty entry for chat |
| `NULLION_OPENAI_API_KEY` | Key for the OpenAI-compatible client; provider-specific credential settings are also supported |
| `NULLION_OPENAI_BASE_URL` | Optional base URL for a compatible endpoint |
| `NULLION_REASONING_EFFORT` | `low`, `medium`, or `high`, where supported by the model |
| `NULLION_AGENT_MODEL_TIMEOUT_SECONDS` | Per ordinary agent model call: default 45 seconds, bounded to 10–180 seconds |
| `NULLION_ARTIFACT_MODEL_TIMEOUT_SECONDS` | Per artifact or structured source-answer preparation call: default 120 seconds, bounded to 30–180 seconds |

These are per-call deadlines, not a total task duration. Exhausted model-call
timeouts are reported separately from the overall request deadline. Answer
preparation can recover selected source excerpts without repeating research;
it still reports unfinished work and does not certify completion.

Use `/models`, `/model <provider> <model_name>`, or Settings → Model to select
from the models actually available to your account. A comma-separated model list
is not a promise of automatic failover for every error. Keep secrets out of
shell history and issue reports; use credential fields or the configured store.

## Capabilities and visibility

Requests use the existing iteration limits and user cancellation, without
separate tool-call, cumulative-input-character or retained-inspection quotas.
The removed `NULLION_REQUEST_MAX_TOOL_CALLS`,
`NULLION_REQUEST_MAX_MODEL_INPUT_CHARS` and
`NULLION_REQUEST_MAX_EVIDENCE_INSPECTIONS` settings have no effect, including
if left in an existing environment. Diagnostic counts do not stop requests.
Schedules and account permissions are unchanged. Large
source histories are compacted without discarding original receipts; native
email bodies retain text beyond the former 6,000-character cutoff. When a
readable snippet replaces a low-signal body preview, `body_payload` preserves
the original decoded text and `body_is_preview` identifies the preview.
The newest result batch is consumed before archival compaction. Exact inspected
windows remain available across later tool steps while their owned source is
unchanged; parallel inspections must not erase one another's findings.

| Setting | Meaning |
| --- | --- |
| `NULLION_ENABLED_PLUGINS` | Comma-separated enabled capability plugins |
| `NULLION_PROVIDER_BINDINGS` | Explicit `plugin_id=provider_id` bindings |
| `NULLION_ENABLED_SKILL_PACKS` | Enabled instruction packs, without granting access |
| `NULLION_WORKSPACE_ROOT` | Default file/project root |
| `NULLION_ALLOWED_ROOTS` | Additional allowed filesystem roots |
| `NULLION_BROWSER_ENABLED` | Enable the browser capability |
| `NULLION_BROWSER_BACKEND` | `auto`, `cdp`, or `playwright` for low-level browser tools |
| `NULLION_BROWSER_PREFERRED` | Preferred visible browser app |
| `NULLION_BROWSER_CDP_URL` | Low-level browser CDP endpoint, when configured |
| `NULLION_DIRECT_WEB_FETCH_ENABLED` | Opt in to the direct fetch tool |

`/verbose on`, `/verbose off`, and `/verbose status` control activity details.
They do not control planner-card visibility. The current runtime keeps task
planner cards enabled; the old `NULLION_TASK_PLANNER_FEED_MODE` and
`NULLION_TASK_PLANNER_FEED_ENABLED` switches are not read by this build. `/planner <message>` starts one explicitly
planned request. `/thinking` and `/streaming` control separate displays.

See [Plugins](plugins.md), [Skill packs](skill-packs.md), and
[Browser tasks](../src/nullion/plugins/browser_plugin/README.md) for exact setup.
Live-tracker worker capacity and recurring-job capacity are separate persisted
Web settings, not undocumented environment switches.

## Chat adapters

Telegram uses `NULLION_TELEGRAM_BOT_TOKEN`,
`NULLION_TELEGRAM_OPERATOR_CHAT_ID`, and `NULLION_TELEGRAM_CHAT_ENABLED`.
Use `/chat <message>` when plain-text chat is disabled.

Slack uses `NULLION_SLACK_ENABLED`, `NULLION_SLACK_BOT_TOKEN`, and
`NULLION_SLACK_APP_TOKEN`. Discord uses `NULLION_DISCORD_ENABLED`,
`NULLION_DISCORD_BOT_TOKEN`, and `NULLION_DISCORD_OPERATOR_USER_ID` for its
single-user operator. Authorize member accounts under Settings → Users.
Never use a bot token as a substitute for an authorized user's identity.

## Data and encryption

The default runtime checkpoint is `~/.nullion/runtime.db`. It includes runtime
and audit state, scheduler records, and live trackers. Chat history uses
`chat_history.db`; provider secrets use the encrypted credential store.
The presence of encrypted history does not mean every file or every field in
the runtime directory is encrypted. Protect the entire directory and its backups.

`NULLION_KEY_STORAGE=local` is the default and keeps local Fernet key files
with owner-only permissions. `NULLION_KEY_STORAGE=system` uses macOS Keychain,
Windows Credential Manager, or Linux Secret Service. `keychain` remains a
macOS-specific alias. Back up encryption keys securely; a database without its
required key cannot be decrypted. See [Security](../SECURITY.md).

## Updates, recovery, and contributors

`nullion update` follows the latest release; `nullion update --hash` follows
repository head, which may include unreleased behavior. Health checks and
rollback protect updates but do not replace backing up your data. Recovery
commands and troubleshooting are in [Support](support.md).

Contributors use one repository for app code, tests, installer harnesses, and
local operations. Website source is maintained separately. Instance homes are
runtime data directories, not additional test repositories. See
[Testing](e2e-testing.md) before starting isolated verification.

### Interactive output budgets and retained evidence

Scheduled executions default to 128 model calls per run, shared across source
work, reviews, report composition and provider retries. Ordinary chat keeps its
separate default of 48 producer iterations. This allowance counts model calls,
not tokens, tool calls or elapsed minutes; raising it permits more work but does
not make blocked sources accessible or remove the existing recovery rules.

`NULLION_CRON_AGENT_MAX_ITERATIONS` overrides the scheduled base allowance. For
compatibility with the existing execution policy, a fresh scheduled run shares
twice that value across all model calls (the default base is 64). Positive
explicit values are honored rather than silently capped at 40; invalid values
use the default, and zero/negative values use a base of one. With no explicit
scheduled override, a larger configured chat allowance can raise that base.
Existing saved approvals retain their original allowance and selected grant.

Doctor budget approvals also apply to scheduled executions. The numbered 2x,
5x and 10x choices grant additional model rounds for the saved run, with its
prior work retained. They do not change `NULLION_CRON_AGENT_MAX_ITERATIONS`,
future schedules, connector access or action approvals. Declining affects only
the continuation. Each approved scheduled continuation shares its granted rounds
across execution recovery; recovery cannot silently double that grant.

`NULLION_WEB_INTERACTIVE_FAST_MAX_TOKENS` and
`NULLION_MESSAGING_INTERACTIVE_FAST_MAX_TOKENS` apply to the interactive chat
profile. They do not replace the configured workflow client when tool execution
or typed artifact work begins. A response truncated by that optimization is
retried with the original workflow client before a partial call can execute.
An explicit workflow output limit remains in force. The LangChain adapter also
inherits the configured client limit unless its caller provides an override.

`NULLION_TOOL_SCOPE_CLASSIFIER_MAX_TOKENS` defaults to 512 so the structured
decision can fit its schema, including the observation policy. This is an output
ceiling for an already-gated selector, not additional calls on plain chat.
`NULLION_TOOL_SCOPE_CLASSIFIER_TIMEOUT_SECONDS` remains 5. Older explicit limits
such as 64 or 96 can truncate the JSON decision; remove those overrides or raise
them when updating. An invalid selector cannot automatically turn previous
source-continuation hints into another source check. Invalid-decision diagnostics
record the output size, stop reason and budget without the private prompt.

Model context archives consumed tool results above 8,000 characters and bounds
aggregate archival tool history at 64,000 characters. These are context preview
targets, not limits on generated files, email volume, tool calls, or final output.
New result batches remain readable before archiving. Evidence with ambiguous
protocol ids remains intact even if that exceeds the preview target. Exact older fields remain
available through request-local retained-evidence inspection. Repeated invalid
argument diagnostics and superseded runtime checkpoints are compacted without
rewriting the original execution records or user instructions.

### User-local time display

The saved Preferences timezone is the source of truth for chat, Web timestamps,
history dates, cron descriptions, reminders, tracker notices, approvals and report
generation. The default clock is 12-hour with AM/PM; a 24-hour clock is available
only as an explicit saved preference. Web labels use these settings even when the
browser runs in another timezone or uses a 24-hour locale. Saving a change updates
visible chat timestamps without changing their stored instants. UTC is a valid
explicit user timezone and is never replaced by the host's timezone.

Scheduler expressions retain their stored execution timezone, and persisted/API
timestamps remain ISO timestamps with timezone information. Presentation changes
do not reschedule existing jobs. Date-only calendar events remain dates. Report
composition receives the same timezone and clock policy as chat; source times are
converted for display while source-zone context and machine timestamps are retained
when relevant. Invalid timezone/clock/date settings are rejected before saving.

Cron descriptions use the upcoming run's timezone offset, including DST, rather
than a fixed reference season. Cross-zone conversion carries minutes and weekday
rollovers. If a monthly pattern or DST transition cannot be expressed faithfully
as one local repeating pattern, the UI labels it custom and shows the exact next
run in the saved user timezone.

Media model, command and enable/disable changes take effect in Web after saving settings. Existing messaging services apply the same media adapter refresh when they next reload saved configuration; ordinary messages with unchanged settings reuse their registry. Configuring a model requires a usable credential and provider access, not just an enabled switch. A provider fallback artifact does not prove that the requested image was generated successfully.

Errors from a configured image model or generation command remain generation failures. The caller receives the provider failure, including available HTTP status and retryability metadata, instead of a placeholder labelled as the requested image. Authentication or depleted-credit failures require fixing the provider configuration or billing before retrying.
