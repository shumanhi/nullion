# Nulliøn Skill Packs

Skill packs are reusable reference instructions. They teach Nulliøn how to
approach a product, workflow, or domain, while plugins provide the actual
tools and account access.

The boundary is important:

- **Skill pack** = when and how to work well.
- **Plugin** = what Nulliøn can actually do.
- **Provider** = which service powers a plugin.
- **Sentinel** = whether a tool action is allowed, remembered, or blocked.

A skill pack can suggest using a browser, email, calendar, search, or files,
but it does not grant those capabilities. The relevant plugin must still be
enabled, configured, and approved by policy.

## Built-in catalog

| Skill pack | Status | Source | Covers |
| --- | --- | --- | --- |
| `nullion/web-research` | Built-in | Nulliøn | Search/fetch workflows, source comparison, summaries, citations, uncertainty handling |
| `nullion/browser-automation` | Built-in | Nulliøn | Browser navigation, UI inspection, forms, screenshots, local web-app testing |
| `nullion/files-and-docs` | Built-in | Nulliøn | Local files, notes, reports, spreadsheets, slide decks, document artifact workflows |
| `nullion/pdf-documents` | Built-in | Nulliøn | PDF creation, conversion, verification, image-aware reports, attachment delivery |
| `nullion/email-calendar` | Built-in | Nulliøn | Inbox triage, drafted replies, meeting prep, scheduling, reminders, calendar summaries |
| `nullion/github-code` | Built-in | Nulliøn | Repository work, code review, issues, PRs, release notes, CI triage |
| `nullion/media-local` | Built-in | Nulliøn | Audio transcription, image OCR, image understanding, local image generation |
| `nullion/productivity-memory` | Built-in | Nulliøn | Task planning, daily summaries, recurring workflows, preferences, follow-ups |
| `nullion/connector-skills` | Built-in | Nulliøn | SaaS/API connector gateways, MCP workflows, account authorization checks, and custom HTTP bridges |
| `google/skills` | Available | `https://github.com/google/skills` | Gemini API, BigQuery, Cloud Run, Cloud SQL, Firebase, GKE, AlloyDB, Google Cloud onboarding/authentication/network observability, and Well-Architected Framework guidance |

Google's public repository describes itself as Agent Skills for Google
products and technologies, including Google Cloud. The repo is Apache-2.0 and
is under active development, so Nulliøn treats it as an optional reference
pack rather than a mandatory core dependency.

## Enable a skill pack

Use the web Settings screen, the CLI installer, or set:

```bash
NULLION_ENABLED_SKILL_PACKS=google/skills
```

The installer ships all built-in Nulliøn skill packs and selects them by
default. During setup, or later in Settings, you can disable any pack for a
quieter prompt surface:

```bash
NULLION_ENABLED_SKILL_PACKS=nullion/web-research,nullion/browser-automation,nullion/files-and-docs,nullion/pdf-documents,nullion/email-calendar,nullion/github-code,nullion/media-local,nullion/productivity-memory,nullion/connector-skills
```

Then restart Nulliøn.

Commands:

```text
/skill-packs
/skill-packs available
/skill-pack google/skills
```

## Install a skill pack

Nulliøn can import OpenClaw-style skills: a directory tree containing one or
more `SKILL.md` files. Installing a pack copies or clones the files into
`~/.nullion/skill-packs` and writes a small manifest. It does not execute shell
scripts, install dependencies, or grant any tools.

CLI:

```bash
nullion-cli skill-pack install ~/.openclaw/workspace/skills --id openclaw/local-skills
nullion-cli skill-pack install https://github.com/example/skills.git --id example/skills
nullion-cli skill-pack list
```

Web:

Open Settings → Learning and planning → Skill packs, paste a local folder or Git
URL, optionally provide an ID such as `openclaw/local-skills`, then choose
Install and enable. Restart Nulliøn after changing enabled packs.

Review imported `SKILL.md` files before enabling packs from public marketplaces.
The installer scans for suspicious text such as shell commands or secret/token
references and surfaces warnings, but review is still required.

## Auth and workspace availability

Skill packs are installed globally, but account access is not. Nulliøn uses the
pack instructions to decide whether a skill is instruction-only or
auth-required:

- Instruction-only packs are available to all workspaces once enabled.
- Packs that mention API keys, OAuth, tokens, credentials, secrets, or login
  requirements become auth-required.
- Auth-required packs expose provider options in Settings -> Users ->
  Connections.
- Admins choose whether a provider should use per-workspace credentials or, for
  providers that allow it, one shared admin credential across all workspaces.
- Active external connector providers are discovered from workspace
  connections. If a native email/calendar provider fails because its own
  credential is missing or unauthorized, the agent should check enabled
  connector skills and try a relevant connector provider before declaring that
  account access is unavailable.

This means a newly installed custom skill pack does not need a code change in
Nulliøn. If it is just guidance, every workspace can use it. If it needs an
account or API key, the Connections UI shows a provider entry derived from the
skill pack metadata and stores only credential references.

## Google Skills and plugins

For Google-related work, keep these choices separate:

- Enable `google/skills` when you want Google product workflow knowledge.
- Enable `search_plugin` when Nulliøn should search or fetch public web pages.
- Enable `browser_plugin` when Nulliøn should open pages or capture screenshots.
- Enable `email_plugin` and bind it to `google_workspace_provider` when Gmail account access is needed.
- Enable `calendar_plugin` and bind it to `google_workspace_provider` when Google Calendar access is needed.

This avoids a vague “Google integration” switch that accidentally grants more
power than the user expected.

## Security rules

Skill packs are treated as untrusted instruction sources until the user
chooses to enable them.

Nulliøn should:

- show the source and status before enabling a pack
- never execute scripts or shell commands from a skill pack automatically
- apply Sentinel approval to every real tool invocation suggested by a skill
- keep account secrets in provider configuration, not in skill pack files
- pin downloaded packs to a commit before future automatic refresh support
- show enabled packs in commands and settings

## Authoring a skill pack

Community skill packs should use the common `SKILL.md` shape:

```text
your-pack/
  skill-name/
    SKILL.md
    references/
```

Each `SKILL.md` should include a concise name, description, trigger guidance,
safe operating notes, and any required plugins or provider accounts.

Recommended front matter:

```yaml
---
name: Example Workflow
description: Use when the user asks Nulliøn to perform the example workflow.
requires_plugins:
  - search_plugin
  - browser_plugin
requires_auth:
  - provider: example_api_provider
    reason: Needs a service API token to read account data.
---
```

Author checklist:

- keep instructions platform-agnostic
- list required plugins explicitly
- call out API keys, OAuth, credentials, or account profiles explicitly when
  the skill needs them
- avoid embedding secrets, tokens, or user-specific account identifiers
- avoid instructions that bypass approvals or hide side effects
- include examples of successful and blocked flows
- prefer small focused skills over one giant catch-all skill
