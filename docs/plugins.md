# Nulliøn Plugins

Plugins are optional capability packs. They let Nulliøn connect to email,
calendar, search, browser automation, workspace files, and future services
without turning the kernel into a pile of provider-specific code.

The rule is simple:

- **Plugin** = what the user wants Nulliøn to be able to do.
- **Provider** = the service or local adapter that powers that capability.
- **Account connection** = the specific user/account credentials for a provider.
- **Skill pack** = product/workflow instructions that explain when and how to use capabilities well.
- **Policy** = what Sentinel allows, asks about, remembers, or blocks.

For example, Google is a provider family. It should not be a vague “Gmail
plugin” that silently enables everything. A user enables `email_plugin` and,
separately, `calendar_plugin`; both may bind to `google_workspace_provider`.

## Current catalog

| Plugin | Capability | Status | Provider options |
| --- | --- | --- | --- |
| `search_plugin` | Search and fetch public web pages | Available | `builtin_search_provider`; `brave_search_provider`; `google_custom_search_provider`; `perplexity_search_provider`; `duckduckgo_instant_answer_provider` |
| `browser_plugin` | Agent browser sessions, screenshots, page inspection | Available | Playwright; Chrome/Brave CDP |
| `workspace_plugin` | File/project workspace tools inside allowed roots | Available | Local filesystem |
| `media_plugin` | Audio transcription, image OCR, optional image generation | Available | Local media tools |
| `email_plugin` | Search/read email | Preview | Google Workspace preview; Microsoft 365 and IMAP/SMTP planned |
| `calendar_plugin` | List calendar events | Preview | Google Workspace preview; Microsoft 365 and Apple Calendar planned |
| `messaging_plugin` | Account-level messaging tools | Planned | Telegram/Slack/Twilio style providers planned |

Telegram and Web are chat adapters today. They are not the same thing as
account-capability plugins.

## Configure plugins

Short-term configuration is env based so install scripts, launchd, and chat
commands can reason about one source of truth.

```bash
NULLION_ENABLED_PLUGINS=search_plugin,browser_plugin,workspace_plugin,media_plugin
NULLION_PROVIDER_BINDINGS=search_plugin=builtin_search_provider,media_plugin=local_media_provider
NULLION_BROWSER_ENABLED=true
NULLION_BROWSER_BACKEND=auto
NULLION_WORKSPACE_ROOT=/Users/you/Projects
NULLION_ALLOWED_ROOTS=/Users/you/Projects,/Users/you/.nullion/.nullion-artifacts
```

Fresh installs provision the default local media runtime when the platform has
a supported package manager, then expose stable tool names so the user can
enable or disable the behavior without discovering a missing package later:

```bash
NULLION_ENABLED_PLUGINS=search_plugin,browser_plugin,workspace_plugin,media_plugin
NULLION_PROVIDER_BINDINGS=search_plugin=builtin_search_provider,media_plugin=local_media_provider

# Audio → text. Recommended: installer-managed whisper.cpp + ffmpeg with the
# base.en GGML model at ~/.nullion/models/ggml-base.en.bin. Leave this unset
# for the built-in default so Nullion can convert Telegram OGG/Opus notes
# before whisper-cli.
# Python Whisper is intentionally not used as an automatic fallback because it
# often downloads/runs a much larger model than short operator voice notes need.
# NULLION_AUDIO_TRANSCRIBE_COMMAND='custom-transcribe {input} {language}'
# Optional: local media command timeout in seconds. Defaults to 120.
NULLION_MEDIA_COMMAND_TIMEOUT_SECONDS=120

# Image → text. If this is omitted and tesseract exists, Nulliøn uses:
# tesseract {input} stdout
NULLION_IMAGE_OCR_COMMAND='tesseract {input} stdout'

# Text → image. Good local providers: ComfyUI, Stable Diffusion WebUI, InvokeAI,
# or a small local wrapper script. The command must write the image to {output}.
NULLION_IMAGE_GENERATE_COMMAND='/Users/you/.nullion/providers/media/generate-image {prompt_file} {output} {size}'
```

Media tools are platform-agnostic. Web, Telegram, and future chat adapters all
call the same tool names:

- `audio_transcribe(path, language?)`
- `image_extract_text(path)`
- `image_generate(prompt, output_path, size?)`

Recommended defaults:

- Audio transcription: `whisper.cpp` for small, fast local models; `faster-whisper`
  for GPU-backed installs.
- Image OCR: `tesseract` as the lightweight default; PaddleOCR or docTR for
  heavier multilingual/structured OCR.
- Image generation: local ComfyUI/Stable Diffusion wrapper when installed, or a
  model-provider adapter only if the configured provider explicitly supports
  image generation and the user enables it.

Provider-backed plugins declare explicit bindings:

```bash
NULLION_ENABLED_PLUGINS=search_plugin,email_plugin,calendar_plugin
NULLION_PROVIDER_BINDINGS=search_plugin=builtin_search_provider,email_plugin=google_workspace_provider,calendar_plugin=google_workspace_provider
```

Custom HTTP email API option:

```bash
NULLION_ENABLED_PLUGINS=email_plugin
NULLION_PROVIDER_BINDINGS=email_plugin=custom_api_provider
NULLION_CUSTOM_API_BASE_URL=https://api.example.com
NULLION_CUSTOM_API_TOKEN=...
```

Then add a workspace connection for the Custom Email API bridge and enter
`NULLION_CUSTOM_API_TOKEN` as the token reference. Workspace connections are
scoped to the member or operator workspace that owns the request; a member
without a matching connection does not fall back to the admin account unless an
admin explicitly creates a shared credential for a provider that allows shared
use. The service must support:

- `GET /email/search?q=invoice&limit=5` returning `{ "results": [...] }`
- `GET /email/read/{id}` returning either a message object or `{ "message": {...} }`

### Provider connections and skills

Settings -> Users -> Connections is generated from auth-required skills and
provider bindings. It should only list providers that need credentials. A
custom skill pack that contains only workflow guidance is available to enabled
workspaces without any connection row; a custom skill pack that declares or
mentions API keys, OAuth, tokens, credentials, secrets, or login requirements
gets a provider entry so the admin can attach credentials.

Credential scope is explicit:

- `workspace`: the credential is usable only by that workspace.
- `shared`: an admin-owned credential is usable across workspaces after a
  confirmation dialog.

Native account tools and connector skills are separate routes. When a native
provider such as Gmail fails because its credential is invalid, Nulliøn now
surfaces any active external connector providers to the agent so it can try a
relevant enabled connector skill before giving up. The connector still needs
usable skill instructions and a valid connector URL; Nulliøn never invents
account access from a token alone.

Use shared credentials only for service accounts or intentionally shared
provider accounts. Personal mail and calendar providers should normally remain
workspace-specific.

Search provider options:

```bash
# Default: no extra account or key.
NULLION_PROVIDER_BINDINGS=search_plugin=builtin_search_provider

# Brave Search API.
NULLION_PROVIDER_BINDINGS=search_plugin=brave_search_provider
NULLION_BRAVE_SEARCH_API_KEY=...

# Google Custom Search JSON API. Requires a Programmable Search Engine ID.
NULLION_PROVIDER_BINDINGS=search_plugin=google_custom_search_provider
NULLION_GOOGLE_SEARCH_API_KEY=...
NULLION_GOOGLE_SEARCH_CX=...

# Perplexity Search API.
NULLION_PROVIDER_BINDINGS=search_plugin=perplexity_search_provider
NULLION_PERPLEXITY_API_KEY=...

# DuckDuckGo Instant Answers. Keyless, but not a full organic-results API.
NULLION_PROVIDER_BINDINGS=search_plugin=duckduckgo_instant_answer_provider
```

Disabling a plugin means removing it from `NULLION_ENABLED_PLUGINS` and
restarting Nulliøn. Provider bindings for disabled plugins should also be
removed.

## Chat commands

These commands work from Telegram and any future chat adapter that routes
operator commands through the platform-agnostic command handler:

```text
/plugins
/plugins available
/plugin search_plugin
/tools
```

The settings UI exposes the same model:

- enabled/disabled switch per plugin
- provider selector per enabled plugin
- account connection status per provider
- workspace-specific connection assignment for member accounts
- policy summary and revoke controls
- setup/repair action when credentials or dependencies are missing

## Installer flow

Recommended install/setup flow:

1. Pick capabilities:
   - Search
   - Browser
   - Workspace
   - Media: audio transcription, image text extraction, optional image generation
   - Email
   - Calendar
2. Pick provider for each enabled capability.
3. Connect account credentials where needed.
4. Show Sentinel policy defaults.
5. Save config and restart Nulliøn.

Do not ask “Install Gmail?” and then quietly enable both email and calendar.
Ask for Email and Calendar separately, then allow both to use Google Workspace
if the user chooses that provider.

## Building a plugin

Plugins should be boring adapters around typed tools. Keep secrets and provider
specific behavior out of the kernel.

Minimum plugin shape:

```python
from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass


def register_example_plugin(registry: ToolRegistry, *, provider_callable) -> None:
    registry.register(
        ToolSpec(
            name="example_lookup",
            description="Look up something through the example provider.",
            risk_level=ToolRiskLevel.LOW,
            side_effect_class=ToolSideEffectClass.READ_ONLY,
            requires_approval=True,
        ),
        lambda invocation: provider_callable(invocation.arguments),
    )
    registry.mark_plugin_installed("example_plugin")
```

Plugin author checklist:

- use stable capability names such as `email_search`, not provider names
- expose provider adapters separately from capability registration
- declare read/write behavior accurately
- set `requires_approval=True` for account, network, filesystem, or external side effects
- emit useful errors when credentials or provider dependencies are missing
- never claim a capability is installed unless its tools are actually registered
- keep account identities and secrets out of normal logs
- write tests for registration, provider failure, and policy boundaries

Provider resolver checklist:

- map `plugin_id + provider_id` to concrete callables in one place
- reject unknown providers clearly
- do not let provider bindings enable disabled plugins
- prefer keychain or encrypted storage for secrets

## Popular provider ideas

These are provider adapters, not kernel features:

- Brave Search API, Google Custom Search JSON API, Perplexity Search API, and
  DuckDuckGo Instant Answers for `search_plugin`
- whisper.cpp, faster-whisper, Tesseract, PaddleOCR, docTR, ComfyUI, Stable
  Diffusion WebUI, and InvokeAI for `media_plugin`
- Google Workspace for `email_plugin` and `calendar_plugin`
- Microsoft 365 for `email_plugin` and `calendar_plugin`
- IMAP/SMTP for `email_plugin`
- Apple Calendar for `calendar_plugin`
- Notion, GitHub, Slack, Linear, Jira, and Twilio as future plugin/provider families

If a community plugin uses another provider, it should still present a stable
capability contract to Nulliøn.

See `docs/skill-packs.md` for Google Skills and other external `SKILL.md`
reference packs. Skill packs are deliberately separate from plugins: enabling a
Google skill pack should not grant Gmail, Calendar, Drive, or Cloud access by
itself.
