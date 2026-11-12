# Security Policy

Nullion is local-first software, but it can be connected to model providers,
chat adapters, browser automation, files, terminals, plugins, and third-party
accounts. Please treat your configuration and connected credentials with the
same care you would give any operator tool.

## Supported Versions

Security updates are handled on the latest public release line.

| Version | Supported |
| --- | --- |
| Latest public release | Yes |
| Older releases and unreleased source | No separate maintenance guarantee |

## Reporting a Vulnerability

Please report security issues privately by opening a private security advisory
on GitHub:

https://github.com/shumanhi/nullion/security/advisories/new

If GitHub advisories are not available yet, open a minimal public issue asking
for a private contact path. Do not include exploit details, API keys, bot
tokens, OAuth tokens, private URLs, logs with secrets, or private file paths in
public issues.

Helpful reports include:

- A short description of the issue and the affected feature.
- The operating system and Nullion version.
- Steps to reproduce using redacted example data.
- Expected behavior and actual behavior.
- Any relevant logs with secrets removed.

## Operator Responsibility

Nullion is provided as-is under the Apache License 2.0. You are responsible for
choosing which tools, plugins, model providers, chat adapters, file roots,
browser permissions, API keys, schedules, and approval modes you enable.

Before running Nullion against sensitive workspaces or connected accounts:

- Review enabled plugins and tool permissions.
- Keep approval mode strict until you trust the workflow.
- Scope file roots and external destinations narrowly.
- Redact secrets before sharing logs or screenshots.
- Rotate any token that may have been exposed.

## Local Data Encryption

Nullion encrypts local chat-history text, titles, and artifact metadata before
writing them to `~/.nullion/chat_history.db`.

Chat-history and provider-credential encryption keys can use:

- `NULLION_KEY_STORAGE=local` (default): owner-only local key files, including
  `chat_history.key` and `credentials.key` in the instance home.
- `NULLION_KEY_STORAGE=system`: macOS Keychain, Windows Credential Manager,
  or Linux Secret Service.
- `NULLION_KEY_STORAGE=keychain`: the older macOS-specific alias.

Anyone who copies both an encrypted database and its local key can decrypt
that database. System key storage reduces that portability; keep access to its
original secret store when planning backup and restore. Never publish keys or
raw runtime checkpoints.

This is field-level encryption, not full-disk or full-directory encryption.
Runtime/audit records, live-tracker source arguments and conditions, browser
profiles, generated artifacts, configuration, logs and backups may contain
private data. Protect the instance directory and limit access to the Web UI.
Connected models and services receive the data required for authorized tool
work; local-first storage does not make external processing local.

Skill instructions and website text are untrusted inputs. Sanitization, length
limits, domain checks and approvals reduce specific risks; they do not prove
that prompt injection is impossible. Review enabled capabilities, destinations
and destructive actions independently.

## Public Issues

Public bug reports are welcome, but please keep them free of secrets and private
data. If a report needs sensitive details, use the private advisory path above.
