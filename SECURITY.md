# Security Policy

Nullion is local-first software, but it can be connected to model providers,
chat adapters, browser automation, files, terminals, plugins, and third-party
accounts. Please treat your configuration and connected credentials with the
same care you would give any operator tool.

## Supported Versions

Security updates are handled on the latest public release line.

| Version | Supported |
| --- | --- |
| 0.1.x | Yes |

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

During setup, macOS users can choose where the chat-history data key lives:

- `NULLION_KEY_STORAGE=keychain` stores the data key in macOS Keychain.
- `NULLION_KEY_STORAGE=local` stores the data key at
  `~/.nullion/chat_history.key` with `0600` file permissions.

Keychain storage is recommended on macOS because copying the `~/.nullion`
folder alone is not enough to decrypt chat history. Local key-file storage is
more portable, but anyone who copies both `chat_history.db` and
`chat_history.key` can decrypt that chat history.

## Public Issues

Public bug reports are welcome, but please keep them free of secrets and private
data. If a report needs sensitive details, use the private advisory path above.
