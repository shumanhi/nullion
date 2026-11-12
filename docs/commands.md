# Commands

Chat slash commands use the shared operator handler. Use `/help` for the short
menu and `/help commands` for the complete list installed in your version.
Permissions and adapter support still apply. Telegram group commands can include
`@bot_username`; underscore aliases in Telegram's menu map to the same commands.

There is no live-tracker or cron-management slash-command family. Ask in chat or
use **Settings → Crons**. Agent tool names such as `create_live_tracker` and
`create_cron` are not slash commands. `/stop` cancels active session work; it
does not mean “delete every schedule or stop every persistent tracker.”

## Chat commands

The following catalog matches `operator_commands.py`. Angle-bracket arguments
are placeholders. Numbered selections accept the displayed number where offered.

| Command | Purpose |
| --- | --- |
| `/help` | Show the quick-reference command menu |
| `/help commands` | Show the full command list |
| `/chat <message>` | Talk to Nullion Assistant |
| `/planner <message>` | Run a step-by-step planned task |
| `/stop` | Stop active work in this session |
| `/new` | Clear conversation history and start fresh |
| `/verbose [on\|off\|status]` | Toggle activity details |
| `/thinking [on\|off\|status]` | Show or hide provider reasoning summaries separately |
| `/streaming [on\|off\|status]` | Toggle streamed chat replies where supported |
| `/update [--ignore-checks\|--force]` | Update Nullion safely with automatic rollback |
| `/restart` | Restart the bot process |
| `/models` | Show available and saved models |
| `/model <provider> <model_name>` | Switch to a different model |
| `/memory [all]` | Show saved memory |
| `/memory delete <n\|entry_id\|last-hour\|24h\|all>` | Delete saved memory |
| `/approvals` | List pending approval requests |
| `/approval <n\|approval_id>` | Show one approval request |
| `/approve <n\|approval_id>` | Approve one pending request |
| `/deny <n\|approval_id>` | Deny one pending request |
| `/grants` | List active permission grants |
| `/grant <n\|grant_id>` | Show one permission grant |
| `/revoke-grant <n\|grant_id>` | Revoke one permission grant |
| `/proposals` | List Builder proposals |
| `/proposal <n\|proposal_id>` | Show one Builder proposal |
| `/accept-proposal <n\|proposal_id>` | Accept a saved skill proposal |
| `/reject-proposal <n\|proposal_id>` | Reject a saved proposal |
| `/archive-proposal <n\|proposal_id>` | Archive a saved proposal |
| `/reminder cancel <task_id>` | Cancel a pending reminder |
| `/skills` | List saved skills |
| `/skill <n\|skill_id>` | Show one saved skill |
| `/skill-history <n\|skill_id>` | Show one skill revision history |
| `/update-skill <skill_id> <field> <value>` | Update one skill field |
| `/revert-skill <skill_id> <revision>` | Revert to a prior skill revision |
| `/ping` | Liveness check |
| `/version` | Show installed version |
| `/health` | Show runtime health summary |
| `/doctor [diagnose\|n\|start n\|complete n\|dismiss n]` | Diagnose health or change alert status; `complete` records your resolution choice |
| `/doctor run <id> <doctor:action>` | Request a supported Doctor action or receive its manual next steps |
| `/uptime` | Show current operator runtime uptime |
| `/status` | Show full runtime status |
| `/status active` | Show active work only |
| `/status <capsule_id>` | Show status for one capsule |
| `/deliveries [failed\|partial\|succeeded\|all]` | Show recent platform delivery receipts |
| `/system-context` | Show internal system context |
| `/codebase` | Show abstract codebase summary |
| `/tools` | Show available tool inventory |
| `/plugins` | Show installed and available plugins |
| `/plugins available` | Show plugin catalog |
| `/plugin <plugin_id>` | Show setup details for one plugin |
| `/skill-packs` | Show enabled and available reference skill packs |
| `/skill-packs available` | Show skill pack catalog |
| `/skill-pack <pack_id>` | Show setup details for one skill pack |
| `/backups` | List backup generations |
| `/restore` | Restore newest backup |
| `/restore latest` | Restore newest backup |
| `/restore <generation>` | Restore a specific backup |
| `/auto-skill [conv_id]` | Scan conversation history and propose automatable skills |
| `/accept-skill <n>` | Accept skill proposal number n from the last /auto-skill run |

## Terminal commands

```text
nullion --dashboard
nullion --status
nullion --logs
nullion --errors
nullion --config
nullion --restart
nullion --stop
nullion update
nullion update --hash
nullion uninstall --dry-run
nullion-cli --one-shot "Summarize this project"
nullion-cli skill-pack list
nullion-auth --reauth codex
nullion-recovery status
```

Run `<command> --help` for flags in the installed build. `nullion update` selects
the latest release; `--hash` selects repository head. An update bypass such as
`--ignore-checks` skips protection and should be a deliberate recovery choice.
`nullion --stop` stops services; chat `/stop` stops session work.

See [Support](support.md) for recovery and [Configuration](configuration.md) for
provider, adapter, browser, and local-data settings.
