# Deep Agents / LangChain Improvement Checklist

This checklist tracks the remaining Nullion surfaces that can benefit from the
Deep Agents and LangChain migration.

## In Progress

- [x] Let Deep Agents own mini-agent planning and tool-loop execution.
- [x] Add delegated-task golden workflows for research, repo analysis, artifact creation, user-input pauses, approval-required tools, and failure recovery.
- [x] Map core Deep Agents stream events into Nullion progress traces.
- [x] Infer Deep Agents skills and subagent profiles from delegated task scope.
- [x] Convert inferred task profiles into real in-memory Deep Agents `SKILL.md` sources.
- [x] Add richer trace events for subagents, tool argument previews, retries, approval pauses, and recoverable tool failures.
- [x] Treat delegated approval/user-input partials as paused tasks instead of failed tasks.

## Next

- [ ] Resume delegated tasks after approval or user input from the paused Deep Agents step.
- [ ] Route auto-skill proposals through a Deep Agents skill/subagent golden test loop.
- [ ] Add scheduled-job agents for cron/reminder run-inspect-notify workflows.
- [x] Move artifact report/screenshot delivery checks behind a dedicated artifact verifier subagent profile.
- [x] Strengthen LangChain tool schemas with better descriptions and grouping from Nullion's registry.
- [x] Add LangChain retry wrappers for transient Nullion-backed model failures.
- [x] Add LangChain retry wrappers for transient Nullion tool invocation failures.
- [x] Add scheduled-job Deep Agents profile for cron/reminder/monitor run-inspect-notify tasks.
- [x] Attach Deep Agents validation skill/subagent/golden-check descriptors to auto-skill proposals.
- [x] Add LangChain fallback wrappers for fragile tool paths with multiple equivalent tools.
- [x] Add delegated Deep Agents validation task builder for auto-skill golden checks.
- [x] Capture Deep Agents thread IDs and structured resume tokens for approval/user-input pauses.
- [ ] Replace remaining hand-rolled mini-agent planning in mission/task planner code where Deep Agents is a clear fit.
